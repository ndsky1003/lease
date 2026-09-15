package lease

import (
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/RussellLuo/timingwheel"
)

// entry 是缓存中的一个条目。它不随 Get 移动定时任务，而是记录最后访问时间，
// 由定时检查任务在到期时判断是否空闲超时。
type entry[K comparable, V any] struct {
	key   K
	value V
	ttl   time.Duration

	renewInterval time.Duration // 访问合并阈值
	lastRenew     atomic.Int64  // 上次更新 lastAccess 的时间（UnixNano），用于访问合并
	lastAccess    atomic.Int64  // 最后访问时间（UnixNano）

	mu    sync.Mutex
	timer *timingwheel.Timer // 定时检查任务；ttl<=0 时为 nil（永不过期）
}

// stop 取消并清空定时任务。幂等：重复调用无副作用。
func (e *entry[K, V]) stop() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.timer != nil {
		e.timer.Stop()
		e.timer = nil
	}
}

// touch 记录一次访问。renewInterval>0 时合并高频访问，避免无谓的原子写。
func (e *entry[K, V]) touch() {
	now := time.Now().UnixNano()
	if e.renewInterval > 0 {
		last := e.lastRenew.Load()
		if now-last < int64(e.renewInterval) {
			return // 距上次更新不足阈值，跳过
		}
		if !e.lastRenew.CompareAndSwap(last, now) {
			return // 其他 goroutine 已更新
		}
	}
	e.lastAccess.Store(now)
}

// Options 用于配置缓存。
type Options[K comparable, V any] struct {
	Tick          time.Duration // 时间轮每格时长，默认 1 秒
	WheelSize     int64         // 每层槽位数量，默认 64
	OnEvict       func(K, V)    // 释放回调；为 nil 时，若 value 实现了 io.Closer 则自动 Close
	RenewInterval time.Duration // 访问合并阈值：距上次更新不足该值则跳过；<=0 表示每次访问都更新
}

// Lease 是一个带空闲超时（滑动过期）的资源容器。
//
// 底层调度使用开源库 github.com/RussellLuo/timingwheel。Set 时挂一个定时
// 检查任务，到期时根据最后访问时间判断是否空闲超时：超时则释放并清理，
// 否则重新调度到剩余时间。Get 只更新最后访问时间，不移动定时任务。
type Lease[K comparable, V any] struct {
	wheel *timingwheel.TimingWheel

	mu    sync.RWMutex
	items map[K]*entry[K, V]

	onEvict       func(K, V)
	renewInterval time.Duration
}

// New 使用默认配置创建一个缓存。
func New[K comparable, V any](onEvict func(K, V)) *Lease[K, V] {
	return NewWithOptions(Options[K, V]{OnEvict: onEvict})
}

// NewWithOptions 使用自定义配置创建一个缓存，并启动时间轮。
func NewWithOptions[K comparable, V any](opts Options[K, V]) *Lease[K, V] {
	if opts.Tick <= 0 {
		opts.Tick = time.Second
	}
	if opts.WheelSize <= 0 {
		opts.WheelSize = 64
	}
	l := &Lease[K, V]{
		items:         make(map[K]*entry[K, V]),
		onEvict:       opts.OnEvict,
		renewInterval: opts.RenewInterval,
	}
	l.wheel = timingwheel.NewTimingWheel(opts.Tick, opts.WheelSize)
	l.wheel.Start()
	return l
}

// Set 添加或更新一个资源，并设置空闲存活时长 ttl。
// 若 key 已存在，旧的定时检查任务会被停掉，资源被新值替换。ttl <= 0 表示永不过期。
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) {
	l.mu.Lock()
	if old, ok := l.items[key]; ok {
		old.stop()
	}
	e := &entry[K, V]{
		key:           key,
		value:         value,
		ttl:           ttl,
		renewInterval: l.renewInterval,
	}
	l.items[key] = e
	l.mu.Unlock()

	if ttl > 0 {
		now := time.Now().UnixNano()
		e.lastAccess.Store(now)
		e.lastRenew.Store(now)
		l.schedule(e, ttl)
	}
}

// Get 读取一个资源，并记录本次访问（用于滑动过期）。返回的 ok 表示该 key 是否存在。
func (l *Lease[K, V]) Get(key K) (value V, ok bool) {
	l.mu.RLock()
	e, exists := l.items[key]
	l.mu.RUnlock()
	if !exists {
		return
	}
	if e.ttl > 0 {
		e.touch()
	}
	return e.value, true
}

// schedule 挂一个定时检查任务，d 时间后检查该元素是否空闲超时。
func (l *Lease[K, V]) schedule(e *entry[K, V], d time.Duration) {
	e.mu.Lock()
	e.timer = l.wheel.AfterFunc(d, func() { l.check(e) })
	e.mu.Unlock()
}

// check 由定时任务在到期时触发：若距最后访问已超过 ttl（空闲超时），释放资源；
// 否则重新调度到剩余时间。
func (l *Lease[K, V]) check(e *entry[K, V]) {
	now := time.Now().UnixNano()
	last := e.lastAccess.Load()
	if remaining := int64(e.ttl) - (now - last); remaining > 0 {
		l.reschedule(e, time.Duration(remaining))
		return
	}
	//get

	// 已空闲超时，双重检查后再释放，避免与并发 Get 冲突。
	l.mu.Lock()
	if l.items[e.key] != e {
		l.mu.Unlock()
		return
	}
	if now-e.lastAccess.Load() >= int64(e.ttl) {
		delete(l.items, e.key)
		l.mu.Unlock()
		e.stop()
		l.release(e.key, e.value)
		return
	}
	l.mu.Unlock()

	// lastAccess 被并发更新，重新检查。对应上面get那,有个请求进来了
	l.check(e)
}

// reschedule 重新调度定时任务；若元素已被删除，则不再调度，避免僵尸任务。
func (l *Lease[K, V]) reschedule(e *entry[K, V], d time.Duration) {
	l.mu.RLock()
	exists := l.items[e.key] == e
	l.mu.RUnlock()
	if !exists {
		return
	}
	l.schedule(e, d)
}

// Has 判断 key 是否存在。
func (l *Lease[K, V]) Has(key K) bool {
	_, ok := l.Get(key)
	return ok
}

// Delete 主动删除一个资源并释放。会停掉对应的定时任务，避免后续重复释放。
func (l *Lease[K, V]) Delete(key K) {
	l.mu.Lock()
	e, exists := l.items[key]
	if !exists {
		l.mu.Unlock()
		return
	}
	delete(l.items, key)
	l.mu.Unlock()

	e.stop()
	l.release(e.key, e.value)
}

// Len 返回当前缓存中的条目数量。
func (l *Lease[K, V]) Len() int {
	l.mu.RLock()
	n := len(l.items)
	l.mu.RUnlock()
	return n
}

// Stop 停止时间轮并释放所有仍存活的资源。调用后该缓存不应再被使用。
func (l *Lease[K, V]) Stop() {
	l.wheel.Stop()

	l.mu.Lock()
	items := l.items
	l.items = make(map[K]*entry[K, V])
	l.mu.Unlock()

	for _, e := range items {
		e.stop()
		l.release(e.key, e.value)
	}
}

// release 执行资源的释放逻辑。
func (l *Lease[K, V]) release(key K, value V) {
	if l.onEvict != nil {
		l.onEvict(key, value)
		return
	}
	if closer, ok := any(value).(io.Closer); ok {
		_ = closer.Close()
	}
}
