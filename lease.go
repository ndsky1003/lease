package lease

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// entry 是缓存中的一个条目。它不持有定时任务，而是按到期时刻落入到期桶，
// 由单个对齐 ticker 批量处理到期桶。
type entry[K comparable, V any] struct {
	key   K
	value V
	ttl   time.Duration

	renewInterval time.Duration // 访问合并阈值
	lastRenew     atomic.Int64  // 上次更新 lastAccess 的时间（UnixNano），用于访问合并
	lastAccess    atomic.Int64  // 最后访问时间（UnixNano）
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
	Tick          time.Duration // 到期桶处理周期，默认 1 秒
	WheelSize     int64         // 已弃用：分桶实现下不再需要，保留以兼容旧代码
	OnEvict       func(K, V)    // 释放回调；为 nil 时，若 value 实现了 io.Closer 则自动 Close
	RenewInterval time.Duration // 访问合并阈值：距上次更新不足该值则跳过；<=0 表示每次访问都更新
}

// Lease 是一个带空闲超时（滑动过期）的资源容器。
//
// 条目按到期时刻（对齐到 Tick）落入到期桶，由单个对齐 ticker 每 Tick 批量
// 处理到期桶：空闲超时则释放并清理，否则按剩余时间重新分桶。Get 只更新最后
// 访问时间，不移动条目、不创建定时任务。
type Lease[K comparable, V any] struct {
	tick time.Duration

	mu      sync.RWMutex
	items   map[K]*entry[K, V]
	buckets map[int64][]*entry[K, V] // 到期时刻（对齐到 Tick）-> 条目列表

	onEvict       func(K, V)
	renewInterval time.Duration

	stopC    chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// New 使用默认配置创建一个缓存。
func New[K comparable, V any](onEvict func(K, V)) *Lease[K, V] {
	return NewWithOptions(Options[K, V]{OnEvict: onEvict})
}

// NewWithOptions 使用自定义配置创建一个缓存，并启动到期处理循环。
func NewWithOptions[K comparable, V any](opts Options[K, V]) *Lease[K, V] {
	if opts.Tick <= 0 {
		opts.Tick = time.Second
	}
	l := &Lease[K, V]{
		tick:          opts.Tick,
		items:         make(map[K]*entry[K, V]),
		buckets:       make(map[int64][]*entry[K, V]),
		onEvict:       opts.OnEvict,
		renewInterval: opts.RenewInterval,
		stopC:         make(chan struct{}),
	}
	l.wg.Add(1)
	go l.run()
	return l
}

// run 是对齐到 Tick 边界的到期处理循环：逐格推进，flush 到期桶。
func (l *Lease[K, V]) run() {
	defer l.wg.Done()
	tickNs := int64(l.tick)
	var lastDue int64
	for {
		now := time.Now()
		next := now.Truncate(l.tick).Add(l.tick)
		t := time.NewTimer(time.Until(next))
		select {
		case <-t.C:
			n := time.Now().UnixNano()
			cur := n / tickNs * tickNs
			if lastDue == 0 {
				lastDue = cur - tickNs
			}
			// 逐个 flush 从上次到当前之间的每个边界，避免调度延迟漏掉到期桶。
			for d := lastDue + tickNs; d <= cur; d += tickNs {
				l.flush(d, n)
			}
			lastDue = cur
		case <-l.stopC:
			if !t.Stop() {
				select {
				case <-t.C:
				default:
				}
			}
			return
		}
	}
}

// flush 取出 due 时刻的到期桶并逐条处理。
func (l *Lease[K, V]) flush(due, now int64) {
	l.mu.Lock()
	es := l.buckets[due]
	delete(l.buckets, due)
	l.mu.Unlock()

	for _, e := range es {
		l.expire(e, now)
	}
}

// Set 添加或更新一个资源，并设置空闲存活时长 ttl。
// 若 key 已存在，旧值会被释放（OnEvict 或 Close），再由新值替换。ttl <= 0 表示永不过期。
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) {
	l.mu.Lock()
	old := l.items[key]
	e := &entry[K, V]{
		key:           key,
		value:         value,
		ttl:           ttl,
		renewInterval: l.renewInterval,
	}
	l.items[key] = e
	if ttl > 0 {
		now := time.Now().UnixNano()
		e.lastAccess.Store(now)
		e.lastRenew.Store(now)
		due := l.align(now + int64(ttl))
		l.buckets[due] = append(l.buckets[due], e)
	}
	l.mu.Unlock()

	if old != nil {
		l.release(old.key, old.value)
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

// Has 判断 key 是否存在。
func (l *Lease[K, V]) Has(key K) bool {
	_, ok := l.Get(key)
	return ok
}

// Delete 主动删除一个资源并释放。
func (l *Lease[K, V]) Delete(key K) {
	l.mu.Lock()
	e, exists := l.items[key]
	if exists {
		delete(l.items, key)
	}
	l.mu.Unlock()
	if exists {
		l.release(e.key, e.value)
	}
}

// Len 返回当前缓存中的条目数量。
func (l *Lease[K, V]) Len() int {
	l.mu.RLock()
	n := len(l.items)
	l.mu.RUnlock()
	return n
}

// Stop 停止到期处理循环并释放所有仍存活的资源。幂等，调用后该缓存不应再被使用。
func (l *Lease[K, V]) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopC)
		l.wg.Wait()

		l.mu.Lock()
		items := l.items
		l.items = make(map[K]*entry[K, V])
		l.buckets = make(map[int64][]*entry[K, V])
		l.mu.Unlock()

		for _, e := range items {
			l.release(e.key, e.value)
		}
	})
}

// expire 处理一个到期条目：空闲未超时则重新分桶，否则双重检查后释放。
func (l *Lease[K, V]) expire(e *entry[K, V], now int64) {
	last := e.lastAccess.Load()
	if remaining := int64(e.ttl) - (now - last); remaining > 0 {
		l.rebucket(e, now+remaining)
		return
	}

	l.mu.Lock()
	if l.items[e.key] != e {
		l.mu.Unlock()
		return
	}
	if now-e.lastAccess.Load() >= int64(e.ttl) {
		delete(l.items, e.key)
		l.mu.Unlock()
		l.release(e.key, e.value)
		return
	}
	l.mu.Unlock()

	// lastAccess 被并发更新，重新检查。
	l.expire(e, now)
}

// align 把时刻向上对齐到 Tick 边界，保证落到未来的桶。
func (l *Lease[K, V]) align(x int64) int64 {
	tickNs := int64(l.tick)
	return (x + tickNs - 1) / tickNs * tickNs
}

// rebucket 把条目重新放到 at 时刻对应的到期桶；若已失效则丢弃。
func (l *Lease[K, V]) rebucket(e *entry[K, V], at int64) {
	due := l.align(at)
	l.mu.Lock()
	if l.items[e.key] == e {
		l.buckets[due] = append(l.buckets[due], e)
	}
	l.mu.Unlock()
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
