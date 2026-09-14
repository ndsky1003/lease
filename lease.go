package lease

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// timer 是时间轮中的一个任务，对应一个缓存条目的过期定时。
// 它实现 task 接口供时间轮调度；expiration 用原子操作维护，
// 使得"续期"（Get 时延长生命周期）可以无锁完成。
type timer[K comparable, V any] struct {
	expiration atomic.Int64 // 绝对过期时间（UnixNano 纳秒），原子访问
	cancelled  atomic.Int32 // 是否已被取消（原子标记，惰性删除）
	lastRenew  atomic.Int64 // 上次续期时间（UnixNano 纳秒），用于续期合并

	key K
}

// cancel 标记该任务已取消。被取消的任务会在时间轮前进到对应槽位时被直接丢弃，
// 而不会触发释放。这样避免了对链表做查找删除，保证删除操作是 O(1)。
func (t *timer[K, V]) cancel() {
	t.cancelled.Store(1)
}

func (t *timer[K, V]) isCancelled() bool {
	return t.cancelled.Load() == 1
}

func (t *timer[K, V]) getExpiration() int64 {
	return t.expiration.Load()
}

// renew 无锁续期：把过期时间顺延 ttl。续期只是原子写，任务仍留在原槽位，
// 时间轮 flush 到该槽位时会发现"还没到期"从而惰性重调度。
//
// interval > 0 时启用"续期合并"：距上次续期不足 interval 则跳过，
// 避免高频访问时无谓地反复原子写。合并通过 CAS 保证并发下只有一个续期生效。
func (t *timer[K, V]) renew(ttl, interval time.Duration) {
	now := time.Now().UnixNano()
	if interval > 0 {
		last := t.lastRenew.Load()
		if now-last < int64(interval) {
			return // 距上次续期不足阈值，跳过
		}
		if !t.lastRenew.CompareAndSwap(last, now) {
			return // 其他 goroutine 已续期
		}
	}
	t.expiration.Store(now + int64(ttl))
}

// entry 是缓存中的一个条目。它不可变：覆盖一个 key 时是创建新 entry 并替换，
// 而非原地修改，因此 Get 在释放读锁后仍可安全地读取并续期该条目。
type entry[K comparable, V any] struct {
	key           K
	value         V
	timer         *timer[K, V] // 创建时的定时任务；ttl<=0 时为 nil
	ttl           time.Duration
	renewInterval time.Duration // 续期合并阈值
}

// Options 用于配置缓存。
type Options[K comparable, V any] struct {
	Tick          time.Duration // 时间轮每格时长，默认 1 秒
	WheelSize     int64         // 每层槽位数量，默认 64
	OnEvict       func(K, V)    // 释放回调；为 nil 时，若 value 实现了 io.Closer 则自动 Close
	RenewInterval time.Duration // 续期合并阈值：距上次续期不足该值则跳过；<=0 表示每次 Get 都续期
}

// Lease 是一个带空闲超时（滑动过期）的资源容器。
//
// 并发模型：
//   - Get 读 value 用读锁（多读者并发，几乎无竞争），续期完全无锁（原子写）；
//   - Set / Delete / 过期释放仅用写锁保护"检查 + 替换"的一致性。
type Lease[K comparable, V any] struct {
	wheel *DelayTimingWheel

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
	l.wheel = newDelayTimingWheel(opts.Tick, opts.WheelSize, func(tasks []task) {
		l.expireBatch(tasks)
	})
	l.wheel.Start()
	return l
}

// Set 添加或更新一个资源，并设置空闲存活时长 ttl。
// 若 key 已存在，旧的过期任务会被取消，资源会被新值替换。
// ttl <= 0 表示该资源永不过期。
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) {
	var tm *timer[K, V]
	if ttl > 0 {
		tm = &timer[K, V]{key: key}
		now := time.Now().UnixNano()
		tm.expiration.Store(now + int64(ttl))
		tm.lastRenew.Store(now)
	}

	l.mu.Lock()
	if old, ok := l.items[key]; ok && old.timer != nil {
		old.timer.cancel()
	}
	l.items[key] = &entry[K, V]{
		key:           key,
		value:         value,
		timer:         tm,
		ttl:           ttl,
		renewInterval: l.renewInterval,
	}
	l.mu.Unlock()

	if tm != nil {
		l.wheel.add(tm)
	}
}

// Get 读取一个资源，并顺延其生命周期（续期）。
// 返回的 ok 表示该 key 是否存在（且未过期）。
func (l *Lease[K, V]) Get(key K) (value V, ok bool) {
	l.mu.RLock()
	e, exists := l.items[key]
	l.mu.RUnlock()
	if !exists {
		return
	}
	if e.timer != nil {
		// 续期（无锁）；RenewInterval>0 时自动合并高频续期。
		e.timer.renew(e.ttl, e.renewInterval)
	}
	return e.value, true
}

// Has 判断 key 是否存在（且未过期）。
func (l *Lease[K, V]) Has(key K) bool {
	_, ok := l.Get(key)
	return ok
}

// Delete 主动删除一个资源并释放。
// 会取消对应的过期任务，避免后续重复释放。
func (l *Lease[K, V]) Delete(key K) {
	l.mu.Lock()
	e, exists := l.items[key]
	if !exists {
		l.mu.Unlock()
		return
	}
	if e.timer != nil {
		e.timer.cancel()
	}
	delete(l.items, key)
	l.mu.Unlock()

	l.release(e.key, e.value)
}

// Len 返回当前缓存中的条目数量。
func (l *Lease[K, V]) Len() int {
	l.mu.RLock()
	n := len(l.items)
	l.mu.RUnlock()
	return n
}

// Stop 停止时间轮并释放所有仍存活的资源。
// 调用后该缓存不应再被使用。
func (l *Lease[K, V]) Stop() {
	l.wheel.Stop()

	l.mu.Lock()
	items := l.items
	l.items = make(map[K]*entry[K, V])
	l.mu.Unlock()

	for _, e := range items {
		l.release(e.key, e.value)
	}
}

// expireBatch 由时间轮在整槽任务到期时调用，批量释放。
// 一次加锁处理整个槽位，避免为每个任务各抢一次锁。
// 通过比较当前 key 生效的 entry 指针，确保不会因覆盖/删除而误释放新资源。
func (l *Lease[K, V]) expireBatch(tasks []task) {
	var reAdd []*timer[K, V]

	l.mu.Lock()
	var released []*entry[K, V]
	for _, tk := range tasks {
		tm := tk.(*timer[K, V])
		// 双重检查：续期可能发生在时间轮判定过期之后，
		// 这里再看一眼是否已被续期，若是则重新投递。
		if tm.getExpiration() > time.Now().UnixNano() {
			reAdd = append(reAdd, tm)
			continue
		}
		e, ok := l.items[tm.key]
		if !ok || e.timer != tm {
			continue // 已删除或被覆盖
		}
		delete(l.items, tm.key)
		released = append(released, e)
	}
	l.mu.Unlock()

	for _, e := range released {
		l.release(e.key, e.value)
	}
	for _, tm := range reAdd {
		l.wheel.add(tm)
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
