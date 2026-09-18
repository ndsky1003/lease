package lease

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// entry 是缓存中的一个条目。到期检测由内置的单层分桶时间轮统一调度：
// 每个有期条目按到期时刻落入时间轮桶，到期时做空闲超时判定并决定释放或重新落桶。
//
// refs 是引用计数：初始为 1（代表 items map 持有），每次 Get 命中 +1，调用方
// 用完需调用返回的 release 函数 -1。只有当 refs 归零时才真正释放 value，从而
// 消除「Get 返回后 value 被并发过期释放」的 use-after-free。
type entry[K comparable, V any] struct {
	key   K
	value V
	ttl   time.Duration

	renewInterval time.Duration // 访问合并阈值
	lastRenew     atomic.Int64  // 上次更新 lastAccess 的时间（UnixNano），用于访问合并
	lastAccess    atomic.Int64  // 最后访问时间（UnixNano）

	refs      atomic.Int64 // 引用计数：items 持有 1，每次 Get +1，release -1，归零才释放
	cancelled atomic.Bool  // 是否已取消（被覆盖/删除），到期回调据此丢弃
}

// newEntry 创建一个引用计数为 1（items 持有）的条目。
func newEntry[K comparable, V any](key K, value V, ttl, renewInterval time.Duration) *entry[K, V] {
	e := &entry[K, V]{
		key:           key,
		value:         value,
		ttl:           ttl,
		renewInterval: renewInterval,
	}
	e.refs.Store(1)
	return e
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

// unref 释放一个引用，返回是否应执行物理释放（即 refs 恰好从 1 降到 0）。
// refs 归零后再次调用返回 false，保证 value 恰好释放一次（幂等）。
func (e *entry[K, V]) unref() bool {
	for {
		n := e.refs.Load()
		if n <= 0 {
			return false
		}
		if e.refs.CompareAndSwap(n, n-1) {
			return n == 1
		}
	}
}

// Options 用于配置缓存。
type Options[K comparable, V any] struct {
	Tick          time.Duration // 时间轮 tick（到期检查粒度），默认 1 秒
	OnEvict       func(K, V)    // 释放回调；为 nil 时，若 value 实现了 io.Closer 则自动 Close
	RenewInterval time.Duration // 访问合并阈值：距上次更新不足该值则跳过；<=0 表示每次访问都更新
}

// Lease 是一个带空闲超时（滑动过期）的资源容器。
//
// 过期检测由内置时间轮完成：Set 时把条目按到期时刻落入到期桶，单个对齐 ticker
// 每 Tick 批量处理到期桶——空闲超时则从容器移除，否则按剩余时间重新落桶。
// value 的真正释放由引用计数保证：Get 命中后调用方持有引用，直到调用返回的
// release 才释放，因此 Get 返回的 value 在 release 之前始终有效。
type Lease[K comparable, V any] struct {
	tick    time.Duration
	startNs int64 // run 启动时刻，用于首次 flush 覆盖启动至今的边界

	wheelMu sync.Mutex
	lastDue int64                    // 已 flush 到的最晚边界，用于落桶时检测已过期
	buckets map[int64][]*entry[K, V] // 到期时刻（对齐到 Tick）-> 条目列表

	stopC    chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup

	mu    sync.RWMutex
	items map[K]*entry[K, V]

	onEvict       func(K, V)
	renewInterval time.Duration
}

// New 使用默认配置创建一个缓存。
func New[K comparable, V any](onEvict func(K, V)) *Lease[K, V] {
	return NewWithOptions(Options[K, V]{OnEvict: onEvict})
}

// NewWithOptions 使用自定义配置创建一个缓存，并启动时间轮到期处理循环。
func NewWithOptions[K comparable, V any](opts Options[K, V]) *Lease[K, V] {
	if opts.Tick <= 0 {
		opts.Tick = time.Second
	}
	l := &Lease[K, V]{
		tick:          opts.Tick,
		startNs:       time.Now().UnixNano(),
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

// Set 添加或更新一个资源，并设置空闲存活时长 ttl。
// 若 key 已存在，旧值会被移除（若无人持有引用则立即释放），再由新值替换。ttl <= 0 表示永不过期。
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) {
	l.mu.Lock()
	old := l.items[key]
	if old != nil {
		old.cancelled.Store(true)
	}
	e := newEntry(key, value, ttl, l.renewInterval)
	l.items[key] = e
	if ttl > 0 {
		now := time.Now().UnixNano()
		e.lastAccess.Store(now)
		e.lastRenew.Store(now)
		l.put(e, l.align(now+int64(ttl)))
	}
	l.mu.Unlock()

	if old != nil {
		l.releaseRef(old)
	}
}

// Get 读取一个资源，记录本次访问（续期）并增加引用计数。
//
// 命中时返回的 release 必须在用完后调用（通常 defer release()）以释放引用；
// 在 release 之前，value 不会被释放。release 幂等，可安全重复调用。未命中时 release 为 nil。
func (l *Lease[K, V]) Get(key K) (value V, release func(), ok bool) {
	l.mu.RLock()
	e, exists := l.items[key]
	if exists {
		if e.ttl > 0 {
			e.touch()
		}
		e.refs.Add(1)
	}
	l.mu.RUnlock()
	if !exists {
		return
	}
	var released atomic.Int32
	return e.value, func() {
		if released.CompareAndSwap(0, 1) {
			l.releaseRef(e)
		}
	}, true
}

// Has 判断 key 是否存在。纯探测：不续期、不增加引用计数。
func (l *Lease[K, V]) Has(key K) bool {
	l.mu.RLock()
	_, exists := l.items[key]
	l.mu.RUnlock()
	return exists
}

// Delete 主动删除一个资源。若无人持有引用则立即释放，否则延迟到最后一个引用释放。
func (l *Lease[K, V]) Delete(key K) {
	l.mu.Lock()
	e, exists := l.items[key]
	if exists {
		delete(l.items, key)
		e.cancelled.Store(true)
	}
	l.mu.Unlock()
	if exists {
		l.releaseRef(e)
	}
}

// Len 返回当前容器中的条目数量。
func (l *Lease[K, V]) Len() int {
	l.mu.RLock()
	n := len(l.items)
	l.mu.RUnlock()
	return n
}

// Stop 停止时间轮并释放所有仍存活的资源。幂等，调用后该缓存不应再被使用。
func (l *Lease[K, V]) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopC)
		l.wg.Wait()

		l.mu.Lock()
		items := l.items
		l.items = make(map[K]*entry[K, V])
		l.mu.Unlock()

		l.wheelMu.Lock()
		l.buckets = make(map[int64][]*entry[K, V])
		l.wheelMu.Unlock()

		for _, e := range items {
			l.releaseRef(e)
		}
	})
}

// put 把条目落入到期桶。若 due 已过期（不晚于已 flush 的最晚边界），则顺延到
// 下一个边界，避免投递到已被 flush 删除的桶而永久丢失。
func (l *Lease[K, V]) put(e *entry[K, V], due int64) {
	l.wheelMu.Lock()
	if due <= l.lastDue {
		due = l.lastDue + int64(l.tick)
	}
	l.buckets[due] = append(l.buckets[due], e)
	l.wheelMu.Unlock()
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
				// 首次 flush 从 run 启动时刻对齐到 Tick 边界开始，
				// 覆盖启动至今可能被调度延迟跳过的所有边界，避免丢任务。
				lastDue = l.startNs/tickNs*tickNs - tickNs
			}
			// 逐个 flush 从上次到当前之间的每个边界，避免调度延迟漏掉到期桶。
			for d := lastDue + tickNs; d <= cur; d += tickNs {
				l.flush(d)
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
func (l *Lease[K, V]) flush(due int64) {
	l.wheelMu.Lock()
	l.lastDue = due
	es := l.buckets[due]
	delete(l.buckets, due)
	l.wheelMu.Unlock()

	for _, e := range es {
		if e.cancelled.Load() {
			continue
		}
		l.expire(e)
	}
}

// expire 是到期回调：空闲未超时则重新落桶，否则从容器移除。
// 用循环而非递归重试「lastAccess 被并发更新」的情况，避免极端并发下的深递归栈溢出。
func (l *Lease[K, V]) expire(e *entry[K, V]) {
	for {
		now := time.Now().UnixNano()
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
			l.releaseRef(e)
			return
		}
		l.mu.Unlock()
	}
}

// rebucket 把条目重新落到 at 时刻对应的到期桶；若条目已被替换或删除则丢弃。
func (l *Lease[K, V]) rebucket(e *entry[K, V], at int64) {
	l.mu.Lock()
	if l.items[e.key] == e {
		l.put(e, l.align(at))
	}
	l.mu.Unlock()
}

// align 把时刻向上对齐到 Tick 边界，保证落到未来的桶。
func (l *Lease[K, V]) align(x int64) int64 {
	tickNs := int64(l.tick)
	return (x + tickNs - 1) / tickNs * tickNs
}

// releaseRef 释放一个引用（items 持有或调用方持有），归零时物理释放 value。
func (l *Lease[K, V]) releaseRef(e *entry[K, V]) {
	if e.unref() {
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
