package lease

import (
	"io"
	"sync"
	"sync/atomic"
	"time"
)

// bentry 是分桶实现的缓存条目。与 entry 的区别：它不持有定时任务，
// 而是按到期时刻落到桶里，由单个对齐 ticker 批量处理到期桶。
type bentry[K comparable, V any] struct {
	key   K
	value V
	ttl   time.Duration

	renewInterval time.Duration
	lastRenew     atomic.Int64
	lastAccess    atomic.Int64
}

func (e *bentry[K, V]) touch() {
	now := time.Now().UnixNano()
	if e.renewInterval > 0 {
		last := e.lastRenew.Load()
		if now-last < int64(e.renewInterval) {
			return
		}
		if !e.lastRenew.CompareAndSwap(last, now) {
			return
		}
	}
	e.lastAccess.Store(now)
}

// BucketOptions 用于配置分桶版缓存。
type BucketOptions[K comparable, V any] struct {
	Tick          time.Duration // 到期桶处理周期，默认 1 秒
	OnEvict       func(K, V)    // 释放回调；为 nil 时，若 value 实现 io.Closer 则自动 Close
	RenewInterval time.Duration // 访问合并阈值；<=0 表示每次访问都更新
}

// BucketLease 是分桶版的空闲超时资源容器：条目按到期时刻落到桶里，
// 由单个 ticker 每 Tick 批量处理到期桶，避免每次 Set 都创建定时任务。
type BucketLease[K comparable, V any] struct {
	tick time.Duration

	mu      sync.RWMutex
	items   map[K]*bentry[K, V]
	buckets map[int64][]*bentry[K, V] // 到期时刻(对齐到 Tick) -> 条目列表

	onEvict       func(K, V)
	renewInterval time.Duration

	stopC    chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

// NewBucket 使用默认配置创建分桶版缓存。
func NewBucket[K comparable, V any](onEvict func(K, V)) *BucketLease[K, V] {
	return NewBucketWithOptions(BucketOptions[K, V]{OnEvict: onEvict})
}

// NewBucketWithOptions 使用自定义配置创建分桶版缓存，并启动到期处理循环。
func NewBucketWithOptions[K comparable, V any](opts BucketOptions[K, V]) *BucketLease[K, V] {
	if opts.Tick <= 0 {
		opts.Tick = time.Second
	}
	l := &BucketLease[K, V]{
		tick:          opts.Tick,
		items:         make(map[K]*bentry[K, V]),
		buckets:       make(map[int64][]*bentry[K, V]),
		onEvict:       opts.OnEvict,
		renewInterval: opts.RenewInterval,
		stopC:         make(chan struct{}),
	}
	l.wg.Add(1)
	go l.run()
	return l
}

// run 是到期处理循环：对齐到 Tick 边界逐格推进，flush 当前到期桶。
func (l *BucketLease[K, V]) run() {
	defer l.wg.Done()
	tickNs := int64(l.tick)
	for {
		now := time.Now()
		next := now.Truncate(l.tick).Add(l.tick)
		t := time.NewTimer(time.Until(next))
		select {
		case <-t.C:
			n := time.Now().UnixNano()
			l.flush(n/tickNs*tickNs, n)
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
func (l *BucketLease[K, V]) flush(due, now int64) {
	l.mu.Lock()
	es := l.buckets[due]
	delete(l.buckets, due)
	l.mu.Unlock()

	for _, e := range es {
		l.expire(e, now)
	}
}

// Set 添加或更新一个资源，并设置空闲存活时长 ttl。ttl <= 0 表示永不过期。
func (l *BucketLease[K, V]) Set(key K, value V, ttl time.Duration) {
	l.mu.Lock()
	e := &bentry[K, V]{
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
}

// Get 读取一个资源，并记录本次访问（用于滑动过期）。
func (l *BucketLease[K, V]) Get(key K) (value V, ok bool) {
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
func (l *BucketLease[K, V]) Has(key K) bool {
	_, ok := l.Get(key)
	return ok
}

// Delete 主动删除一个资源并释放。
func (l *BucketLease[K, V]) Delete(key K) {
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
func (l *BucketLease[K, V]) Len() int {
	l.mu.RLock()
	n := len(l.items)
	l.mu.RUnlock()
	return n
}

// Stop 停止到期处理循环并释放所有仍存活的资源。
func (l *BucketLease[K, V]) Stop() {
	l.stopOnce.Do(func() {
		close(l.stopC)
		l.wg.Wait()

		l.mu.Lock()
		items := l.items
		l.items = make(map[K]*bentry[K, V])
		l.buckets = make(map[int64][]*bentry[K, V])
		l.mu.Unlock()

		for _, e := range items {
			l.release(e.key, e.value)
		}
	})
}

// expire 处理一个到期条目：空闲未超时则重新分桶，否则双重检查后释放。
func (l *BucketLease[K, V]) expire(e *bentry[K, V], now int64) {
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

// align 把时刻向上对齐到 Tick 边界，保证落到未来的桶，避免被推进中的 ticker 漏掉。
func (l *BucketLease[K, V]) align(x int64) int64 {
	tickNs := int64(l.tick)
	return (x + tickNs - 1) / tickNs * tickNs
}

// rebucket 把条目重新放到 at 时刻对应的到期桶；若已失效则丢弃。
func (l *BucketLease[K, V]) rebucket(e *bentry[K, V], at int64) {
	due := l.align(at)
	l.mu.Lock()
	if l.items[e.key] == e {
		l.buckets[due] = append(l.buckets[due], e)
	}
	l.mu.Unlock()
}

// release 执行资源的释放逻辑。
func (l *BucketLease[K, V]) release(key K, value V) {
	if l.onEvict != nil {
		l.onEvict(key, value)
		return
	}
	if closer, ok := any(value).(io.Closer); ok {
		_ = closer.Close()
	}
}
