// Package cache 实现了一个基于分层时间轮的缓存库。
//
// 核心思想：资源被添加进来后，会根据过期时间被放入时间轮的某个槽位；
// 时间轮每前进一格只处理一个槽位里的资源，过期即自动释放（调用回调）。
// 整个过程不存在全局扫描，从而避免类似 GC 那样"一次扫全表"造成的全局卡顿。
package lease

import (
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// task 是时间轮调度的最小单元。时间轮只关心任务的到期时间与取消状态，
// 不关心任务承载的具体数据类型，从而与上层（缓存等）解耦、保持可复用。
type task interface {
	isCancelled() bool
	getExpiration() int64
}

// TimingWheel 分层时间轮。
//
// 每一层由 wheelSize 个槽位组成，每个槽位代表 tick 时长，一圈覆盖 interval = tick * wheelSize。
// 当任务的过期时间超过当前层一圈的覆盖范围时，会被投递到上一层（上层 tick 等于当前层的 interval），
// 从而以指数级扩大可表示的时间跨度，同时每一格仍然只处理 O(1) 个槽位。
type TimingWheel struct {
	tick      time.Duration // 每格时长
	wheelSize int64         // 槽位数量
	interval  time.Duration // 一圈时长

	currentTick atomic.Int64 // 当前推进到的时间点（纳秒，原子访问）

	slots []*list.List // 槽位，每个槽位是一个定时任务链表
	mu    sync.Mutex   // 保护 slots

	overflow     *TimingWheel // 上一层时间轮
	overflowOnce sync.Once    // 惰性创建上层

	// onExpired 是整槽任务到期时的批量处理回调，由使用方注入。
	onExpired func([]task)

	ticker   *time.Ticker
	stopCh   chan struct{}
	stopOnce sync.Once
	done     chan struct{}
}

// newTimingWheel 创建一个时间轮，但不会启动，需要调用 Start。
func newTimingWheel(tick time.Duration, wheelSize int64, onExpired func([]task)) *TimingWheel {
	slots := make([]*list.List, wheelSize)
	for i := range slots {
		slots[i] = list.New()
	}
	return &TimingWheel{
		tick:      tick,
		wheelSize: wheelSize,
		interval:  tick * time.Duration(wheelSize),
		slots:     slots,
		onExpired: onExpired,
		stopCh:    make(chan struct{}),
		done:      make(chan struct{}),
	}
}

// Start 启动时间轮的后台驱动协程。
// currentTick 会向下对齐到 tick 边界，保证后续槽位索引计算与任务投递保持一致。
func (tw *TimingWheel) Start() {
	tw.ticker = time.NewTicker(tw.tick)
	now := time.Now().UnixNano()
	now = now / int64(tw.tick) * int64(tw.tick)
	tw.currentTick.Store(now)
	go tw.run()
}

// Stop 停止时间轮，并级联停止上层时间轮。
func (tw *TimingWheel) Stop() {
	tw.stopOnce.Do(func() {
		close(tw.stopCh)
		if tw.ticker != nil {
			tw.ticker.Stop()
		}
		<-tw.done
		if tw.overflow != nil {
			tw.overflow.Stop()
		}
	})
}

// run 是时间轮的驱动循环，每 tick 前进一次。
func (tw *TimingWheel) run() {
	defer close(tw.done)
	for {
		select {
		case <-tw.ticker.C:
			tw.advance(time.Now().UnixNano())
		case <-tw.stopCh:
			return
		}
	}
}

// advance 将时间轮推进到 now。若中间有遗漏（例如协程调度卡顿导致多个 tick 没处理），
// 会逐格补齐，确保不会漏掉任何过期的槽位。
func (tw *TimingWheel) advance(now int64) {
	tick := int64(tw.tick)
	for {
		current := tw.currentTick.Load()
		if current+tick > now {
			return
		}
		tw.currentTick.Store(current + tick)
		tw.advanceOne(current + tick)
	}
}

// advanceOne 处理时间点 t 对应的槽位：
//   - 已取消的任务直接丢弃；
//   - 已到期的任务取出并交给 onExpired 批量处理；
//   - 尚未到期的任务（跨圈放入、expiration 仍大于 t）重新投递回时间轮。
func (tw *TimingWheel) advanceOne(t int64) {
	idx := (t / int64(tw.tick)) % tw.wheelSize
	bucket := tw.slots[idx]

	tw.mu.Lock()
	var expired, reAdd []task
	for e := bucket.Front(); e != nil; {
		tm := e.Value.(task)
		next := e.Next()
		bucket.Remove(e)
		if tm.isCancelled() {
			// 已取消，丢弃即可
		} else if tm.getExpiration() <= t {
			expired = append(expired, tm)
		} else {
			reAdd = append(reAdd, tm)
		}
		e = next
	}
	tw.mu.Unlock()

	// 整槽任务交给一个协程批量处理，一次加锁完成校验与删除，降低锁竞争。
	if len(expired) > 0 {
		go tw.onExpired(expired)
	}
	for _, tm := range reAdd {
		tw.add(tm)
	}
}

// add 将任务投递到合适的层与槽位。
func (tw *TimingWheel) add(tm task) bool {
	now := tw.currentTick.Load()
	tick := int64(tw.tick)
	exp := tm.getExpiration()
	if exp <= now {
		return false // 已过期
	}
	// 向上对齐到 tick 边界，确保槽位索引与 advance 处理的槽位一致。
	aligned := (exp + tick - 1) / tick * tick
	if aligned < now+int64(tw.interval) {
		// 落在当前层一圈范围内
		idx := (aligned / tick) % tw.wheelSize
		tw.mu.Lock()
		tw.slots[idx].PushBack(tm)
		tw.mu.Unlock()
		return true
	}
	// 超出当前层覆盖范围，交给上一层
	tw.overflowOnce.Do(func() {
		tw.overflow = newTimingWheel(tw.interval, tw.wheelSize, tw.onExpired)
		tw.overflow.Start()
	})
	return tw.overflow.add(tm)
}
