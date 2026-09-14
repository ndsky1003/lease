package lease

import (
	"container/heap"
	"container/list"
	"sync"
	"sync/atomic"
	"time"
)

// 本文件实现了基于延迟队列（最小堆）的分层时间轮 DelayTimingWheel。
//
// 与简单版 TimingWheel（见 timing_wheel.go）的核心区别仅在于驱动方式：
//   - 简单版：每层一个固定 time.Ticker，每 tick 前进一格，即使槽位为空也会醒来（空转）；
//   - 本版本：用一个 DelayQueue（最小堆）记录"有任务的槽位"的到期时间，
//     只在最早到期的槽位到期时才被唤醒处理，无任务时完全休眠，零 CPU 空转。
//
// 两者的分层、投递、取消语义完全一致，因此方便对照学习。

// slot 是时间轮中的一个槽位，对应一个固定的时间点（expiration）。
type slot struct {
	expiration int64 // 槽位对应的时间点（纳秒，原子访问）
	index      int   // 在延迟队列堆中的位置，-1 表示不在堆中

	mu     sync.Mutex
	timers *list.List // 任务链表
}

func newSlot() *slot {
	return &slot{index: -1, timers: list.New()}
}

// setExpiration 设置槽位的到期时间，返回是否发生变化。
// 只有变化时才需要将槽位（重新）入队，从而保证同一圈内不会重复入队。
func (s *slot) setExpiration(exp int64) bool {
	return atomic.SwapInt64(&s.expiration, exp) != exp
}

func (s *slot) getExpiration() int64 {
	return atomic.LoadInt64(&s.expiration)
}

// slotHeap 实现 container/heap.Interface，按槽位到期时间维护一个最小堆。
type slotHeap []*slot

func (h slotHeap) Len() int { return len(h) }
func (h slotHeap) Less(i, j int) bool {
	return h[i].getExpiration() < h[j].getExpiration()
}
func (h slotHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index = i
	h[j].index = j
}
func (h *slotHeap) Push(x any) {
	s := x.(*slot)
	s.index = len(*h)
	*h = append(*h, s)
}
func (h *slotHeap) Pop() any {
	old := *h
	n := len(old)
	s := old[n-1]
	old[n-1] = nil // 避免堆持有引用
	s.index = -1
	*h = old[:n-1]
	return s
}

// delayQueue 是一个按到期时间排序的延迟队列，底层是最小堆。
// 它保证只有"最早到期的槽位"到期时才被取出，从而避免固定 ticker 的空转。
type delayQueue struct {
	C chan *slot // 已到期的槽位会从这里送出

	mu       sync.Mutex
	pq       slotHeap
	sleeping int32         // 原子标记：poll 是否正处于休眠等待
	wakeupC  chan struct{} // 用于唤醒休眠中的 poll
}

func newDelayQueue() *delayQueue {
	return &delayQueue{
		C:       make(chan *slot),
		wakeupC: make(chan struct{}),
	}
}

// offer 将槽位入队（或更新其在堆中的位置）。若它成为新的最早到期槽位，
// 则唤醒休眠中的 poll 协程。
func (dq *delayQueue) offer(s *slot) {
	dq.mu.Lock()
	if s.index >= 0 {
		heap.Fix(&dq.pq, s.index) // 已在堆中，更新位置
	} else {
		heap.Push(&dq.pq, s) // 新入队
	}
	top := dq.pq[0]
	dq.mu.Unlock()

	if top == s {
		// 成为堆顶，唤醒 poll（仅当其正处于休眠状态，避免无谓唤醒）。
		if atomic.CompareAndSwapInt32(&dq.sleeping, 1, 0) {
			dq.wakeupC <- struct{}{}
		}
	}
}

// poll 循环从堆中取出最早到期的槽位，发送到 C。无到期项时休眠，
// 直到被 offer 唤醒、或等待到最近一个槽位的到期时间、或收到退出信号。
func (dq *delayQueue) poll(exitC chan struct{}) {
	for {
		now := time.Now().UnixNano()

		dq.mu.Lock()
		var s *slot
		var delta int64
		if dq.pq.Len() == 0 {
			delta = 0 // 堆空
		} else {
			top := dq.pq[0]
			if top.getExpiration() <= now {
				s = heap.Pop(&dq.pq).(*slot) // 已到期，取出
			} else {
				delta = top.getExpiration() - now // 还需等待
			}
		}
		if s == nil {
			// 与上面的堆检查保持原子性，避免与 offer 产生竞态。
			atomic.StoreInt32(&dq.sleeping, 1)
		}
		dq.mu.Unlock()

		if s != nil {
			select {
			case dq.C <- s:
			case <-exitC:
				return
			}
			continue
		}

		if delta == 0 {
			// 堆空，等待被唤醒。
			select {
			case <-dq.wakeupC:
			case <-exitC:
				return
			}
		} else {
			// 堆顶尚未到期，睡到到期时间；期间可被唤醒。
			t := time.NewTimer(time.Duration(delta))
			select {
			case <-dq.wakeupC:
				t.Stop()
			case <-t.C:
				// 自然到期醒来。若 sleeping 已被 offer 置 0，
				// 说明有 offer 正在阻塞发送 wakeupC，需 drain 以解除其阻塞。
				if atomic.SwapInt32(&dq.sleeping, 0) == 0 {
					<-dq.wakeupC
				}
			case <-exitC:
				t.Stop()
				return
			}
		}
	}
}

// DelayTimingWheel 是基于延迟队列的分层时间轮。
type DelayTimingWheel struct {
	tick      time.Duration // 每格时长
	wheelSize int64         // 槽位数量
	interval  time.Duration // 一圈时长

	currentTick atomic.Int64 // 当前推进到的时间点（纳秒）

	slots []*slot
	queue *delayQueue

	overflow     *DelayTimingWheel // 上一层时间轮
	overflowOnce sync.Once         // 惰性创建上层

	onExpired func([]task) // 整槽到期时的批量处理回调

	exitC chan struct{}
	wg    sync.WaitGroup
}

func newDelayTimingWheel(tick time.Duration, wheelSize int64, onExpired func([]task)) *DelayTimingWheel {
	slots := make([]*slot, wheelSize)
	for i := range slots {
		slots[i] = newSlot()
	}
	return &DelayTimingWheel{
		tick:      tick,
		wheelSize: wheelSize,
		interval:  tick * time.Duration(wheelSize),
		slots:     slots,
		queue:     newDelayQueue(),
		onExpired: onExpired,
		exitC:     make(chan struct{}),
	}
}

// Start 启动时间轮的后台协程。
func (tw *DelayTimingWheel) Start() {
	now := time.Now().UnixNano()
	now = now / int64(tw.tick) * int64(tw.tick)
	tw.currentTick.Store(now)

	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		tw.queue.poll(tw.exitC)
	}()

	tw.wg.Add(1)
	go func() {
		defer tw.wg.Done()
		tw.run()
	}()
}

// run 处理延迟队列送出的到期槽位。
func (tw *DelayTimingWheel) run() {
	for {
		select {
		case s := <-tw.queue.C:
			tw.advance(s.getExpiration())
			tw.flush(s)
		case <-tw.exitC:
			return
		}
	}
}

// Stop 停止时间轮，并级联停止上层时间轮。
func (tw *DelayTimingWheel) Stop() {
	close(tw.exitC)
	tw.wg.Wait()
	if tw.overflow != nil {
		tw.overflow.Stop()
	}
}

// advance 将时钟直接推进到 exp（无需逐格前进），并级联推进上层时钟。
func (tw *DelayTimingWheel) advance(exp int64) {
	tick := int64(tw.tick)
	current := tw.currentTick.Load()
	if exp >= current+tick {
		tw.currentTick.Store(truncate(exp, tick))
		if tw.overflow != nil {
			tw.overflow.advance(exp)
		}
	}
}

// add 将任务投递到合适的层与槽位。
func (tw *DelayTimingWheel) add(tm task) bool {
	tick := int64(tw.tick)
	now := tw.currentTick.Load()
	exp := tm.getExpiration()
	if exp <= now {
		return false // 已过期
	}
	// 向上对齐到 tick 边界，确保槽位索引与 advance 处理的槽位一致。
	aligned := (exp + tick - 1) / tick * tick
	if aligned < now+int64(tw.interval) {
		// 落在当前层一圈范围内
		idx := (aligned / tick) % tw.wheelSize
		s := tw.slots[idx]
		s.mu.Lock()
		s.timers.PushBack(tm)
		s.mu.Unlock()
		if s.setExpiration(aligned) {
			tw.queue.offer(s)
		}
		return true
	}
	// 超出当前层覆盖范围，交给上一层
	tw.overflowOnce.Do(func() {
		tw.overflow = newDelayTimingWheel(tw.interval, tw.wheelSize, tw.onExpired)
		tw.overflow.Start()
	})
	return tw.overflow.add(tm)
}

// flush 处理一个到期槽位：
//   - 已取消的任务直接丢弃；
//   - 已到期的任务交给 onExpired 批量处理；
//   - 尚未到期的任务（跨圈放入）重新投递（实际会落入上层）。
func (tw *DelayTimingWheel) flush(s *slot) {
	exp := s.getExpiration()
	s.setExpiration(-1) // 重置，允许下一圈重新入队

	s.mu.Lock()
	var expired, reAdd []task
	for e := s.timers.Front(); e != nil; {
		tm := e.Value.(task)
		next := e.Next()
		s.timers.Remove(e)
		if tm.isCancelled() {
			// 已取消，丢弃
		} else if tm.getExpiration() <= exp {
			expired = append(expired, tm)
		} else {
			reAdd = append(reAdd, tm)
		}
		e = next
	}
	s.mu.Unlock()

	if len(expired) > 0 {
		go tw.onExpired(expired)
	}
	for _, tm := range reAdd {
		tw.add(tm)
	}
}

// truncate 将 x 向下对齐到 unit 的整数倍。
func truncate(x, unit int64) int64 {
	return x / unit * unit
}
