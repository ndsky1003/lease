package lease

import (
	"sync/atomic"
	"testing"
	"time"
)

// testTask 是 task 接口的一个测试实现，用于验证 DelayTimingWheel。
type testTask struct {
	expiration int64
	cancelled  atomic.Int32
	fired      atomic.Int32
}

func (t *testTask) isCancelled() bool    { return t.cancelled.Load() == 1 }
func (t *testTask) getExpiration() int64 { return t.expiration }
func (t *testTask) cancel()              { t.cancelled.Store(1) }

func TestDelayWheelExpire(t *testing.T) {
	var fired atomic.Int32
	tw := newDelayTimingWheel(5*time.Millisecond, 4, func(tasks []task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	n := 100
	for i := 0; i < n; i++ {
		tw.add(&testTask{expiration: time.Now().Add(20 * time.Millisecond).UnixNano()})
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}

func TestDelayWheelCrossLayers(t *testing.T) {
	var fired atomic.Int32
	tw := newDelayTimingWheel(time.Millisecond, 4, func(tasks []task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	// 远大于当前层一圈（4ms），会跨越多层时间轮
	tw.add(&testTask{expiration: time.Now().Add(time.Second).UnixNano()})

	waitFor(t, 3*time.Second, func() bool { return fired.Load() == 1 })
}

func TestDelayWheelCancel(t *testing.T) {
	var fired atomic.Int32
	tw := newDelayTimingWheel(5*time.Millisecond, 4, func(tasks []task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	tm := &testTask{expiration: time.Now().Add(50 * time.Millisecond).UnixNano()}
	tw.add(tm)
	tm.cancel()

	time.Sleep(100 * time.Millisecond)
	if fired.Load() != 0 {
		t.Fatalf("取消的任务不应触发，实际触发 %d 次", fired.Load())
	}
}

func TestDelayWheelBatch(t *testing.T) {
	var fired atomic.Int32
	tw := newDelayTimingWheel(5*time.Millisecond, 4, func(tasks []task) {
		fired.Add(int32(len(tasks)))
	})
	tw.Start()
	defer tw.Stop()

	// 同一槽位放多个任务，验证批量回调
	n := 10
	for i := 0; i < n; i++ {
		tw.add(&testTask{expiration: time.Now().Add(20 * time.Millisecond).UnixNano()})
	}

	waitFor(t, 2*time.Second, func() bool { return fired.Load() == int32(n) })
}
