package lease

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 高频续期 + 短 ttl：到期检测持续触发 rebucket/释放，同时 Get 持续续期。
// 配合 -race 检测竞态，并验证 expire 的循环重试不会 panic。
func TestExpireRenewContention(t *testing.T) {
	c := NewWithOptions(Options[int, int]{
		Tick:    time.Millisecond,
		OnEvict: func(key, value int) {},
	})
	defer c.Stop()

	const keys = 100
	for i := 0; i < keys; i++ {
		c.Set(i, i, 10*time.Millisecond)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-stop:
					return
				default:
					for i := 0; i < keys; i++ {
						if _, release, ok := c.Get(i); ok {
							release()
						}
					}
				}
			}
		}()
	}

	time.Sleep(200 * time.Millisecond)
	close(stop)
	wg.Wait()
}

// 大量覆盖同一 key：每次覆盖恰好释放一次旧值，最后一次由 Stop 释放，总数精确。
func TestMassiveOverwriteExactRelease(t *testing.T) {
	var released atomic.Int64
	c := NewWithOptions(Options[int, int]{
		Tick:    time.Millisecond,
		OnEvict: func(key, value int) { released.Add(1) },
	})

	const n = 10000
	for i := 0; i < n; i++ {
		c.Set(1, i, time.Hour)
	}
	if got := released.Load(); got != n-1 {
		t.Fatalf("覆盖应释放 %d 次，实际 %d", n-1, got)
	}

	c.Stop()
	if got := released.Load(); got != n {
		t.Fatalf("Stop 后应释放 %d 次，实际 %d", n, got)
	}
}

// ttl 远小于 tick 也能正常过期，不 panic、不永久卡死。
func TestTTLVerySmall(t *testing.T) {
	c := NewWithOptions(Options[int, int]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set(1, 1, time.Nanosecond)
	waitFor(t, time.Second, func() bool { return c.Len() == 0 })
}

// Stop 与并发 Set/Get/Delete 混合：验证不 panic、无数据竞态（配合 -race）。
// 注：Stop 后继续使用属契约违反，本测试仅验证不 panic/不竞态。
func TestStopConcurrentAccess(t *testing.T) {
	for i := 0; i < 10; i++ {
		c := NewWithOptions(Options[int, int]{Tick: time.Millisecond})
		c.Set(1, 1, time.Hour)

		var wg sync.WaitGroup
		for g := 0; g < 4; g++ {
			wg.Add(1)
			go func() {
				defer wg.Done()
				for j := 0; j < 200; j++ {
					c.Set(j, j, time.Hour)
					if _, release, ok := c.Get(j % 10); ok {
						release()
					}
					c.Delete(j)
				}
			}()
		}
		go c.Stop()
		wg.Wait()
		c.Stop()
	}
}

// 永不过期条目（ttl<=0）的引用计数：Get 加引用、release 减引用，Delete 时释放。
func TestNoExpireRefCount(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v", 0) // 永不过期
	_, release, ok := c.Get("k")
	if !ok {
		t.Fatal("Get 失败")
	}

	c.Delete("k") // 仍持有引用，不应立即释放
	if released.Load() != 0 {
		t.Fatal("Delete 后仍持有引用，不应释放")
	}

	release()
	if released.Load() != 1 {
		t.Fatalf("release 后应释放，实际 %d", released.Load())
	}
}
