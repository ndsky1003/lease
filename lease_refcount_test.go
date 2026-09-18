package lease

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// release 应幂等：同一个 release 闭包调用多次只释放一次引用，不误释放容器持有的引用。
func TestReleaseIdempotent(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v", time.Hour)
	_, release, ok := c.Get("k")
	if !ok {
		t.Fatal("Get 失败")
	}

	release()
	release() // 幂等：不应重复释放

	if released.Load() != 0 {
		t.Fatalf("release 后 value 仍被容器持有，不应释放，实际 %d", released.Load())
	}
	if c.Len() != 1 {
		t.Fatalf("条目应仍在容器中，Len=%d", c.Len())
	}

	c.Delete("k")
	if released.Load() != 1 {
		t.Fatalf("Delete 应释放一次，实际 %d", released.Load())
	}
}

// 并发 Get 全部 release 后，引用计数应精确归零，只由容器持有的引用兜底。
func TestConcurrentGetRefCount(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v", time.Hour)

	const n = 1000
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, ok := c.Get("k")
			if !ok {
				t.Error("Get 失败")
				return
			}
			release()
		}()
	}
	wg.Wait()

	if released.Load() != 0 {
		t.Fatalf("并发 Get 全部 release 后不应释放，实际 %d", released.Load())
	}

	c.Delete("k")
	if released.Load() != 1 {
		t.Fatalf("Delete 应释放一次，实际 %d", released.Load())
	}
}

// Stop 时若仍持有引用，value 应延迟到最后一个 release 才释放。
func TestStopDelaysReleaseWithHeldRefs(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})

	c.Set("k", "v", time.Hour)
	_, release, ok := c.Get("k")
	if !ok {
		t.Fatal("Get 失败")
	}

	c.Stop()
	if released.Load() != 0 {
		t.Fatal("Stop 后仍持有引用，不应释放")
	}

	release()
	if released.Load() != 1 {
		t.Fatalf("release 后应释放，实际 %d", released.Load())
	}
}

// Has 不应增加引用计数：若 Has 走 Get 会因不 release 导致引用泄漏、value 永不释放。
func TestHasNoRefCount(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:          5 * time.Millisecond,
		RenewInterval: time.Hour, // 让 Get 不续期，突出引用计数的作用
		OnEvict:       func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v", 20*time.Millisecond)

	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		c.Has("k")
		time.Sleep(2 * time.Millisecond)
	}

	waitFor(t, time.Second, func() bool { return released.Load() == 1 })
}

// Has 不应续期：高频 Has 不延长存活时间，条目仍按原 ttl 过期。
func TestHasDoesNotRenew(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "v", 30*time.Millisecond)

	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		c.Has("k")
		time.Sleep(2 * time.Millisecond)
	}

	if c.Len() != 0 {
		t.Fatalf("Has 不应续期，条目应按原 ttl 过期，Len=%d", c.Len())
	}
}

// 混合并发压力：Set（含覆盖）+ Get/release + 短 ttl 过期，配合 -race 检测竞态。
func TestConcurrentGetExpireRefCount(t *testing.T) {
	c := NewWithOptions(Options[int, int]{
		Tick:    time.Millisecond,
		OnEvict: func(key, value int) {},
	})

	const (
		setters = 4
		getters = 16
		ops     = 2000
	)
	var wg sync.WaitGroup

	for g := 0; g < setters; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.Set((base*ops+i)%100, i, 5*time.Millisecond)
			}
		}(g)
	}

	for g := 0; g < getters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				if _, release, ok := c.Get(i % 100); ok {
					release()
				}
			}
		}()
	}

	wg.Wait()
	c.Stop()
}
