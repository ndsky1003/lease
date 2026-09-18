package lease

import (
	"sync/atomic"
	"testing"
	"time"
)

type closer struct {
	closed atomic.Int32
}

func (c *closer) Close() error {
	c.closed.Add(1)
	return nil
}

func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("等待条件超时")
}

func TestSetGet(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "v", time.Minute)
	v, release, ok := c.Get("k")
	if !ok || v != "v" {
		t.Fatalf("Get 失败: v=%v ok=%v", v, ok)
	}
	release()
	if c.Len() != 1 {
		t.Fatalf("Len 应为 1，实际 %d", c.Len())
	}
}

func TestExpireAutoRelease(t *testing.T) {
	released := make(chan string, 1)
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		OnEvict: func(key, value string) {
			released <- key
		},
	})
	defer c.Stop()

	c.Set("k", "v", 20*time.Millisecond)
	select {
	case k := <-released:
		if k != "k" {
			t.Fatalf("释放的 key 错误: %v", k)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("资源未被自动释放")
	}
	if c.Len() != 0 {
		t.Fatalf("过期后 Len 应为 0，实际 %d", c.Len())
	}
}

func TestExpireWithCloser(t *testing.T) {
	c := NewWithOptions(Options[string, *closer]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	v := &closer{}
	c.Set("k", v, 20*time.Millisecond)

	waitFor(t, 2*time.Second, func() bool { return v.closed.Load() > 0 })
}

func TestDeleteCancels(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		OnEvict: func(key, value string) {
			released.Add(1)
		},
	})
	defer c.Stop()

	c.Set("k", "v", 50*time.Millisecond)
	c.Delete("k")

	time.Sleep(100 * time.Millisecond)
	if released.Load() != 1 {
		t.Fatalf("Delete 应释放一次，实际释放 %d 次", released.Load())
	}
	if c.Len() != 0 {
		t.Fatalf("Delete 后 Len 应为 0，实际 %d", c.Len())
	}
}

func TestOverwriteReleasesOldValue(t *testing.T) {
	var released atomic.Int32
	var last atomic.Value
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		OnEvict: func(key, value string) {
			released.Add(1)
			last.Store(value)
		},
	})
	defer c.Stop()

	c.Set("k", "v1", 30*time.Millisecond)
	c.Set("k", "v2", 80*time.Millisecond) // 覆盖时应立即释放 v1

	// v1 被覆盖释放一次，v2 过期后再释放一次，共 2 次，不重复也不遗漏
	waitFor(t, 2*time.Second, func() bool { return released.Load() == 2 })
	time.Sleep(50 * time.Millisecond)
	if released.Load() != 2 {
		t.Fatalf("覆盖 + 过期应释放 2 次，实际 %d 次", released.Load())
	}
	if v, _ := last.Load().(string); v != "v2" {
		t.Fatalf("最后一次释放应是 v2，实际 %q", v)
	}
}

func TestLongTTL(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: time.Millisecond})
	defer c.Stop()

	// 长 TTL 也应到期释放
	c.Set("k", "v", time.Second)

	waitFor(t, 3*time.Second, func() bool { return c.Len() == 0 })
}

func TestNoExpire(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "v", 0)
	time.Sleep(20 * time.Millisecond)
	if c.Len() != 1 {
		t.Fatalf("永不过期的条目应保留，实际 Len=%d", c.Len())
	}
}

func TestSlidingExpiration(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "v", 30*time.Millisecond)

	// 每 10ms Get 一次续期，持续 200ms，资源应始终存活
	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, release, ok := c.Get("k")
		if !ok {
			t.Fatal("续期期间资源不应被释放")
		}
		release()
		time.Sleep(10 * time.Millisecond)
	}

	// 停止续期，等待 ttl（30ms）后资源应被释放
	waitFor(t, time.Second, func() bool { return c.Len() == 0 })
}

func TestRenewCoalescing(t *testing.T) {
	c := NewWithOptions(Options[string, string]{
		Tick:          5 * time.Millisecond,
		RenewInterval: time.Hour, // 阈值很大，Get 几乎不续期
	})
	defer c.Stop()

	c.Set("k", "v", 30*time.Millisecond)

	// 高频 Get，但因合并阈值很大，不会真正续期
	deadline := time.Now().Add(50 * time.Millisecond)
	for time.Now().Before(deadline) {
		if _, release, ok := c.Get("k"); ok {
			release()
		}
		time.Sleep(5 * time.Millisecond)
	}

	// 资源仍按原 ttl（30ms）过期，未被续期延长
	waitFor(t, time.Second, func() bool { return c.Len() == 0 })
}

func TestStopReleasesAll(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, int]{
		Tick: 5 * time.Millisecond,
		OnEvict: func(key string, value int) {
			released.Add(1)
		},
	})

	c.Set("a", 1, time.Hour)
	c.Set("b", 2, time.Hour)
	c.Stop()

	if released.Load() != 2 {
		t.Fatalf("Stop 应释放所有资源，实际 %d", released.Load())
	}
}

// 引用计数应保证：Get 命中后，即使条目空闲过期被移出容器，value 也只在
// release 时才真正释放，消除 use-after-free。
func TestRefCountDelaysRelease(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:          5 * time.Millisecond,
		RenewInterval: time.Hour, // 让 Get 不续期，便于观察过期
		OnEvict:       func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v", 20*time.Millisecond)
	v, release, ok := c.Get("k")
	if !ok || v != "v" {
		t.Fatal("Get 失败")
	}

	// 等待 ttl 过期：条目已从容器移除，但因持有引用，value 不应被释放
	time.Sleep(60 * time.Millisecond)
	if c.Len() != 0 {
		t.Fatalf("过期后应已从容器移除，Len=%d", c.Len())
	}
	if released.Load() != 0 {
		t.Fatal("持有引用期间 value 不应被释放")
	}

	release()
	if released.Load() != 1 {
		t.Fatalf("release 后应释放一次，实际 %d", released.Load())
	}
}

// 覆盖时若旧 value 仍被引用，应延迟到最后一个引用释放，而非立即释放。
func TestRefCountOverwriteDelaysRelease(t *testing.T) {
	var released []string
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released = append(released, value) },
	})
	defer c.Stop()

	c.Set("k", "v1", time.Hour)
	_, release, ok := c.Get("k")
	if !ok {
		t.Fatal("Get 失败")
	}

	c.Set("k", "v2", time.Hour) // 覆盖：v1 应延迟释放

	if len(released) != 0 {
		t.Fatalf("覆盖后 v1 仍被引用，不应立即释放，实际已释放 %v", released)
	}

	release() // 释放 v1 的最后一个引用
	if len(released) != 1 || released[0] != "v1" {
		t.Fatalf("release 后应释放 v1，实际 %v", released)
	}
}
