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
	if v, ok := c.Get("k"); !ok || v != "v" {
		t.Fatalf("Get 失败: v=%v ok=%v", v, ok)
	}
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
		if _, ok := c.Get("k"); !ok {
			t.Fatal("续期期间资源不应被释放")
		}
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
		c.Get("k")
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
