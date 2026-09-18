package lease

import (
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestHas(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	if c.Has("k") {
		t.Fatal("不存在的 key 应返回 false")
	}
	c.Set("k", "v", time.Minute)
	if !c.Has("k") {
		t.Fatal("存在的 key 应返回 true")
	}
}

func TestGetMissing(t *testing.T) {
	c := NewWithOptions(Options[string, int]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	v, _, ok := c.Get("none")
	if ok {
		t.Fatal("不存在的 key 应返回 ok=false")
	}
	if v != 0 {
		t.Fatalf("不存在的 key 应返回零值，实际 %v", v)
	}
}

func TestDeleteMissing(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Delete("none") // 不应 panic，不应触发释放

	time.Sleep(20 * time.Millisecond)
	if released.Load() != 0 {
		t.Fatalf("删除不存在的 key 不应释放，实际 %d 次", released.Load())
	}
}

func TestDeleteThenReset(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "v1", 50*time.Millisecond)
	c.Delete("k")
	c.Set("k", "v2", time.Minute)

	if v, release, ok := c.Get("k"); !ok || v != "v2" {
		t.Fatalf("Delete 后重新 Set 应生效，v=%v ok=%v", v, ok)
	} else {
		release()
	}
}

func TestSetOverwriteChangesTTL(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "v1", 20*time.Millisecond)
	c.Set("k", "v2", 200*time.Millisecond) // 覆盖为长 ttl

	time.Sleep(60 * time.Millisecond) // 若按旧 ttl 20ms，此刻应已释放
	if c.Len() != 1 {
		t.Fatalf("覆盖后应按新 ttl 存活，Len=%d", c.Len())
	}
	if v, release, ok := c.Get("k"); !ok || v != "v2" {
		t.Fatalf("覆盖后应返回新值，v=%v ok=%v", v, ok)
	} else {
		release()
	}
}

func TestOverwriteOldBucketAfterNew(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	// 旧桶（100ms）晚于新桶（20ms）到期：覆盖时释放 v1，新桶到期释放 v2，
	// 旧桶 flush 时被指针判断丢弃，不重复释放 v1。
	c.Set("k", "v1", 100*time.Millisecond)
	c.Set("k", "v2", 20*time.Millisecond)

	waitFor(t, 2*time.Second, func() bool { return released.Load() == 2 })
	time.Sleep(150 * time.Millisecond) // 覆盖旧桶到期时刻
	if released.Load() != 2 {
		t.Fatalf("覆盖 + 过期应释放 2 次，旧桶 flush 不应重复释放，实际 %d 次", released.Load())
	}
}

func TestOverwriteToNoExpire(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v1", 20*time.Millisecond)
	c.Set("k", "v2", 0) // 覆盖为永不过期，覆盖时应释放 v1

	time.Sleep(60 * time.Millisecond) // 旧桶 20ms 已过期
	if released.Load() != 1 {
		t.Fatalf("覆盖应释放旧值 v1 一次，实际 %d 次", released.Load())
	}
	if c.Len() != 1 {
		t.Fatalf("永不过期条目应保留，Len=%d", c.Len())
	}
	if v, release, ok := c.Get("k"); !ok || v != "v2" {
		t.Fatalf("应返回新值 v2，v=%v ok=%v", v, ok)
	} else {
		release()
	}
}

func TestOverwriteFromNoExpire(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	c.Set("k", "v1", 0)                   // 永不过期，不落桶
	c.Set("k", "v2", 20*time.Millisecond) // 覆盖释放 v1，v2 改为有期

	waitFor(t, 2*time.Second, func() bool { return released.Load() == 2 })
	if c.Len() != 0 {
		t.Fatalf("有期条目应释放，Len=%d", c.Len())
	}
}

func TestTTLBoundaries(t *testing.T) {
	tick := 5 * time.Millisecond
	ttls := []time.Duration{
		1 * time.Millisecond, // 小于 Tick
		5 * time.Millisecond, // 等于 Tick
		6 * time.Millisecond, // 略大于 Tick，非整数倍
		7 * time.Millisecond,
		10 * time.Millisecond, // Tick 的整数倍
		23 * time.Millisecond,
	}
	for _, ttl := range ttls {
		t.Run(ttl.String(), func(t *testing.T) {
			var released atomic.Int32
			c := NewWithOptions(Options[string, int]{
				Tick:    tick,
				OnEvict: func(key string, value int) { released.Add(1) },
			})
			defer c.Stop()

			c.Set("k", 1, ttl)
			waitFor(t, 2*time.Second, func() bool { return released.Load() == 1 })
			if c.Len() != 0 {
				t.Fatalf("ttl=%v 应释放，Len=%d", ttl, c.Len())
			}
		})
	}
}

func TestBatchExpire(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[int, int]{
		Tick:    5 * time.Millisecond,
		OnEvict: func(key, value int) { released.Add(1) },
	})
	defer c.Stop()

	const n = 1000
	for i := 0; i < n; i++ {
		c.Set(i, i, 20*time.Millisecond)
	}

	waitFor(t, 3*time.Second, func() bool { return released.Load() == n })
	if c.Len() != 0 {
		t.Fatalf("批量过期后 Len 应为 0，实际 %d", c.Len())
	}
}

func TestStopIdempotent(t *testing.T) {
	c := NewWithOptions(Options[string, int]{Tick: 5 * time.Millisecond})
	c.Set("a", 1, time.Hour)
	c.Stop()
	c.Stop() // 第二次不应 panic
}

func TestConcurrentSetSameKey(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, int]{
		Tick:    time.Millisecond,
		OnEvict: func(key string, value int) { released.Add(1) },
	})

	const (
		goroutines = 16
		ops        = 1000
	)
	var wg sync.WaitGroup
	for g := 0; g < goroutines; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.Set("k", base*ops+i, time.Hour)
			}
		}(g)
	}
	wg.Wait()

	if c.Len() != 1 {
		t.Fatalf("并发覆盖后 Len 应为 1，实际 %d", c.Len())
	}
	c.Stop()
	// 每次覆盖释放一次旧值（首次除外），加上 Stop 释放最终值，恰好等于 Set 总次数
	if want := int32(goroutines * ops); released.Load() != want {
		t.Fatalf("并发覆盖应恰好释放 %d 次，实际 %d 次", want, released.Load())
	}
}

func TestOverwriteReleasesEachValue(t *testing.T) {
	var mu sync.Mutex
	var released []string
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		OnEvict: func(key, value string) {
			mu.Lock()
			released = append(released, value)
			mu.Unlock()
		},
	})
	defer c.Stop()

	c.Set("k", "v1", 20*time.Millisecond)
	c.Set("k", "v2", 20*time.Millisecond) // 覆盖释放 v1
	c.Set("k", "v3", 20*time.Millisecond) // 覆盖释放 v2

	// 等待全部三个值释放完成（释放时机与容器长度非原子一致，须以 released 为准）
	waitFor(t, 2*time.Second, func() bool {
		mu.Lock()
		n := len(released)
		mu.Unlock()
		return n == 3
	})

	mu.Lock()
	defer mu.Unlock()
	if len(released) != 3 {
		t.Fatalf("应释放 3 个值，实际 %d 个: %v", len(released), released)
	}
	if released[0] != "v1" || released[1] != "v2" || released[2] != "v3" {
		t.Fatalf("释放顺序应为 v1,v2,v3，实际 %v", released)
	}
}
