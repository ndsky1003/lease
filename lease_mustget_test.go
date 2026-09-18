package lease

import (
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// 命中时复用已存在的 value，不调用 gen。
func TestMustGetHit(t *testing.T) {
	var genCalls atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		Gen: func(key string) (string, error) {
			genCalls.Add(1)
			return "generated", nil
		},
	})
	defer c.Stop()

	c.Set("k", "existing", time.Minute)
	v, release, err := c.MustGet("k", time.Minute)
	if err != nil {
		t.Fatalf("MustGet 不应返回 err: %v", err)
	}
	release()
	if v != "existing" {
		t.Fatalf("命中应返回已存在的值，实际 %q", v)
	}
	if genCalls.Load() != 0 {
		t.Fatalf("命中不应调用 gen，实际调用 %d 次", genCalls.Load())
	}
}

// 未命中时调用 gen 加载并写入容器。
func TestMustGetMiss(t *testing.T) {
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		Gen:  func(key string) (string, error) { return "generated", nil },
	})
	defer c.Stop()

	v, release, err := c.MustGet("k", time.Minute)
	if err != nil {
		t.Fatalf("MustGet 不应返回 err: %v", err)
	}
	release()
	if v != "generated" {
		t.Fatalf("未命中应返回 gen 的值，实际 %q", v)
	}
	if c.Len() != 1 {
		t.Fatalf("未命中应写入容器，Len=%d", c.Len())
	}
}

// gen 返回 err 时，MustGet 返回 err、release 为 nil、不写入容器。
func TestMustGetGenError(t *testing.T) {
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		Gen:  func(key string) (string, error) { return "", errors.New("boom") },
	})
	defer c.Stop()

	_, release, err := c.MustGet("k", time.Minute)
	if err == nil {
		t.Fatal("gen 返回 err 时 MustGet 应返回 err")
	}
	if release != nil {
		t.Fatal("err 时 release 应为 nil")
	}
	if c.Len() != 0 {
		t.Fatalf("err 时不应写入容器，Len=%d", c.Len())
	}
}

// MustGet 返回的引用在 release 之前阻止释放（引用计数语义一致）。
func TestMustGetRelease(t *testing.T) {
	var released atomic.Int32
	c := NewWithOptions(Options[string, string]{
		Tick:          5 * time.Millisecond,
		RenewInterval: time.Hour, // 让命中不续期，便于观察过期
		Gen:           func(key string) (string, error) { return "generated", nil },
		OnEvict:       func(key, value string) { released.Add(1) },
	})
	defer c.Stop()

	v, release, err := c.MustGet("k", 20*time.Millisecond)
	if err != nil || v != "generated" {
		t.Fatalf("MustGet 失败: v=%q err=%v", v, err)
	}

	time.Sleep(60 * time.Millisecond)
	if c.Len() != 0 {
		t.Fatalf("过期后应已移除，Len=%d", c.Len())
	}
	if released.Load() != 0 {
		t.Fatal("持有引用期间不应释放")
	}

	release()
	if released.Load() != 1 {
		t.Fatalf("release 后应释放一次，实际 %d", released.Load())
	}
}

// 并发未命中同一 key：gen 可能被多次调用（击穿），但双重检查保证只写入一个 entry，
// 且所有 goroutine 拿到的是同一个 value。
func TestMustGetConcurrentMiss(t *testing.T) {
	var genCalls atomic.Int32
	c := NewWithOptions(Options[int, int]{
		Tick: time.Millisecond,
		Gen: func(key int) (int, error) {
			time.Sleep(5 * time.Millisecond) // 放大击穿窗口
			return int(genCalls.Add(1)), nil
		},
	})
	defer c.Stop()

	const n = 100
	var wg sync.WaitGroup
	values := make([]int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			v, release, err := c.MustGet(1, time.Hour)
			if err != nil {
				t.Error(err)
				return
			}
			values[i] = v
			release()
		}(i)
	}
	wg.Wait()

	if genCalls.Load() < 1 {
		t.Fatal("gen 应至少调用一次")
	}
	for i := 1; i < n; i++ {
		if values[i] != values[0] {
			t.Fatalf("并发未命中应返回同一个 value，values[0]=%d values[%d]=%d", values[0], i, values[i])
		}
	}
	if c.Len() != 1 {
		t.Fatalf("双重检查应保证只写入一个 entry，Len=%d", c.Len())
	}
}

// 混合并发压力：MustGet + Get + Set 并发，配合 -race 检测竞态。
func TestMustGetConcurrentStress(t *testing.T) {
	c := NewWithOptions(Options[int, int]{
		Tick: time.Millisecond,
		Gen:  func(key int) (int, error) { return key * 10, nil },
	})
	defer c.Stop()

	const (
		keys    = 100
		workers = 16
		ops     = 2000
	)
	var wg sync.WaitGroup
	for g := 0; g < workers; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				k := (base*ops + i) % keys
				switch (base + i) % 3 {
				case 0:
					c.Set(k, i, 10*time.Millisecond)
				case 1:
					if _, release, err := c.MustGet(k, 10*time.Millisecond); err == nil {
						release()
					}
				case 2:
					if _, release, ok := c.Get(k); ok {
						release()
					}
				}
			}
		}(g)
	}
	wg.Wait()
}
