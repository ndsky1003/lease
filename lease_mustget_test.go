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

// gen 返回 err 时，MustGet 返回 err、零值 value、release 为 nil、不写入容器。
func TestMustGetGenError(t *testing.T) {
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		Gen:  func(key string) (string, error) { return "partial", errors.New("boom") },
	})
	defer c.Stop()

	v, release, err := c.MustGet("k", time.Minute)
	if err == nil {
		t.Fatal("gen 返回 err 时 MustGet 应返回 err")
	}
	if release != nil {
		t.Fatal("err 时 release 应为 nil")
	}
	if v != "" {
		t.Fatalf("err 时 value 应为零值，实际 %q", v)
	}
	if c.Len() != 0 {
		t.Fatalf("err 时不应写入容器，Len=%d", c.Len())
	}
}

// MustGetWithGen 使用传入的 gen 而非 Options.Gen 加载。
func TestMustGetWithGenUsesArg(t *testing.T) {
	c := NewWithOptions(Options[string, string]{
		Tick: 5 * time.Millisecond,
		Gen:  func(key string) (string, error) { return "from-options", nil },
	})
	defer c.Stop()

	v, release, err := c.MustGetWithGen("k", time.Minute, func(key string) (string, error) {
		return "from-arg", nil
	})
	if err != nil {
		t.Fatalf("MustGetWithGen 不应返回 err: %v", err)
	}
	release()
	if v != "from-arg" {
		t.Fatalf("未命中应使用传入的 gen，实际 %q", v)
	}
}

// MustGetWithGen 命中时复用已存在 value，不调用传入的 gen。
func TestMustGetWithGenHit(t *testing.T) {
	var genCalls atomic.Int32
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	c.Set("k", "existing", time.Minute)
	v, release, err := c.MustGetWithGen("k", time.Minute, func(key string) (string, error) {
		genCalls.Add(1)
		return "generated", nil
	})
	if err != nil {
		t.Fatalf("MustGetWithGen 不应返回 err: %v", err)
	}
	release()
	if v != "existing" {
		t.Fatalf("命中应返回已存在的值，实际 %q", v)
	}
	if genCalls.Load() != 0 {
		t.Fatalf("命中不应调用 gen，实际调用 %d 次", genCalls.Load())
	}
}

// MustGetWithGen 传入 nil gen：未命中时返回 ErrGenNil 而非 panic。
func TestMustGetWithGenNilGen(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	v, release, err := c.MustGetWithGen("k", time.Minute, nil)
	if !errors.Is(err, ErrGenNil) {
		t.Fatalf("nil gen 应返回 ErrGenNil，实际 err=%v", err)
	}
	if release != nil {
		t.Fatal("nil gen 时 release 应为 nil")
	}
	if v != "" {
		t.Fatalf("nil gen 时 value 应为零值，实际 %q", v)
	}
	if c.Len() != 0 {
		t.Fatalf("nil gen 时不应写入容器，Len=%d", c.Len())
	}
}

// 未配置 Options.Gen 时，MustGet 未命中应返回 ErrGenNil 而非 panic。
func TestMustGetNilGen(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	_, release, err := c.MustGet("k", time.Minute)
	if !errors.Is(err, ErrGenNil) {
		t.Fatalf("未配置 Gen 时 MustGet 应返回 ErrGenNil，实际 err=%v", err)
	}
	if release != nil {
		t.Fatal("未配置 Gen 时 release 应为 nil")
	}
	if c.Len() != 0 {
		t.Fatalf("未配置 Gen 时不应写入容器，Len=%d", c.Len())
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
	if c.Len() != 1 {
		t.Fatalf("持有引用期间条目应保持，Len=%d", c.Len())
	}
	if released.Load() != 0 {
		t.Fatal("持有引用期间不应释放")
	}

	release()
	waitFor(t, time.Second, func() bool { return released.Load() == 1 })
	if c.Len() != 0 {
		t.Fatalf("release 后条目应被移除，Len=%d", c.Len())
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

// countingCloser 实现 io.Closer，用于统计 Close 调用次数。
type countingCloser struct{ closed *atomic.Int32 }

func (c *countingCloser) Close() error { c.closed.Add(1); return nil }

// 并发未命中同一 key：被双重检查丢弃的 gen 结果会释放资源（Close），
// 但不触发 OnEvict（该 key 仍在容器中，未被驱逐）。
func TestMustGetConcurrentMissDiscard(t *testing.T) {
	var genCalls, closed, evicts atomic.Int32
	c := NewWithOptions(Options[int, *countingCloser]{
		Tick: 5 * time.Millisecond,
		Gen: func(key int) (*countingCloser, error) {
			time.Sleep(5 * time.Millisecond) // 放大击穿窗口
			genCalls.Add(1)
			return &countingCloser{closed: &closed}, nil
		},
		OnEvict: func(key int, v *countingCloser) { evicts.Add(1) },
	})
	defer c.Stop()

	const n = 50
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, release, err := c.MustGet(1, time.Hour)
			if err != nil {
				t.Error(err)
				return
			}
			release()
		}()
	}
	wg.Wait()

	if c.Len() != 1 {
		t.Fatalf("双重检查应只写入一个 entry，Len=%d", c.Len())
	}
	if got := closed.Load(); got != genCalls.Load()-1 {
		t.Fatalf("被丢弃的加载结果应被 Close：closed=%d genCalls=%d", got, genCalls.Load())
	}
	if evicts.Load() != 0 {
		t.Fatalf("丢弃不应触发 OnEvict，实际 %d 次", evicts.Load())
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
