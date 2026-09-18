package lease

import (
	"testing"
	"time"
)

// Tick <= 0 应退化为 1 秒。
func TestTickDefault(t *testing.T) {
	c := NewWithOptions(Options[int, int]{Tick: 0})
	defer c.Stop()
	if c.tick != time.Second {
		t.Fatalf("Tick<=0 应退化为 1s，实际 %v", c.tick)
	}
}

// align 必须向上取整到 Tick 边界（ceil），否则到期时刻会落到已过去的桶被漏掉。
func TestAlignCeil(t *testing.T) {
	l := &Lease[int, int]{tick: 5 * time.Millisecond}
	tickNs := int64(l.tick)

	cases := []struct {
		x, want int64
	}{
		{1, tickNs},                // 1ns → 向上到 5ms
		{tickNs, tickNs},           // 恰好在边界 → 不变
		{tickNs + 1, 2 * tickNs},   // 边界后 1ns → 下一个边界
		{2*tickNs - 1, 2 * tickNs}, // 边界前 1ns → 下一个边界
		{2 * tickNs, 2 * tickNs},   // 恰好在边界 → 不变
	}
	for _, c := range cases {
		if got := l.align(c.x); got != c.want {
			t.Errorf("align(%d) = %d, want %d", c.x, got, c.want)
		}
	}
}

// RenewInterval < ttl 时，合并续期仍生效：高频 Get 能让条目存活超过原 ttl。
// 与 TestRenewCoalescing（RenewInterval >= ttl 时续期失效）互为对照。
func TestRenewIntervalLTRenews(t *testing.T) {
	c := NewWithOptions(Options[string, string]{
		Tick:          5 * time.Millisecond,
		RenewInterval: 5 * time.Millisecond, // < ttl，合并但会续期
	})
	defer c.Stop()

	c.Set("k", "v", 30*time.Millisecond)

	// 高频 Get（立即 release），合并窗口内至少续期一次，条目存活远超原 ttl
	deadline := time.Now().Add(100 * time.Millisecond)
	for time.Now().Before(deadline) {
		_, release, ok := c.Get("k")
		if !ok {
			t.Fatal("RenewInterval < ttl 时条目应持续存活")
		}
		release()
		time.Sleep(2 * time.Millisecond)
	}

	// 停止访问后，条目最终按剩余空闲时长过期
	waitFor(t, time.Second, func() bool { return c.Len() == 0 })
}
