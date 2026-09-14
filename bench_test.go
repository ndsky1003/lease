package lease

import (
	"sync/atomic"
	"testing"
	"time"
)

// Set 吞吐：插入长 TTL 任务，测量每秒可插入数量。
func BenchmarkSet(b *testing.B) {
	c := NewWithOptions(Options[int, int]{Tick: time.Millisecond})
	defer c.Stop()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Set(i, i, time.Hour)
	}
}

// Get 热路径：读 + 续期（无锁）的吞吐。
func BenchmarkGet(b *testing.B) {
	c := NewWithOptions(Options[int, int]{Tick: time.Millisecond})
	defer c.Stop()
	for i := 0; i < 100000; i++ {
		c.Set(i, i, time.Hour)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		c.Get(i % 100000)
	}
}

// 到期释放吞吐：插入 n 个相同短 TTL 任务，测量全部释放完成耗时。
func BenchmarkExpire(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var done atomic.Int64
		c := NewWithOptions(Options[int, int]{
			Tick: time.Millisecond,
			OnEvict: func(key, value int) {
				done.Add(1)
			},
		})
		start := time.Now()
		for j := 0; j < 100000; j++ {
			c.Set(j, j, 10*time.Millisecond)
		}
		for done.Load() != 100000 {
			time.Sleep(time.Millisecond)
		}
		b.ReportMetric(float64(100000)/time.Since(start).Seconds(), "expire/s")
		c.Stop()
	}
}
