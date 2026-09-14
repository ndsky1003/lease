package lease

import (
	"sync"
	"testing"
	"time"
)

// 并发压力测试：多 goroutine 同时 Set / Get（触发续期）/ Delete，
// 配合 -race 检测是否存在数据竞态。
func TestConcurrentAccess(t *testing.T) {
	c := NewWithOptions(Options[int, int]{Tick: 5 * time.Millisecond})
	defer c.Stop()

	const (
		keyRange = 1000
		setters  = 8
		getters  = 8
		removers = 4
		ops      = 5000
	)

	var wg sync.WaitGroup

	// 并发写
	for g := 0; g < setters; g++ {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.Set((base*ops+i)%keyRange, i, 100*time.Millisecond)
			}
		}(g)
	}

	// 并发读（触发续期）
	for g := 0; g < getters; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.Get(i % keyRange)
			}
		}()
	}

	// 并发删
	for g := 0; g < removers; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < ops; i++ {
				c.Delete(i % keyRange)
			}
		}()
	}

	wg.Wait()
}

// 并发续期：多个 goroutine 持续 Get 同一个 key，确保不 panic、无竞态。
func TestConcurrentRenew(t *testing.T) {
	c := NewWithOptions(Options[string, string]{Tick: time.Millisecond})
	defer c.Stop()

	c.Set("hot", "value", 50*time.Millisecond)

	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				if _, ok := c.Get("hot"); !ok {
					t.Error("并发续期期间资源被释放")
					return
				}
			}
		}()
	}
	wg.Wait()
}
