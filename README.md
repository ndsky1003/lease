# lease

一个基于**时间轮**的高性能资源容器，提供**空闲超时（滑动过期 / 租约）**语义：

- 资源添加进来时附带一个存活时长（TTL）；
- 每次使用（`Get`）会**自动续期**；
- 超过存活时长未被使用，就**自动释放**该资源。

> `lease`（租约）：`Set` 发租约，`Get` 续租，超时不续自动回收。
> 它面向的是"带空闲超时的资源容器"，而非传统意义上的缓存（无容量上限、无 LRU、不保证可重建）。

## 特性

- **租约语义**：`Set` 发租约，`Get` 续租，超时不续自动回收。
- **无全局扫描**：过期检测由分层时间轮驱动，每格只处理一个槽位，O(1)，不存在类似 GC 的全表扫描卡顿。
- **无锁续期**：`Get` 的续期是原子写 + 惰性重调度，零锁、零分配。
- **续期节流**：可配置 `RenewInterval`，合并高频访问的无谓续期。
- **自动释放**：过期时回调 `OnEvict`，或 value 实现了 `io.Closer` 则自动 `Close`。
- **线程安全**：并发读写经 `-race` 验证。
- **泛型**：`Lease[K comparable, V any]`，零装箱。

## 快速开始

```go
import "lease"

func main() {
	l := lease.New(func(key string, value *Conn) {
		// 过期自动释放：关连接、回收内存等
		value.Close()
	})

	l.Set("conn:1", conn, 5*time.Minute) // 存活 5 分钟

	v, ok := l.Get("conn:1") // 命中并自动续期 5 分钟

	l.Delete("conn:1") // 主动释放

	l.Stop() // 停止并释放所有资源
}
```

## 核心概念：租约（滑动过期）

```
Set(key, v, 2天)   →  发租约，2 天后到期
Get(key)           →  命中并续期，重新计时 2 天
2 天没 Get          →  到期，OnEvict 自动释放
再次 Set            →  重新加载并重新计时
```

这是"空闲超时"（idle timeout）语义：资源在**最后一次使用**之后存活 `ttl` 时长，
而不是在**添加**之后固定 `ttl` 到期。适合 session、连接池、玩家状态等场景。

## API

```go
// 创建
func New[K comparable, V any](onEvict func(K, V)) *Lease[K, V]
func NewWithOptions[K comparable, V any](opts Options[K, V]) *Lease[K, V]

// 操作
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) // ttl<=0 表示永不过期
func (l *Lease[K, V]) Get(key K) (value V, ok bool)          // 命中并续期
func (l *Lease[K, V]) Has(key K) bool
func (l *Lease[K, V]) Delete(key K)
func (l *Lease[K, V]) Len() int
func (l *Lease[K, V]) Stop() // 停止时间轮并释放所有资源
```

## 配置项

```go
type Options[K comparable, V any] struct {
	Tick          time.Duration // 时间轮每格时长，默认 1 秒
	WheelSize     int64         // 每层槽位数量，默认 64
	OnEvict       func(K, V)    // 释放回调
	RenewInterval time.Duration // 续期节流阈值，<=0 表示每次 Get 都续期
}
```

`RenewInterval` 用于合并高频续期：距上次续期不足该值则跳过，避免频繁访问时的无谓原子写。
代价是空闲超时最多产生该值的偏差（例如 TTL 2 天、阈值 10 分钟，实际释放时间在 2 天到 2 天 10 分钟之间）。

## 示例：玩家会话管理

```go
type Player struct {
	conn net.Conn
	// ...
}

func (p *Player) Release() {
	p.conn.Close()
	// 落库、回收内存等
}

var players = lease.New(func(id string, p *Player) {
	p.Release() // 2 天没操作，自动释放
})

func OnLogin(id string, p *Player) {
	players.Set(id, p, 48*time.Hour)
}

func OnAction(id string) *Player {
	p, ok := players.Get(id) // 命中则自动续期 2 天
	if !ok {
		p = loadFromDB(id) // 已过期释放，重新加载
		players.Set(id, p, 48*time.Hour)
	}
	return p
}
```

## 并发模型与性能

| 操作 | 机制 | 性能（Apple M1）|
|------|------|----------------|
| `Get`（读 + 续期） | 读锁 + 原子续期 | ~92 ns，0 分配，约 1100 万 QPS |
| `Set` | 写锁 + 投递时间轮 | ~450 ns，约 220 万 QPS |
| 过期释放 | 批量释放（整槽一次加锁） | 约 240 万/s |

- 续期是**无锁**的：`timer.expiration` 为原子变量，`Get` 里一次 `Store` 即完成。
- 过期判定与释放均为原子操作 + 指针比较，无数据竞态（`-race` 通过）。

## 原理：时间轮

过期检测用**分层时间轮**实现，避免"定时器堆/全表扫描"的全局开销：

- 每层由若干槽位组成，一圈覆盖 `tick × wheelSize`，超出范围的定时任务投递到上层（上层 tick 等于当前层一圈），从而指数级扩大时间跨度。
- 每格只处理当前一个槽位（O(1)），不随条目总数退化。
- 本库内置两种实现，`Lease` 默认使用延迟队列版（无任务时零 CPU 空转）：

| 文件 | 实现 | 说明 |
|------|------|------|
| `delay_timing_wheel.go` | `DelayTimingWheel` | 延迟队列（最小堆）按需唤醒，无任务零空转，**`Lease` 正在使用** |
| `timing_wheel.go` | `TimingWheel` | 固定 ticker 逐格推进，更简单，供学习对照 |

## License

MIT
