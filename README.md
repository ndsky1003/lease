# lease

一个基于**分桶**的高性能资源容器，提供**空闲超时（滑动过期 / 租约）**语义：

- 资源添加进来时附带一个存活时长（TTL）；
- 每次使用（`Get`）会**记录访问时间**；
- 超过存活时长未被使用，就**自动释放**该资源。

> `lease`（租约）：`Set` 发租约，`Get` 续租，超时不续自动回收。
> 它面向的是"带空闲超时的资源容器"，而非传统意义上的缓存（无容量上限、无 LRU、不保证可重建）。

## 特性

- **租约语义**：`Set` 发租约，`Get` 续租，超时不续自动回收。
- **无锁读取**：`Get` 只对最后访问时间做一次原子写，零锁、零分配，不移动条目。
- **无全局扫描**：过期检测由单个 ticker 按到期桶批量处理，每个桶 O(1)，不存在类似 GC 的全表扫描卡顿。
- **访问合并**：可配置 `RenewInterval`，合并高频访问的无谓原子写。
- **自动释放**：过期时回调 `OnEvict`，或 value 实现了 `io.Closer` 则自动 `Close`。
- **线程安全**：并发读写经 `-race` 验证。
- **泛型**：`Lease[K comparable, V any]`，零装箱。

## 快速开始

```go
import "github.com/ndsky1003/lease"

func main() {
	l := lease.New(func(key string, value *Conn) {
		// 过期自动释放：关连接、回收内存等
		value.Close()
	})

	l.Set("conn:1", conn, 5*time.Minute) // 空闲存活 5 分钟

	v, ok := l.Get("conn:1") // 命中并记录访问（续租 5 分钟）

	l.Delete("conn:1") // 主动释放

	l.Stop() // 停止并释放所有资源
}
```

## 核心概念：租约（滑动过期）

```
Set(key, v, 2天)   →  发租约，2 天后到期
Get(key)           →  命中并续租，重新计时 2 天
2 天没 Get          →  到期，OnEvict 自动释放
再次 Set            →  释放旧值，重新加载并重新计时
```

这是"空闲超时"（idle timeout）语义：资源在**最后一次使用**之后存活 `ttl` 时长，
而不是在**添加**之后固定 `ttl` 到期。适合 session、连接池、玩家状态等场景。

## 实现方式

本库把"续租"从定时任务中剥离出来：

- `Set` 时把条目按**到期时刻**（`now + ttl`，对齐到 `Tick`）落入对应的到期桶；
- `Get` 只**原子更新最后访问时间**，不移动、不重建任何定时任务；
- 单个 ticker 每 `Tick` 批量处理到期的桶：若距最后访问已超过 `ttl`（空闲超时），则**释放资源**；否则按剩余时间**重新分桶**。

这样 `Get` 完全无锁（一次 `atomic.Store`），避免了传统"续期即 Stop+AfterFunc"的高开销，也省去了每个条目一个定时任务的分配成本。

## API

```go
// 创建
func New[K comparable, V any](onEvict func(K, V)) *Lease[K, V]
func NewWithOptions[K comparable, V any](opts Options[K, V]) *Lease[K, V]

// 操作
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) // ttl<=0 永不过期；覆盖已有 key 先释放旧值
func (l *Lease[K, V]) Get(key K) (value V, ok bool)          // 命中并记录访问
func (l *Lease[K, V]) Has(key K) bool
func (l *Lease[K, V]) Delete(key K)
func (l *Lease[K, V]) Len() int
func (l *Lease[K, V]) Stop() // 停止到期处理循环并释放所有资源
```

## 配置项

```go
type Options[K comparable, V any] struct {
	Tick          time.Duration // 到期桶处理周期，默认 1 秒
	WheelSize     int64         // 已弃用：分桶实现下不再需要，保留以兼容旧代码
	OnEvict       func(K, V)    // 释放回调
	RenewInterval time.Duration // 访问合并阈值，<=0 表示每次 Get 都记录
}
```

`RenewInterval` 用于合并高频访问：距上次更新不足该值则跳过，避免频繁访问时的无谓原子写。
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
	p, ok := players.Get(id) // 命中则自动续租 2 天
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
| `Get`（读 + 记录访问） | 读锁 + 原子写 | ~50 ns，0 分配，约 2000 万 QPS |
| `Set` | 写锁 + 落入到期桶 | ~260 ns |
| 过期释放 | ticker 批量检查 + 释放 | 约 400 万/s |

- `Get` 的续租是**无锁**的：`lastAccess` 为原子变量，`Get` 里一次 `Store` 即完成。
- 过期判定与释放均为原子操作 + 指针比较，无数据竞态（`-race` 通过）。

## 注意事项：TOCTOU

`Get` 返回的 value 可能在返回之后立刻被并发的 `expire` 释放。这是此类租约容器的通病
（检查与使用之间存在时间差），`Get` 本身只保证"此刻还在、已续租"，无法保证调用者使用该 value 期间它始终存活。
调用者需自行判断资源有效性（例如自带的 `Close`/探活机制），或在业务层加引用计数等方式兜底。

具体到本实现，存在两个已知的临界窗口：

- **`Get` 与 `touch` 之间的缝隙**：`Get` 在 `RLock` 内查到条目、`RUnlock` 之后才执行 `touch`
  （原子更新 `lastAccess`）。并发的 `expire` 可能恰好在这段缝隙内完成释放，导致 `Get` 返回一个
  已释放的 value（use-after-free）。`expire` 虽持写锁做了双重检查，但第二次读 `lastAccess` 仍可能
  读到 `touch` 之前的旧值。
- **`RenewInterval > 0` 时 `touch` 可能跳过续租**：访问合并会让 `touch` 因距上次更新不足阈值而
  **故意不更新** `lastAccess`，此时即使 `Get` 命中也不会续租，value 仍可能被立即释放。这是合并语义
  的固有偏差，与上方「配置项」一节对 `RenewInterval` 的说明一致。

上述窗口不通过把 `touch` 移进锁来修复——那会破坏「无锁续租」的热路径、拉长读锁持有时间。
它们属于本库 API 的固有语义边界，需由调用者自行兜底。

## 原理：分桶到期

过期检测用**按到期时刻分桶**实现，避免"每个条目一个定时器"的分配开销，也避免"定时器堆/全表扫描"的全局开销：

- 条目按 `now + ttl` 对齐到 `Tick` 后落入对应的到期桶（`map[到期时刻][]*entry`），`Set` 是 O(1)。
- 单个 ticker 对齐到 `Tick` 边界逐格推进，每个周期只处理当前到期的桶，O(1)，不随条目总数退化。
- 到期桶稀疏存储，天然支持任意跨度的 TTL，无需时间轮的分层/溢出结构。

## License

MIT
