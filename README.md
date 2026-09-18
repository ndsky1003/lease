# lease

一个基于**内置时间轮**的高性能资源容器，提供**空闲超时（滑动过期 / 租约）**语义：

- 资源添加进来时附带一个存活时长（TTL）；
- 每次使用（`Get`）会**记录访问时间**并**增加引用计数**；
- 超过存活时长未被使用、且无调用方持有引用，就从容器中移除；value 的物理释放在**最后一个引用释放后**才发生。

> `lease`（租约）：`Set` 发租约，`Get` 续租，超时不续自动回收。
> 它面向的是"带空闲超时的资源容器"，而非传统意义上的缓存（无容量上限、无 LRU、不保证可重建）。

## 特性

- **租约语义**：`Set` 发租约，`Get` 续租，超时不续自动回收。
- **引用计数防 use-after-free**：`Get` 命中后持有引用，`release` 之前 value 一定不会被释放；持有引用期间即使 ttl 到期，条目也不会被移除（保持可命中），避免数据分裂。
- **轻量读取**：`Get` 只做读锁 + 一次原子写 + 引用计数加一，不移动、不重建定时任务。
- **无全局扫描**：过期检测委托给时间轮按到期时刻批量处理，排期 O(1)，不存在类似 GC 的全表扫描卡顿。
- **访问合并**：可配置 `RenewInterval`，合并高频访问的无谓原子写。
- **自动释放**：释放时回调 `OnEvict`，或 value 实现了 `io.Closer` 则自动 `Close`。
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

	v, release, ok := l.Get("conn:1") // 命中并记录访问（续租 5 分钟）+ 加引用
	if ok {
		defer release() // 用完释放引用；release 之前 v 不会被释放
		use(v)
	}

	l.Delete("conn:1") // 主动删除（无引用时立即释放，有引用时延迟到最后一个 release）
	l.Stop()           // 停止并释放所有资源
}
```

## 核心概念：租约（滑动过期）

```
Set(key, v, 2天)   →  发租约，2 天后到期
Get(key)           →  命中并续租，重新计时 2 天，同时加引用
release()          →  用完释放引用
2 天没 Get、无引用  →  到期，从容器移除，OnEvict 释放
2 天没 Get、有引用  →  保持存活，等引用释放后再判定
再次 Set            →  释放旧值（无引用时立即），重新加载并重新计时
```

这是"空闲超时"（idle timeout）语义：资源在**最后一次使用**之后存活 `ttl` 时长，
而不是在**添加**之后固定 `ttl` 到期。适合 session、连接池、玩家状态等场景。

## 实现方式

本库把"续租"从定时任务中剥离出来：

- `Set` 时把条目按到期时刻落入内置时间轮的到期桶；
- `Get` 只**原子更新最后访问时间**并**增加引用计数**，不移动、不重建任何定时任务；
- 到期桶被单个对齐 ticker 批量处理：若条目还有调用方持有引用（`refs > 1`），视为使用中、推迟判定（重新落桶）；否则若距最后访问已超过 `ttl`（空闲超时），则**从容器移除**；否则按剩余时间**重新落桶**。

value 的物理释放由**引用计数**统一收敛：条目初始引用数为 1（`items` 持有），`Get` 命中 +1，
`release`/删除/覆盖/过期 -1，归零时才真正执行 `OnEvict`/`Close`。此外，**持有引用（`refs > 1`）的
条目不会因到期被移除**，会推迟到引用归零后再做空闲判定，避免「别处 Get 未命中、重新加载」造成的
数据分裂。到期时刻的量化、分桶、逐格 flush 全部由内置时间轮处理。

## API

```go
// 创建
func New[K comparable, V any](onEvict func(K, V)) *Lease[K, V]
func NewWithOptions[K comparable, V any](opts Options[K, V]) *Lease[K, V]

// 操作
func (l *Lease[K, V]) Set(key K, value V, ttl time.Duration) // ttl<=0 永不过期；覆盖已有 key 先释放旧值
func (l *Lease[K, V]) Get(key K) (value V, release func(), ok bool) // 命中并记录访问 + 加引用；release 必须配对调用
func (l *Lease[K, V]) MustGet(key K, ttl time.Duration) (value V, release func(), err error) // 未命中时用 Gen 加载并写入（cache-aside）
func (l *Lease[K, V]) Has(key K) bool                        // 纯探测：不续期、不加引用
func (l *Lease[K, V]) Delete(key K)
func (l *Lease[K, V]) Len() int
func (l *Lease[K, V]) Stop() // 停止时间轮并释放所有资源
```

## 配置项

```go
type Options[K comparable, V any] struct {
	Tick          time.Duration     // 时间轮 tick（到期检查粒度），默认 1 秒
	OnEvict       func(K, V)        // 释放回调
	RenewInterval time.Duration     // 访问合并阈值，<=0 表示每次 Get 都记录
	Gen           func(K) (V, error) // 加载函数；MustGet 未命中时调用
}
```

`RenewInterval` 用于合并高频访问：距上次更新不足该值则跳过续期，避免频繁访问时的无谓原子写。
代价是 `lastAccess` 会落后真实访问时间最多 `RenewInterval`，空闲超时**提前**判定——条目最多提前 `RenewInterval` 释放（例如 TTL 2 天、阈值 10 分钟，实际存活时间在 1 天 23 小时 50 分钟到 2 天之间）。

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

func loadFromDB(id string) (*Player, error) { /* 从数据库加载 */ }

var players = lease.NewWithOptions(lease.Options[string, *Player]{
	Tick:    time.Second,
	OnEvict: func(id string, p *Player) { p.Release() }, // 2 天没操作，自动释放
	Gen:     loadFromDB,                                  // MustGet 未命中时加载
})

func OnLogin(id string, p *Player) {
	players.Set(id, p, 48*time.Hour)
}

// OnAction 把「引用所有权」随 p 一起返回：调用方必须 defer release()。
func OnAction(id string) (*Player, func()) {
	p, release, err := players.MustGet(id, 48*time.Hour) // 命中续租；未命中加载并写入
	if err != nil {
		return nil, nil
	}
	return p, release
}

// 调用方：
p, release := OnAction(id)
defer release() // 用完释放；release 幂等
use(p)
```

`MustGet` 把「命中即返回 / 未命中加载并写入」的 cache-aside 逻辑收敛到库内，省去手写的
「查表 → 加锁 → 双重检查 → 加载 → 写回」样板代码。注意它的 `Gen` 在**锁外**执行——
高并发同时未命中时会各自触发加载（缓存击穿），只是靠双重检查保证不重复写入。若需合并并发
加载，再套一层 `singleflight`。

## 并发模型与性能

| 操作 | 机制 | 性能（Apple M1）|
|------|------|----------------|
| `Get`（读 + 记录访问 + 加引用） | 读锁 + 原子写 + 引用计数 | ~92 ns，2 分配（release 闭包 + 幂等标志），约 1100 万 QPS |
| `Set` | 写锁 + 落入到期桶 | ~345 ns，2 分配，约 290 万 QPS |
| 过期释放 | 时间轮批量回调 + 释放 | 约 390 万/s |

- `Get` 的续租是原子写（`lastAccess` 为原子变量），引用计数为 `atomic.Int64`，无数据竞态（`-race` 通过）。
- `Set` 的 2 次分配来自 `*entry` + 落桶 append，是分桶时间轮的固有代价。
- 释放由引用计数统一收敛，每个 value 恰好释放一次，不重复、不遗漏。

## 注意事项

### 引用计数与 release

`Get` 命中的每个引用都必须**配对调用一次 `release`**（通常 `defer release()`），否则 value 会因引用不归零而泄漏。同一个 `release` 闭包幂等（多次调用只生效一次），但每次 `Get` 返回的 `release` 各自独立，仍需各自调用。

在 `release` 之前，value 不会被释放；持有引用期间，即使 ttl 到期，条目也不会从容器移除（保持可命中）。被 `Delete` 或 `Set` 覆盖时，条目会立即从容器移除，但 value 延迟到最后一个引用释放。`release` 之后不要再使用该 value。

#### 引用所有权的传递

`value` 与 `release` 是绑定的一对。若一个中间函数要把 `value` 返回给上层，必须**把 `release` 一起返回**，绝不能 `defer release()` 后只返回 `value`——那会让调用方拿到已失去保护的 value，use-after-free 又回来了：

```go
// ❌ 错误：defer 在 return 后执行，返回的 v 已无引用保护
func get() *Conn {
	v, release, _ := l.Get("k")
	defer release()
	return v
}

// ✅ 正确：把 release 随 value 一起交给调用方
func get() (*Conn, func()) {
	v, release, ok := l.Get("k")
	if !ok {
		return nil, nil
	}
	return v, release
}
```

如果只是「取出来用一下」，用回调把使用逻辑包进作用域更安全——引用不逃逸，无需手动传递：

```go
func withConn(key string, fn func(*Conn)) bool {
	v, release, ok := l.Get(key)
	if !ok {
		return false
	}
	defer release()
	fn(v)
	return true
}
```

### 访问合并（RenewInterval）

`RenewInterval > 0` 时 `touch` 可能因距上次更新不足阈值而**故意不续租**，导致 `lastAccess` 落后、空闲超时**提前**判定，条目最多提前 `RenewInterval` 释放（见「配置项」）。这不再造成 use-after-free（引用计数兜底），但生命周期边界会比无节流时更宽（不确定范围 = `[ttl - RenewInterval, ttl]`，外加一个 `Tick` 的量化误差）。

## 原理：时间轮到期

过期检测用 **内置单层分桶时间轮**实现，避免"每个条目一个独立定时器"的分配与调度开销，也避免"定时器堆/全表扫描"的全局开销：

- `Set` 时把条目按到期时刻（`now + ttl`，对齐到 `Tick`）落入到期桶（`map[到期时刻][]*entry`），排期 O(1)。
- 单个对齐 ticker 每 `Tick` 批量 flush 到期桶，逐桶 O(1)，不随条目总数退化。
- 到期桶稀疏存储，天然支持任意跨度的 TTL。到期时刻被量化到 `Tick` 边界，误差最大一个 `Tick`。
- 到期回调里做空闲判定：有引用则推迟（重新落桶），无引用且空闲超时则从容器移除，否则按剩余时长重新落桶。

## License

MIT
