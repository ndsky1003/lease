# AGENTS.md

单包 Go 库 `lease`（module `github.com/ndsky1003/lease`，Go 1.22）：带空闲超时（租约/滑动过期）的泛型资源容器 `Lease[K comparable, V any]`。

**零外部依赖**：`go.mod` 无 require，`go.sum` 为空。全部实现在 `lease.go`，过期检测用内置的单层分桶时间轮（`buckets` + `run` + `flush` + `align`），不依赖任何外部库。

## 关键命令

```bash
go test -race ./...        # 并发安全是核心卖点，必须带 -race
go vet ./...
go test -bench=. -benchmem -run='^$' -benchtime=2s
```

没有 Makefile、CI、lint 配置；验证就靠这三条。

## 架构要点（容易踩坑）

- 全部实现都在 `lease.go`。`entry[K,V]` 是缓存条目，持有 `key/value/ttl`、`renewInterval`、`lastAccess`/`lastRenew` 两个 `atomic.Int64`、`refs atomic.Int64`（引用计数）、`cancelled atomic.Bool`（是否被覆盖/删除）。`Lease` 内联了时间轮的字段（`tick`/`startNs`/`lastDue`/`buckets`/`stopC`/`wg`）与方法（`put`/`run`/`flush`/`align`），桶里直接存 `*entry[K,V]`，**没有** `*Task` 或到期闭包（已随内联移除）。

- **过期靠内置时间轮（单层分桶 + 对齐 ticker），不是外部库**：`Set` 时 `l.put(e, l.align(now+ttl))` 把条目落入到期桶；`run()` 用对齐到 Tick 边界的 `time.Timer` 逐格推进，每格 `flush` 到期桶。不要重新造 ticker 或 bucket，也不要退回外部时间轮依赖。

- **桶里存 `*entry`，取消用 `cancelled` 标志**：`Set` 覆盖/`Delete` 时 `e.cancelled.Store(true)`，`flush` 里 `if e.cancelled.Load() { continue }` 丢弃。别改回"每条目一个 Task + Cancel"。

- **续期是"无锁"，不移动/不重建定时任务**：`Get` 只做一次 `RLock` 查表 + `e.touch()`（`atomic.Store` 到 `lastAccess`）+ `e.refs.Add(1)`，不碰时间轮。到期回调 `expire` 先查 `refs`：`refs > 1`（有调用方持有）就 `rebucket` 推迟、不做空闲判定；`refs == 1` 才读 `lastAccess` 判定空闲——未超时 `rebucket` 剩余时长，超时则从 `items` 移除。不要改成"每次 Get 都重排"。

- **物理释放由引用计数收敛，不是指针判断**：`refs` 初始为 1（`items` 持有），`Get` 命中 +1，`release`/`Delete`/覆盖/`expire` 各 -1，归零时才真正 `release`。`entry.unref()` 用 CAS 循环减一、只在从 1 降到 0 时返回 true（触发物理释放），`refs<=0` 后再调用返回 false（幂等）。「释放恰好一次」靠这个 CAS 协议保证，别改回"指针判断后直接释放"。

- **有引用就不逻辑移除**：`expire` 里 `refs > 1`（除 `items` 外还有调用方持有）时不做空闲判定，直接 `rebucket` 推迟，保持条目可命中；只有 `refs == 1`（仅容器持有）才判定空闲超时移除。这是"持有 = 使用中"语义，避免别处 `Get` 未命中重新加载造成数据分裂。锁外先查一次 `refs` 做快速路径，锁内还要再查一次（锁外读后到加锁前可能有 `Get` 进来），别只靠锁外那一次。

- **`release` 闭包幂等**：`Get` 里用 `int32` + `atomic.CompareAndSwapInt32(&released, 0, 1)` 保证同一个 `release` 闭包多次调用只释放一次引用，防止"误调两次 release 把容器引用也释放掉"导致 use-after-free 回归。这是 `Get` 比纯 `func(){ l.releaseRef(e) }` 多 1 alloc 的原因，别为省分配去掉它。

- **`expire`/`rebucket` 的双重检查依赖指针同一性**：`l.items[e.key] != e` 判断条目是否被并发 `Set`/`Delete` 替换，是决定"是否从 `items` 逻辑移除"的关键，防止重复移除和僵尸条目。保持 `*entry` 指针比较，别改成 value 比较。

- **`refs` 增减的时序不变量**：`Get` 的 `refs.Add(1)` 在 `RLock` 内、且只对 `items[key]` 当前指向的 entry 执行；`releaseRef`（触发 `unref`）一定发生在该 entry 已被 `delete`/替换出 `items` **之后**。二者经 `RWMutex` 互斥 + "先移除再释放"的顺序保证，绝不会出现"释放后又 Add(1)"的 use-after-free。别把 `releaseRef` 移到删除 `items` 之前。

- **锁顺序固定为 `l.mu -> l.wheelMu`**：`put` 内部加 `wheelMu`（`Set`/`rebucket` 持 `l.mu` 调 `put`）；时间轮回调（`flush` 在 `wheelMu` 锁外执行 `expire`）里才加 `l.mu`，两者无反向持有，不构成死锁。不要在 `l.mu` 之外反向嵌套。

- **`ttl <= 0` 表示永不过期**：`Set` 时不落桶。`Delete` 直接删 `items`、`cancelled.Store(true)` 并 `releaseRef`；残留的到期回调触发时被 `cancelled`/指针判断丢弃。

- **`Set` 覆盖已有 key 会移除旧值**：替换发生在锁内（含 `cancelled.Store(true)`），旧值 `releaseRef` 在锁外调用（释放引用，避免持锁调用户回调）。若旧值仍被调用方持有，会延迟到最后一个 `release`。

- **`Has` 是纯探测**：直接 `RLock` 查表，不 `touch`、不 `refs.Add`。别让 `Has` 走 `Get`（否则会顺带续期 + 加引用，语义被污染）。

- **泛型 + 零装箱是刻意设计（热路径）**：`Get`/`Set` 热路径全程具体类型，无 `any` 装箱。唯一的装箱在 `release` 的 `any(value).(io.Closer)`，属释放路径、非热路径，可接受。别在 `Get`/`Set`/`touch`/`expire`/`unref` 引入 `any`。

- **`Options.Gen func(K) (V, error)` 是 `MustGet` 的加载函数**：`NewWithOptions` 里赋值到 `l.gen`。`Options` 已无 `WheelSize`（字段已移除）。

- **`MustGet` 是 cache-aside，`gen` 锁外执行**：快速路径 `RLock` 命中即返回；未命中时 `gen` 在锁外调用（不持锁 IO、不死锁），再用 `Lock` 双重检查防重复写入；双重检查命中时丢弃本次 `gen` 结果并 `l.release` 释放。`gen` 锁外意味着并发未命中会各自触发 `gen`（击穿），要合并加载需 singleflight。`release` 闭包统一由 `newRelease` 构造（幂等），`Get`/`MustGet` 共用。

- **分配开销在 `Set`、每次 `rebucket` 与每次 `Get`**：`Set` 分配 `*entry` + 落桶 append（约 2 alloc/op）；`Get` 返回的 `release` 闭包 + 幂等标志（约 2 alloc/op）。这是分桶 + 引用计数的固有代价，别误以为是无谓分配去"优化"掉。

## 测试注意事项

- 测试依赖真实时间（毫秒级 `Tick` + `sleep`/`waitFor` 轮询），时间敏感，偶发因调度抖动变慢或失败；失败先重跑，别急着改逻辑。
- `concurrent_test.go` 是并发压力测试，必须配合 `-race` 才有意义。
- `TestRefCountDelaysRelease`/`TestRefCountOverwriteDelaysRelease`/`TestReleaseIdempotent`/`TestConcurrentGetRefCount` 验证引用计数语义（延迟释放、幂等），改 `refs`/`unref`/`releaseRef`/`Get` 后必须跑。
- 性能数字在 `README.md` 和 `bench_test.go`；改动 `touch`/`expire`/`rebucket`/`Set`/`Get`/`unref`/`put`/`flush` 等热路径后重跑 `BenchmarkGet`/`BenchmarkSet` 确认无回退。
