# AGENTS.md

单包 Go 库 `lease`（module `github.com/ndsky1003/lease`，Go 1.22）：带空闲超时（租约/滑动过期）的泛型资源容器 `Lease[K comparable, V any]`。

**零外部依赖**：`go.mod` 无 require，`go.sum` 为空。全部实现在 `lease.go`，用「按到期时刻分桶 + 单个对齐 ticker」驱动过期，不依赖任何时间轮库。

## 关键命令

```bash
go test -race ./...        # 并发安全是核心卖点，必须带 -race
go vet ./...
go test -bench=. -benchmem -run='^$' -benchtime=2s
```

没有 Makefile、CI、lint 配置；验证就靠这三条。

## 架构要点（容易踩坑）

- 全部实现都在 `lease.go`。`entry[K,V]` 是缓存条目，持有 `key/value/ttl`、`renewInterval`、`lastAccess`/`lastRenew` 两个 `atomic.Int64`。**没有** `sync.Mutex` 和定时任务字段（早前时间轮版的 `mu`/`timer` 已随重构移除）。

- **过期靠"分桶 + 单 ticker"，不是每条目一个定时任务**：`Set` 时把条目按 `align(now+ttl)` 落到 `buckets map[int64][]*entry`（key 是对齐到 Tick 的到期时刻）。`run()` 用一个对齐到 Tick 边界的 `time.Timer` 逐格推进，每格 flush 当前到期桶。不要在 `Set` 里创建任何 per-entry 定时器。

- **续期是"无锁"，不移动条目**：`Get` 只做一次 `RLock` 查表 + `e.touch()`（`atomic.Store` 到 `lastAccess`），不碰 bucket。到期桶 flush 时 `expire` 读 `lastAccess`，空闲未超时就 `rebucket` 剩余时长，否则释放。不要改成"到期即重建/移动"。

- **`align` 是向上取整（ceil），不是 floor**：`(x + tickNs - 1) / tickNs * tickNs`。用 floor 会让到期时刻落在已过去的桶，被推进中的 ticker 永久漏掉（条目永不释放）。改这里要格外小心。

- **`run()` 的 for 循环要逐个 flush 中间边界**：`for d := lastDue + tickNs; d <= cur; d += tickNs`。这是防"调度延迟/GC 停顿 > Tick 导致跳过中间到期桶"的兜底。删掉它会在卡顿时漏释放。

- **`expire`/`rebucket` 的双重检查依赖指针同一性**：`l.items[e.key] != e` 判断条目是否被并发 `Set`/`Delete` 替换，是防重复释放和僵尸条目的关键。保持 `*entry` 指针比较，别改成 value 比较。

- **`ttl <= 0` 表示永不过期**：`Set` 时不落桶。`Delete` 直接删 `items` 并释放，桶里的旧条目留到 flush 时被指针判断丢弃。

- **`Set` 覆盖已有 key 会释放旧值**：替换发生在锁内，旧值 `release` 在锁外调用（避免持锁调用户回调）。释放旧值 + `items[key] != e` 指针判断配合，保证并发 `Set`/`expire`/`Delete` 下每个 value 恰好释放一次，不会重复或遗漏。别把旧值的释放移进锁内，也别删掉指针判断。

- **泛型 + 零装箱是刻意设计（热路径）**：`Get`/`Set` 热路径全程具体类型，无 `any` 装箱。唯一的装箱在 `release` 的 `any(value).(io.Closer)`，属释放路径、非热路径，可接受。别在 `Get`/`Set`/`touch`/`expire` 引入 `any`。

- **`Options.WheelSize` 已弃用**：分桶实现不需要它，字段保留仅为 API 兼容，实现里不读取。

## 测试注意事项

- 测试依赖真实时间（毫秒级 `Tick` + `sleep`/`waitFor` 轮询），时间敏感，偶发因调度抖动变慢或失败；失败先重跑，别急着改逻辑。
- `concurrent_test.go` 是并发压力测试，必须配合 `-race` 才有意义。
- 性能数字在 `README.md` 和 `bench_test.go`；改动 `touch`/`expire`/`rebucket`/`Set`/`run` 等热路径后重跑 `BenchmarkGet`/`BenchmarkSet` 确认无回退。
