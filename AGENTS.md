# AGENTS.md

单包 Go 库 `lease`：基于分层时间轮、带空闲超时（租约/滑动过期）的高性能泛型资源容器。零外部依赖（无 `go.sum`）。

## 关键命令

```bash
go test -race ./...        # 并发安全是核心卖点，必须带 -race 跑
go vet ./...
go test -bench=. -benchmem -run='^$' -benchtime=2s
```

没有 Makefile、CI、lint 配置；验证就靠上面三条。

## 架构要点（容易踩坑）

- **有两个时间轮实现，只有一个是活的**：
  - `delay_timing_wheel.go` 的 `DelayTimingWheel` —— `Lease` 实际使用（延迟队列/最小堆按需唤醒，无任务零空转）。
  - `timing_wheel.go` 的 `TimingWheel` —— 简单版（固定 ticker 逐格推进），**是 dead code，仅供学习对照**，没有任何生产引用。
  - 改时间轮前先确认改的是哪个；`NewWithOptions` 里调用的是 `newDelayTimingWheel`。

- **`task` 接口是时间轮与容器的契约**：定义在 `timing_wheel.go:17`（`isCancelled()` + `getExpiration()`）。`lease.go` 里的 `timer[K,V]` 实现它。即使 `TimingWheel` 类型没人用，`task` 接口仍被 `delay_timing_wheel.go` 使用，不能删。

- **续期靠"无锁 + 惰性重调度"，不是显式移动**：`Get` 续期只是对 `timer.expiration` 做一次 `atomic.Store`；任务留在原槽位，时间轮 `flush` 时发现"还没到期"就 `reAdd` 到新槽位。不要改成"从旧槽位摘出再插入新槽位"——会破坏无锁语义和 `expireBatch` 的双重检查逻辑。

- **泛型 + 零装箱是刻意设计**：`Lease[K comparable, V any]`，`timer`/`entry` 持有具体类型而非 `any`，为了消除装箱。引入 `any`/`interface{}` 会显著降低性能（本项目曾实测对比过 sync.Map 方案，因装箱 Set 慢 2 倍而弃用）。

## 测试注意事项

- 测试依赖真实时间（毫秒级 `Tick` + `sleep`/`waitFor` 轮询），是时间敏感的，偶尔会因调度慢而偏慢或抖动；失败时先重跑确认，别急着改逻辑。
- `concurrent_test.go` 是并发压力测试，必须配合 `-race` 才有意义。
- 性能数字（Get ~92ns/0 alloc、Set ~450ns、Expire ~240 万/s）在 `README.md` 和 benchmark 里，改动涉及 `expiration`/`renew`/`expireBatch` 热路径时需重跑 `BenchmarkGet`/`BenchmarkSet` 确认无回退。
