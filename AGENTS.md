# AGENTS.md

单包 Go 库 `lease`（module `github.com/ndsky1003/lease`，Go 1.22）：带空闲超时（租约/滑动过期）的泛型资源容器 `Lease[K comparable, V any]`。

唯一外部依赖：`github.com/RussellLuo/timingwheel`（Kafka 分层时间轮的 Go 移植），`lease.go` 直接 import 使用，`go.sum` 存在。**本仓库没有自研时间轮实现**。

## 关键命令

```bash
go test -race ./...        # 并发安全是核心卖点，必须带 -race
go vet ./...
go test -bench=. -benchmem -run='^$' -benchtime=2s
```

没有 Makefile、CI、lint 配置；验证就靠这三条。

## 架构要点（容易踩坑）

- 全部实现都在 `lease.go`（约 230 行），没有拆分文件。`entry[K,V]` 是缓存条目，持有 `key/value/ttl`、`lastAccess`/`lastRenew` 两个 `atomic.Int64`、`mu sync.Mutex` 和 `*timingwheel.Timer`。

- **续期是"无锁 + 惰性重调度"，不是移动定时任务**：`Get` 只做一次 `RLock` 查表 + `e.touch()`（`atomic.Store` 到 `lastAccess`），不碰定时任务。任务到期时 `check` 读 `lastAccess`，空闲未超时就 `reschedule` 剩余时长，否则释放。不要改成"到期即 Stop+AfterFunc 重建"或"显式移动任务"，会破坏无锁语义。

- **`check`/`reschedule` 的双重检查依赖指针同一性**：`l.items[e.key] != e` 判断条目是否被并发 `Set`/`Delete` 替换，是防重复释放和僵尸任务的关键。改这两处时保持 `*entry` 指针比较，别改成 value 比较。

- **`ttl <= 0` 表示永不过期**：`Set` 时不会 `schedule`，`timer` 保持 nil。`entry.stop()` 幂等（锁内判 nil），`Set` 覆盖旧 key 时先 `old.stop()`。

- **泛型 + 零装箱是刻意设计（热路径）**：`Get`/`Set` 热路径全程具体类型，无 `any` 装箱。唯一的装箱在 `release` 的 `any(value).(io.Closer)`，属释放路径、非热路径，可接受。别在 `Get`/`Set`/`touch`/`check` 引入 `any`。

## 测试注意事项

- 测试依赖真实时间（毫秒级 `Tick` + `sleep`/`waitFor` 轮询），时间敏感，偶发因调度抖动变慢或失败；失败先重跑，别急着改逻辑。
- `concurrent_test.go` 是并发压力测试，必须配合 `-race` 才有意义。
- 性能数字在 `README.md` 和 `bench_test.go`；改动 `touch`/`check`/`reschedule`/`Set` 等热路径后重跑 `BenchmarkGet`/`BenchmarkSet` 确认无回退。
