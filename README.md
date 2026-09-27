# go-dag-orchestrator

一个本地可运行的 DAG 任务编排引擎。给定一份静态任务编排定义，它按依赖
关系并发调度任务，处理失败、超时、重试、进程中断与重启恢复，并把每一次
状态变更以只追加事件日志（journal）持久化，从而保证：

- 同一份定义重复执行结果一致、可解释；
- 进程被 kill（甚至 `kill -9`）后重新拉起，已完成的任务绝不重跑，未完成
  的任务按原语义继续；
- 任意时刻对外暴露的快照与事件流自洽，无重复计数、无事件丢失。

## 目录结构

```
dag/       图定义与静态校验（缺失依赖、自环、环检测）
engine/    运行时：调度、并发上限、超时/重试、事件日志、崩溃恢复
cmd/dagdemo/  可运行的演示程序（含重试、超时、中断恢复）
```

## 快速开始

```bash
# 正常运行一个含重试与超时的 DAG
go run ./cmd/dagdemo -dir ./data

# 再次运行：检测到 run 已完成，幂等 no-op，不执行任何任务
go run ./cmd/dagdemo -dir ./data

# 中断恢复演示：任务执行中 kill -9，然后重新启动
go run ./cmd/dagdemo -dir ./data -slow &
sleep 0.5 && kill -9 <pid>
go run ./cmd/dagdemo -dir ./data
```

最小代码用法：

```go
g := dag.New([]*dag.Task{
    {ID: "extract", Fn: extract, Timeout: 5 * time.Second},
    {ID: "load", Deps: []string{"extract"},
     Fn: load,
     Retry: dag.RetryPolicy{MaxAttempts: 3, Backoff: time.Second}},
})
if err := g.Validate(); err != nil { /* 环 / 缺失依赖 */ }

store, _ := engine.NewFileStore("data/run.jsonl")
defer store.Close()
eng, _ := engine.New(g, engine.Config{
    MaxConcurrency: 4,
    Store:          store,
    LogSink:        os.Stdout,
})
snap, err := eng.Run(ctx)
```

## 编排语义

### 任务状态（对外稳定字符串）

| 状态      | 含义 |
|-----------|------|
| `pending`   | 尚未开始（初始状态） |
| `running`   | 已记录 `task_started`，正在执行某次尝试（含崩溃后待续跑） |
| `succeeded` | 终态：成功完成 |
| `failed`    | 终态：终止性失败，或重试耗尽 |
| `skipped`   | 终态：因上游失败被阻断，或运行被取消而从未启动 |

### 调度规则

1. **依赖就绪才启动**：一个任务只有在其所有直接依赖均为 `succeeded`
   时才会被调度。
2. **失败不向下游传播执行**：任一依赖为 `failed`/`skipped`，该任务被
   标记为 `skipped`，其函数体永不执行；判定原因可区分：
   - `upstream_failed`：存在真正失败的上游；
   - `canceled`：上游本身也是取消的牺牲品（级联归因到取消）。
3. **并发上限**：同时执行的任务函数数严格不超过
   `Config.MaxConcurrency`（默认 1）。
4. **取消即收敛**：`Run` 的 context 取消后，不再启动新任务；已在执行
   的任务收到取消信号（`dag.Context.Done()` 关闭），引擎等待其退出后
   把所有从未启动的任务记为 `skipped/canceled`，并持久化 `run_finished`。
   取消永远不会让下游继续执行。
5. **确定性日志**：任务按 ID 排序参与调度判定，关键输入（依赖、超时、
   尝试次数）与判定依据（失败原因、是否可重试、跳过原因）都会打印。

### 静态校验：可区分的构图错误

`(*dag.DAG).Validate()` 返回 `*dag.ValidationError`，`Reason` 为稳定值：

- `missing_dependency`：依赖了不存在的任务（空图也归此类）；
- `cycle_detected`：存在依赖环，错误中带 `Cycle []string` 环路径；
  任务依赖自身也按环处理。

## 重试与超时规则

### 错误分类

任务函数返回普通 `error` 视为**可重试失败**（`reason=task_failed`，
`retriable=true`）。使用构造器显式分类：

- `dag.NewRetriableError(msg)`：瞬时失败，可按策略重试；
- `dag.NewTerminalError(msg)`：终止性失败（`reason=terminal_failure`），
  立即失败、不消耗后续重试，下游被 `upstream_failed` 阻断。
- 任务 `panic` 会被引擎捕获并按**终止性失败**处理（不会炸掉整个进程）。

### 超时

- `Task.Timeout > 0` 时，每次尝试独立计时；超时原因是
  `reason=task_timeout`，属于**可重试**失败。
- 超时与运行取消同时发生时，取消优先（不会在已取消的运行里再重试）。
- 超时触发后引擎会等待任务函数响应取消并退出，保证同一任务的两次尝试
  绝不重叠，也不会有任务 goroutine 脱离运行存活。

### 重试

- `Retry.MaxAttempts` 是**总尝试次数**（含首次）；零值按 1 处理。
- 可重试失败且 `attempt < MaxAttempts` 时，先持久化 `task_retry` 事件，
  再按固定 `Retry.Backoff` 退避后执行下一次尝试。
- 退避期间运行被取消：不再重试，任务按 `canceled` 终止。
- 重试耗尽后任务终态为 `failed`，原因为 `retries_exhausted`（消息保留
  最后一次失败原因），与一次命中的 `terminal_failure` 可明确区分。

### 稳定的判定原因一览

`cycle_detected`、`missing_dependency`、`upstream_failed`、`task_timeout`、
`retries_exhausted`、`terminal_failure`、`canceled`、`task_failed`。

## 恢复协议（崩溃与重启）

持久化介质是每条一行 JSON 的**只追加事件日志**（默认
`FileStore`），写入即 `fsync`；同目录下的 `*.lock` 通过 `flock`
保证同一 run 同一时刻只有一个引擎实例（第二个实例打开即报
`ErrJournalLocked`）。

事件类型：`run_started`、`task_started`、`task_retry`、
`task_finished`、`task_skipped`、`run_finished`，每个事件带全局单调
递增的 `seq`（从 1 开始，连续无空洞）。

启动恢复时引擎纯函数式重放日志重建状态，规则如下：

1. **已 `succeeded`/`failed`/`skipped` 的任务绝不重跑**，直接采用终态。
2. **运行已 `run_finished`**：再次 `Run` 是幂等 no-op，返回同一终态
   快照，日志不增长。
3. **崩溃在某次尝试执行中**（最后记录是 `task_started(N)`）：该尝试
   被杀且没有产生失败结论，重启后**按同一个尝试编号 N 重新执行**——
   即中断本身不消耗重试次数。
4. **崩溃在退避等待中**（最后记录是 `task_retry(N)`，下一次
   `task_started` 未发出）：重启后从尝试 N 继续，恰好执行一次。
5. **崩溃导致的下游缺口**：重启后先做确定性的 skip 传播不动点（
   failed/skipped 上游的所有未启动后继按原判定原因落盘），再启动任何
   任务。
6. **重复触发恢复不会错乱**：所有状态变更都由"先追加事件、再归约到
   内存状态"这唯一通道完成，事件序号连续；重放同一日志必然得到同一
   状态。日志序号断裂/损坏会在打开时报错，而不是静默继续。

任务侧的副作用建议保持幂等；虽然成功任务不会被引擎重放，但 attempt
重放（规则 3/4）要求任务能安全重复执行未完成的那一次。

## 可配置项

| 配置 | 位置 | 说明 |
|------|------|------|
| `Task.Timeout` | 每任务 | 单次尝试超时；0 表示不限时（仍响应取消） |
| `Task.Retry.MaxAttempts` | 每任务 | 总尝试次数，>=1；0 视为 1 |
| `Task.Retry.Backoff` | 每任务 | 尝试间固定退避 |
| `Task.Deps` | 每任务 | 直接依赖 ID 列表 |
| `Config.MaxConcurrency` | 引擎 | 全局并发任务上限，<=0 视为 1 |
| `Config.Store` | 引擎 | 持久化后端（必填，自带 `FileStore`） |
| `Config.LogSink` | 引擎 | 日志输出，nil 丢弃 |
| `Config.RunID` | 引擎 | 指定 run id；与既有日志不一致时报 `ErrRunIDMismatch` |
| `Config.Clock` | 引擎 | 时间/退避时钟，主要用于测试 |

## 状态与事件查询

- `eng.Snapshot()` 返回某一时刻一致的深拷贝 `RunSnapshot`：每个任务的
  `Status`、`Attempts`、`Reason`、`Message`、起止时间，以及日志位点
  `Seq`、运行是否结束、是否成功。可在 `Run` 执行期间并发调用。
- `eng.Events()` 返回事件日志的防御性拷贝，`Seq` 严格连续，可用于事件
  重放/审计。并发查询不会出现重复计数或丢失事件（有竞态检测测试覆盖）。

## 测试

```bash
# 普通
go test ./...

# 竞态检测（要求 cgo）
CGO_ENABLED=1 go test -race -count=1 ./...
```

覆盖场景：环依赖与缺失依赖的可区分判定、上游失败阻断下游、并发上限不
被突破、超时可重试、重试耗尽与终止性失败的区分、运行取消的级联归因、
并发快照一致性、以及**真实子进程 `os.Exit`/`kill -9` 后的跨进程崩溃恢复**
（已完成任务不重跑、中断尝试不消耗重试、重复恢复幂等、日志序号连续）。
