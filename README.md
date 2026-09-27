# go-dag-orchestrator

一个本地可运行的 DAG 任务编排引擎：按依赖关系调度任务、处理失败与进程中断，
保证同一份编排定义在重复执行和重启后都能得到一致、可解释的结果。

## 快速开始

```bash
# 运行测试（含竞态检测）
CGO_ENABLED=1 go test ./... -race

# 运行演示 CLI
go run ./cmd/dagrun -dir ./rundata -run-id demo -concurrency 2

# 用相同 run-id 再次执行：已完成任务不重跑（恢复语义）
go run ./cmd/dagrun -dir ./rundata -run-id demo
```

## 编排语义

- 编排定义（`dag.Spec`）由若干任务（`dag.Task`）组成，每个任务声明 `Deps`（上游任务 ID）。
- 任务必须等**所有上游成功**后才会被调度；独立分支互不影响。
- 引擎在执行前校验定义，以下问题会返回可区分的 `*dag.ValidationError`：
  - **环依赖**（含自环）：`Detail` 含 `cycle`，`TaskIDs` 给出环上节点；
  - **依赖缺失**：`Detail` 含 `missing`，`TaskIDs` 给出引用方与缺失方；
  - 任务 ID 为空或重复。
  校验失败时运行结果为 `RunInvalid`。
- 上游失败 / 被跳过 / 被取消时，下游任务被标记为 `skipped`（`FailureUpstream`），
  并沿依赖链向下传播——**失败或取消不会让下游继续执行**。
- 整次运行的判定结果（`Snapshot.Status`）：
  - `succeeded`：全部任务成功；
  - `failed`：存在终止性失败或被跳过的任务；
  - `canceled`：运行被取消（如收到中断信号）；
  - `invalid`：编排定义非法。

## 任务状态与失败分类

任务状态机：`pending → running → (succeeded | failed | skipped | canceled)`，
重试退避期间保持 `running`（任务仍被其执行 goroutine 持有，不会被重复派发）。

失败性质（`FailureKind`）可区分：

| 类别         | 含义                                   | 是否重试 |
|--------------|----------------------------------------|----------|
| `retryable`  | 任务返回 `dag.Retryable(err)` 包装的错误 | 是       |
| `timeout`    | 单次尝试超过 `Task.Timeout`             | 是       |
| `fatal`      | 任务返回其他 error                      | 否       |
| `upstream`   | 上游未成功导致被跳过                     | —        |
| `canceled`   | 运行被取消                              | —        |
| `validation` | 编排定义非法                            | —        |

## 重试与超时规则

- `Task.Timeout`：单次尝试的超时时间，`0` 表示不限制。超时判定由引擎做出，
  即使任务执行体不响应取消，引擎也会判定该次尝试超时失败。
- `Task.Retries`：允许的**失败重试次数**（不含首次执行）。重试预算按失败次数计算，
  被中断 / 取消的尝试**不消耗**预算。
- `Task.Backoff`：重试前的退避等待时间，`0` 表示立即重试；退避期间运行被取消则任务取消。
- 终止性失败（`fatal`）无论剩余多少重试预算都立即失败。
- 快照中的 `Attempts` 是已开始的尝试总数（含被中断的尝试），历史计数在恢复后保留。

## 恢复协议

引擎把每次状态迁移作为不可变事件（`Event`）追加到 `Store`
（`FileStore` 为每个 run-id 维护一个 JSONL 文件，`MemStore` 不持久化）。
事件携带单调递增的 `Seq`，追加幂等（重复 `Seq` 被忽略），重放时去重排序，
因此**事件不会重复计数也不会丢失**。

用相同 `runID` 调用 `Run` 时的恢复语义：

1. 重放事件流重建状态；
2. 运行已是 `succeeded` / `failed`：直接返回重放结果，**任何任务都不重跑**（幂等）；
3. 运行被中断（含 `canceled`）：已成功的任务不重跑；处于 `running` / `canceled`
   等未完成状态的任务重置为 `pending`，按原语义继续调度，
   重试预算从事件流中恢复（崩溃前的失败次数不丢失）；
4. 同一 `runID` 不允许并发执行两次（返回 `ErrRunInProgress`），
   重复触发恢复不会造成状态错乱。

进程中断后重新拉起的标准做法：用相同的存储目录和 run-id 再次执行
（见 `cmd/dagrun`，它把 SIGINT/SIGTERM 转换为运行取消）。

## 状态查询与一致性

- `Engine.Run` 返回最终一致快照；`Engine.Snapshot(runID)` 可在运行期间并发调用，
  通过重放事件流构建，快照是深拷贝，外部修改不会影响引擎内部状态。
- 快照包含每个任务的稳定状态、尝试次数（`Attempts`）、失败性质（`Failure`）
  与最近一次失败原因（`Err`）。

## 可配置项

### `dag.Options`（引擎级）

| 字段          | 含义           | 默认值          |
|---------------|----------------|-----------------|
| `Concurrency` | 并发上限       | `<=0` 表示不限  |
| `Logger`      | 日志输出       | stderr 标准日志 |

### `dag.Task`（任务级）

| 字段      | 含义                       | 默认值     |
|-----------|----------------------------|------------|
| `Timeout` | 单次尝试超时               | `0` 不限   |
| `Retries` | 失败重试次数（不含首次）   | `0` 不重试 |
| `Backoff` | 重试退避间隔               | `0` 立即   |

### `cmd/dagrun` 命令行

| 参数            | 含义                             |
|-----------------|----------------------------------|
| `-dir`          | 事件存储目录                     |
| `-run-id`       | 运行 ID（相同 ID 触发恢复语义）  |
| `-concurrency`  | 并发上限                         |

## 日志与可解释性

引擎在关键判定处打印日志：运行开始/结束、恢复时重放的事件数、每次尝试的
序号与重试预算、失败分类与重试决策、任务被跳过的原因等，例如：

```
run=demo recovered 16 events, status=canceled
run=demo task=transform attempt=2 failures=1/3 timeout=0s
run=demo task=transform attempt=2 failed kind=retryable retryable, will retry: transient error
run=demo task=notify skipped: upstream not succeeded
```

## 测试覆盖

`dag/` 包内的测试覆盖：环依赖与缺失依赖（可区分的校验错误）、上游失败阻断下游、
并发上限、超时与重试（可重试 / 终止性失败区分）、中断恢复（已完成任务不重跑、
崩溃现场续跑、重复恢复幂等）、并发快照一致性、存储幂等与跨实例重载。
全部测试在竞态检测（`-race`）下通过。
