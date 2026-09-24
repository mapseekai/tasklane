# API 参考（0.1.0）

## 包入口

| 入口 | 内容 |
| --- | --- |
| `@mapseekai/worker-runtime` | createWorkerRuntime、RuntimeScope、WorkerSession、browserWorker、binaryByteLength、transferBuffers、RuntimeError、公共类型 |
| `/host` | serve、output、browserHost、HostContext、TaskHandlers、ScopedCache |
| `/node` | nodeWorker、nodeHost；只有此入口导入 node:worker_threads |
| `/testing` | createLoopback；同域 structuredClone 测试端点，不能用于测量并行性能 |

## createWorkerRuntime(options)

类型映射 `type Tasks = { name: TaskType<Input, Output> }` 同时约束主线程任务名、prepare 的输入类型以及 Host 返回类型。消息边界仍需任务函数校验业务字段；TypeScript 不等于运行时数据校验。

| 配置 | 默认值 | 说明 |
| --- | --- | --- |
| pools | 必填 | 名称到 PoolOptions 的映射；至少一个池 |
| maxWorkers | 所有池 size 之和 | 本 Runtime 所有常驻/关闭中物理槽上限 |
| maxActiveTasks | min(所有池容量, 2) | 同时初始化等待/prepare/运行的任务上限 |
| maxQueuedTasks | 1024 | 等待准入的任务数上限；不是数据字节上限 |
| budgets.inputBytes | 64 MiB | 已准入输入数据预留总量 |
| budgets.scratchBytes | 128 MiB | 调用者声明的执行暂存预留总量 |
| budgets.outputBytes | 64 MiB | 执行中输出预留与未消费结果预留总量 |
| budgets.cacheBytes | 128 MiB | 活着的 Worker 固定缓存容量总预留 |
| startupTimeoutMs | 10000 | 单 Worker 协议握手上限 |
| queueTimeoutMs | 120000 | 默认排队截止时间 |
| executionTimeoutMs | 120000 | 获得准入至物理执行完成的上限 |
| ageingMs | 2000 | 等待满此间隔提升一级准入优先级 |
| maxAffinityEntries | 4096 | 软亲和性历史上限 |
| onDiagnostic | 无 | 诊断回调；抛错不会破坏执行器生命周期 |

所有大小、数量与毫秒值要求安全整数；数量必须正数；字节数可为零；超时必须大于零。缓存上限和队列上限不是“文件大小上限”。

## PoolOptions

`factory(): WorkerEndpoint` 每次必须返回全新的物理端点，不允许两个槽共享同一 Worker。`size` 是该池容量；`cacheBytes` 默认 0，`cacheEntries` 默认 4096；`allowHardCancel` 默认 false；`idleTimeoutMs` 默认 30000，0 表示保留空闲 Worker。

`browserWorker(url, options?)` 返回工厂，默认 module Worker。`nodeWorker(url, options?)` 使用 Node Worker，避免把 node:worker_threads 打包进入浏览器。

## Scope

`runtime.createScope(label?)` 创建根作用域；`scope.createScope(label?)` 创建子作用域。label 只供人识别，同名不会产生身份别名。`scope.id` 唯一，`scope.closed` 表示已禁止新任务。

`scope.dispose(): Promise<void>` 幂等：取消自己和子作用域的任务，释放结果和会话，通知 Worker 回收缓存，等待物理任务结束。普通共享 Worker 可继续为其他 Scope 工作。

`runtime.dispose()` 关闭整个运行时并回收所有 Worker；端点终止失败明确 reject。不要忽略 dispose Promise。prepare 必须最终收敛，否则物理任务无法被外部证明完成。

## scope.enqueue(name, options)

必填 `pool`、`budget`、`prepare`。

```ts
{
  pool: 'cpu',
  budget: { inputBytes: 8 * MiB, scratchBytes: 16 * MiB, outputBytes: 8 * MiB },
  prepare: async ({ signal }) => {
    signal.throwIfAborted();
    const packet = await makeOwnedPacket(signal);
    return { payload: packet, transfer: transferBuffers(packet.coordinates) };
  },
  priority: 'interactive',
  group: 'layer-1',
  affinity: 'source-a/stable-shard-3',
  cancellation: 'cooperative',
  signal,
  queueTimeoutMs: 5000,
  executionTimeoutMs: 30000,
  onProgress(value) { /* 更新轻量进度，不进行巨量同步转换 */ },
}
```

priority 默认为 foreground，可选 interactive/foreground/background。group 的公平性在 Scope 内分组。affinity 是软缓存偏好；不是严格会话。cancellation 默认为 cooperative。

prepare 在准入后由主线程执行，闭包本身不会发送给 Worker。异步 prepare 应主动观察 signal；单纯 `await Promise.resolve()` 不会把执行权让给浏览器事件队列。大包复制应分块，并在真实任务队列让出时检查取消。

## TaskHandle 与 ResultLease

`handle.result: Promise<ResultLease<Output>>` 返回结果租约；`handle.cancel(reason?)` 可重复调用。`handle.state` 反映 queued/starting/preparing/running/cancelling/succeeded/failed/cancelled。

`handle.settled: Promise<void>` 只表示物理占用终结，不表示调用成功。逻辑取消可能先于它很久发生。无法确认终止的自定义端点会被隔离，settled 不会假完成。

`handle.timing` 是快照：queueMs、startupMs、prepareMs、roundTripMs、workerMs、totalMs。roundTrip 包含消息交接，workerMs 是 Host 侧执行及结果准备，不能将二者相减当成精确的传输复制时间。多 Worker 的 workerMs 求和不等于墙钟耗时。

`lease.value` 提供结果；`lease.byteLength` 是可计量的二进制 backing store 总量。`lease.release()` 幂等，之后访问 value 报 RESULT_RELEASED。预留上限直到 release 才归还；运行时不能清除调用者自己保存的数组引用。

GPU 集成须等上传端真正消费结果后再释放，而不是刚把 TypedArray 加入一个无界数组队列就释放额度。

## Session

`const session = scope.session('cpu')` 懒绑定独占 Worker。`session.enqueue(name, options)` 不再传 pool/affinity，其余与普通 enqueue 一致。

`session.state` 为 unbound/bound/lost/closed。会话独占而非只固定路由，适合不可重入状态与可被单独终止的任务。过多会话可能占满物理容量，其他任务会等待或排队超时；不会自动偷走会话 Worker。

`session.dispose()` 取消会话任务、释放会话结果并终止其独占 Worker。Worker 丢失后，原 Session 不可再次调用；应由业务重建状态后新建 Session。

## Host

`serve(port, handlers)` 注册静态任务表并返回 Host 释放函数。任务处理器返回 `output(value, transfer?)`，可同步或异步。HostContext 提供 signal、scopeId、sessionId、epoch、cache、progress、checkpoint。

progress 最多每 16ms 发送一次，初次可以立即发送。checkpoint 使用真实 setTimeout 队列让出，检查信号，不声称能中断一个正在运行的同步 WASM 函数。

cache.get(key)、set(key,value,bytes)、setPinned(key,value,bytes)、delete(key) 自动采用当前 Scope/Session 命名空间。常驻预算不能小于实际 TypedArray backing store 大小。缓存内容属于缓存；发送结果前应复制为自有缓冲，**不要把缓存数组的底层 buffer 直接 transfer 导致缓存被 detach**。

## 二进制与输入约束

`transferBuffers(...buffersOrWholeViews)` 去重且要求完整拥有的 ArrayBuffer。传入 partial view、SharedArrayBuffer 会报错；原始数据仍被其他对象使用时不要调用它。Node Buffer 可能使用共享池，不应盲目 transfer 底层 buffer。

`binaryByteLength` 支持 plain object、array、Map、Set、TypedArray、ArrayBuffer、SharedArrayBuffer，Date/RegExp 作为零二进制字节标量。拒绝 accessor/custom class/platform resource，以免隐藏无法计账的二进制成员。默认最多遍历 100000 个元数据对象/待遍历项；大量坐标必须改用 TypedArray。

SharedArrayBuffer 只提供计账，不提供同步、锁或无竞争语义。浏览器可用性由宿主部署环境决定；本版本不依赖 SharedArrayBuffer 或跨源隔离。

## 稳定错误码

| 错误码 | 含义与处理 |
| --- | --- |
| INVALID_ARGUMENT | 参数/对象类型/大小非法，修正输入 |
| CLOSED | Runtime、Scope、Session 已关闭 |
| QUEUE_FULL | 等待任务太多，减少上游并发 |
| QUEUE_TIMEOUT | 等待准入超时，检查容量、未释放结果、会话占槽 |
| STARTUP_TIMEOUT | Worker 资源/CSP/协议握手未完成 |
| EXECUTION_TIMEOUT | 已准入任务超时；同步 prepare 仍需自行退出 |
| BUDGET_EXCEEDED | 单任务/输入/输出/缓存预算不足；应减小包或显式调整预算 |
| ABORTED | 调用取消；用 settled 等待物理结束 |
| WORKER_FAILED | 端点失败、崩溃或终止失败；不可无条件重试有副作用任务 |
| PROTOCOL_ERROR | 主包/Worker 版本或消息不符合协议 |
| UNKNOWN_TASK | Host 没有注册对应任务，prepare 尚未执行 |
| SESSION_LOST | 必需会话的 Worker 丢失，业务必须重建 |
| HARD_CANCEL_DENIED | 该池未授权终止式取消 |
| RESULT_RELEASED | 访问已释放结果 |
| REMOTE_ERROR | 任务处理器抛出的业务/算法错误 |

本库不自动重试数据写入或有外部副作用的任务。重试与幂等键应由业务决定。
