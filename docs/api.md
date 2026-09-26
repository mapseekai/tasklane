# API 参考

## 1. 包入口

| 入口 | 能力 |
| --- | --- |
| `@mapseekai/tasklane` | Runtime、Scope、Session、浏览器 Worker 工厂、二进制工具、公共类型 |
| `@mapseekai/tasklane/host` | Worker Host、任务注册、TaskContext、Worker 本地缓存 |
| `@mapseekai/tasklane/node` | Node.js `worker_threads` 适配器 |
| `@mapseekai/tasklane/testing` | 基于 `structuredClone` 的同域测试端点 |

## 2. createWorkerRuntime

```ts
const runtime = createWorkerRuntime({
  pools: {
    compute: {
      factory: browserWorker('/workers/compute.js'),
      size: 2,
      cacheBytes: 32 * MiB,
    },
  },
});
```

主要配置：

| 配置 | 默认值 | 作用 |
| --- | ---: | --- |
| `maxWorkers` | 所有 Pool size 之和 | Runtime 最大物理 Worker 数 |
| `maxPreparingTasks` | 2 | 异步生产与已准备输入的总窗口 |
| `maxActiveTasks` | `min(pool capacity, maxWorkers)` | 同时处于 Worker 启动、同步 prepare 或执行阶段的任务数 |
| `maxQueuedTasks` | 1024 | 等待 Worker 执行准入的任务数量，包含异步准备与已准备输入 |
| `budgets.inputBytes` | 64 MiB | 输入数据总额度 |
| `budgets.scratchBytes` | 128 MiB | 算法暂存总额度 |
| `budgets.outputBytes` | 64 MiB | 执行中和待消费结果总额度 |
| `budgets.cacheBytes` | 128 MiB | Worker 长驻缓存总额度 |
| `startupTimeoutMs` | 10000 | Worker 启动与协议握手超时 |
| `queueTimeoutMs` | 120000 | 等待准入超时 |
| `executionTimeoutMs` | 120000 | 从准入开始，包含启动、同步准备和 Worker 执行 |
| `priorityPolicy` | strict | 严格优先级；ageing 允许跨优先级提升 |
| `ageingMs` | 2000 | ageing 策略提升间隔 |
| `maxResultLeases` | 1024 | 租约与已准入工作数量上限 |
| `maxScopes` | 4096 | 根与子 Scope 总数上限 |
| `budgetWaitMs` | 1000 | 等待预算任务开始保护所需额度的时间 |
| `releaseTimeoutMs` | 10000 | Worker 释放确认超时 |
| `maxAffinityEntries` | 4096 | 亲和性历史数量 |
| `onDiagnostic` | — | 诊断事件回调 |

## 3. PoolOptions

```ts
interface PoolOptions {
  factory: () => WorkerEndpoint;
  size: number;
  cacheBytes?: number;
  cacheEntries?: number;
  allowHardCancel?: boolean;
  idleTimeoutMs?: number;
}
```

Pool 的 `cacheBytes` 默认 0，`cacheEntries` 默认 4096，`allowHardCancel` 默认 false。`idleTimeoutMs` 默认 30000，设置 0 禁用空闲过期；Session 持续持有绑定的 Worker，直至关闭或 Worker 失效。默认每个存活 Worker 都预留完整 cacheBytes；启用 resize/pressure/adaptive 后按已确认的当前缓存上限计账，构造配置保持硬上限。

适用方式：

```text
compute pool     → 几何、投影、格式转换
raster pool      → 解码、重采样、栅格计算
font pool        → shaping、glyph 生成
database pool    → 数据库 Session
```

### browserWorker

```ts
browserWorker(url, options?)
```

默认创建 module Worker。

```ts
factory: browserWorker(
  new URL('./worker.js', import.meta.url),
  { name: 'compute-worker' },
)
```

### nodeWorker

```ts
import { nodeWorker } from '@mapseekai/tasklane/node';
```

用于 Node.js `worker_threads`。`nodeHost()` 在 Worker 入口传给 serve；已有专用 Worker 实例也可使用同入口导出的 `nodeEndpoint(worker)` 包装。Runtime 拥有 endpoint 的终止权，不应将同一物理 Worker 交给多个 Runtime 或其他任务系统共享。

## 4. Scope

创建根 Scope：

```ts
const scope = runtime.createScope('map-a');
```

创建子 Scope：

```ts
const source = scope.createScope('roads');
```

Scope 适合表达：

- 地图实例
- 数据集
- 数据源
- 文档
- 插件
- 工作区

每个 Scope 拥有独立的任务、结果租约、Session 和 Worker 缓存命名空间。

释放：

```ts
await scope.dispose();
```

销毁等待物理任务、子 Scope、Session 与 Worker 释放确认；确认超时或资源清理失败会拒绝。临时作用域推荐：

```ts
const value = await runtime.withScope('temporary', async (scope) => {
  return consumeResult(scope.enqueue('project', options), (result) => summarize(result));
});
```

`options`、`summarize` 是应用提供的任务参数和消费函数；回调结束时 withScope 会销毁作用域。返回后仍保留的数据引用由应用自行管理。

## 5. enqueue

```ts
const handle = scope.enqueue('project', {
  pool: 'compute',
  priority: 'interactive',
  group: 'roads',
  affinity: 'source-a/shard-17',
  cancellation: 'cooperative',
  signal,
  queueTimeoutMs: 5000,
  executionTimeoutMs: 30000,
  budget: {
    inputBytes: 8 * MiB + 4096,
    scratchBytes: 16 * MiB,
    outputBytes: 8 * MiB + 4096,
  },
  prepare: ({ signal }) => {
    signal.throwIfAborted();
    const packet = createPacket();
    return {
      payload: packet,
      transfer: transferBuffers(packet.coordinates),
    };
  },
});
```

示例中的 `createPacket()` 应返回最多 8 MiB backing store 和不超过 4096 字节协议元数据的普通包，输出也需满足对应上限；其他对象图应按实际编码结构确定元数据上限。

### priority

```ts
'interactive' | 'foreground' | 'background'
```

典型用途：

| priority | 场景 |
| --- | --- |
| interactive | 当前视口、拖动后的补数据、实时交互 |
| foreground | 用户主动分析、导入、导出 |
| background | 预取、缓存构建、后台索引 |

默认任务优先级为 foreground。Runtime 默认 priorityPolicy 为 strict；只有显式设置 ageing 才会跨优先级提升等待任务。两种策略均在准入时决定执行顺序，已执行任务持续运行至完成或取消。

### group

`group` 用于同一 Scope 内的公平调度，例如：

```text
layer-roads
layer-buildings
labels
raster-tiles
```

### affinity

`affinity` 提供软 Worker 亲和性：

```ts
affinity: 'dataset-a/chunk-42'
```

适合存在 Worker 本地缓存的可重建任务。

### budget

```ts
budget: {
  inputBytes,
  scratchBytes,
  outputBytes,
}
```

额度在任务准入时预留，使大规模任务保持有界并发。

### prepare

`prepare()` 在任务获得准入后同步调用，必须直接返回 `{ payload, transfer? }`。Promise/thenable 返回值会以 INVALID_ARGUMENT 拒绝；异步准备放在 Worker handler 中。

适合在这里完成：

- 创建精确大小 TypedArray
- 从源模型复制当前分块
- 编码 Worker 输入包
- 收集 Transferable

## 6. TaskHandle

```ts
interface TaskHandle<T> {
  readonly id: string;
  readonly state: TaskState;
  readonly timing: TaskTiming;
  readonly result: Promise<ResultLease<T>>;
  readonly settled: Promise<void>;
  cancel(reason?: unknown): void;
}
```

状态：

```text
queued
starting
preparing
running
cancelling
succeeded
failed
cancelled
```

### result

```ts
const result = await handle.result;
```

### settled

```ts
await handle.settled;
```

`settled` 在物理生命周期结束后兑现；执行成功或失败由 `result` 表达，成功结果通过 ResultLease 管理。仅等待完成时可设置 `discardResult: true`，并观察 `result` 获知错误；该模式成功时返回已释放租约，访问其 value 会抛出 RESULT_RELEASED。

### timing

```ts
handle.timing
```

包含：

```text
queueMs
startupMs
prepareMs
roundTripMs
workerMs
totalMs
```

## 7. ResultLease

```ts
const lease = await handle.result;

try {
  await consume(lease.value);
} finally {
  lease.release();
}
```

属性：

```ts
lease.value
lease.byteLength
lease.released
```

ResultLease 适合：

- 分块流水线
- 异步写盘
- 网络上传
- 后续转换
- 渐进消费

结果额度在 `release()` 后归还 Runtime。`consumeResult(handle, consume)` 可确保消费结束后释放；`discardResult: true` 在任务成功后直接丢弃结果。`maxResultLeases` 默认 1024。

## 8. Session

```ts
const session = scope.session('database');
```

Session 独占一个物理 Worker，适合状态型执行环境。

```ts
const opened = await session.enqueue('open', {
  budget,
  prepare: () => ({ payload: config }),
}).result;

opened.release();

const queried = await session.enqueue('query', {
  budget,
  prepare: () => ({ payload: sql }),
}).result;
try { await consume(queried.value); } finally { queried.release(); }
```

状态：

```text
unbound
bound
lost
closed
```

适用场景：

- DuckDB
- SQLite
- GDAL dataset
- 长驻 WASM 实例
- 带内部状态的解析器

释放：

```ts
await session.dispose();
```

## 9. Worker Host

Worker 入口：

```ts
import {
  browserHost,
  output,
  serve,
} from '@mapseekai/tasklane/host';

serve(browserHost(self), {
  project(input, ctx) {
    const result = projectCoordinates(input);
    return output(result, [result.buffer]);
  },
});
```

## 10. HostContext

任务处理器可以使用：

```ts
ctx.signal
ctx.scopeId
ctx.sessionId
ctx.epoch
ctx.cache
ctx.scratch.allocate(bytes)
ctx.scratch.release(buffer)
ctx.progress(value)
await ctx.checkpoint()
```

### progress

```ts
ctx.progress({ completed, total });
```

普通记录/数组和有限标量，最多 4 KiB 元数据、64 个对象、256 条边，禁止二进制值。发送间隔至少 16 ms，最多单条在途，ACK 前合并为最新快照。任务完成后的 progress 不发送。

### scratch

```ts
const buffer = ctx.scratch.allocate(1024); // 任务需申报 scratchBytes >= 1024
try {
  const temporary = new Uint8Array(buffer);
  // 临时工作区随任务释放；跨任务数据应使用独立的常驻存储。
  temporary.fill(0);
} finally {
  ctx.scratch.release(buffer); // detach 全部 view，归还 arena 内部可用空间
}
```

`ctx.scratch.bytes` 为当前 arena 已分配字节，`limit` 为任务 scratch 额度。任务结束自动关闭 arena；任务级账本的整笔预留直到物理任务结束才释放。普通 JS 分配和 JSON.parse 的内存由应用管理。

### checkpoint

```ts
for (let i = 0; i < items.length; i++) {
  process(items[i]);

  if ((i & 8191) === 0) {
    await ctx.checkpoint();
  }
}
```

checkpoint 会让出真实事件循环以接收取消消息；完成后调用会以 CLOSED 拒绝。默认 cooperative 依赖任务主动响应，同步长循环可选 terminate，并在 Pool 配置 allowHardCancel: true。三种取消策略均受 executionTimeoutMs 物理截止时间约束。

## 11. Worker Cache

```ts
ctx.cache.get(key)
ctx.cache.set(key, value, bytes)
ctx.cache.setBinary(key, typedArrayOrDataView)
ctx.cache.setPinned(key, value, bytes)
await ctx.cache.delete(key)
```

普通缓存采用 LRU。bytes 至少覆盖 `dataByteLength(value, { resident: true })`；保存后通过外部引用扩容必须重新申报。

典型缓存：

- 三角化结果
- 索引
- 解码块
- 字体数据
- 计算中间结构

Session 可使用 `setPinned()` 保存普通长期数据；类实例和 WASM 等不透明资源使用 `setResource(key, value, bytes, disposer)`，并 `await cache.delete(key)` 等待清理。

## 12. Transferable

```ts
const bytes = new Uint8Array(size);

return {
  payload: bytes,
  transfer: transferBuffers(bytes),
};
```

`transferBuffers()` 支持：

- `ArrayBuffer`
- 覆盖完整 backing store 的 TypedArray
- 去重
- 显式所有权转移

适合大坐标数组、像素块和文件分片。

## 13. 数据计费工具

```ts
binaryByteLength(value, {
  maxObjects: 100_000,
  maxEntries: 1_000_000,
  maxPending: 100_000,
  maxMetadataBytes: 64 * 1024 ** 2,
})
```

`binaryByteLength` 统计二进制 backing store；一般任务预算使用包含元数据的 `packetByteLength`。

支持：

- ArrayBuffer
- SharedArrayBuffer
- TypedArray
- Array
- plain object
- Map
- Set
- Date
- RegExp

对同一 backing store 自动去重。

```ts
packetByteLength('abcd'); // 8，根字符串按 UTF-16 计费
packetByteLength(new Uint8Array(8)); // 8，根 view 的完整 backing store
packetByteLength({ bytes: new Uint8Array(8) }); // 大于 8，还包含图元数据
dataByteLength('abcd'); // 8，普通缓存的驻留数据计费
```

`packetByteLength` 会编码整个值，适合已经存在的小型输入；大型数据应先声明上限，准入后再同步构造。其参数为 value。`dataByteLength` 接受与 binaryByteLength 相同的 limits，缓存计算应传 resident: true；计入标量/属性名和每个非二进制对象 16 字节的结构费用。这些计数用于确定资源申报量，真实 JS 堆占用需结合运行环境测量。

结果在首次读取 `lease.value` 时解码，`lease.byteLength` 是传输计费量。

## 14. RuntimeStats

```ts
runtime.stats
```

包含：

```text
queued
active
workers
closingWorkers
quarantinedWorkers
scopes
leases
workerStarts
workerTerminations
inputBytes
outputBytes
cacheUsedBytes
reserved
peakReserved
completed
cancelled
failed
observerErrors
```

适合运行时监控、性能诊断和自动化基准记录。

## 15. 错误码

| 错误码 | 含义 |
| --- | --- |
| `INVALID_ARGUMENT` | 配置或任务参数校验失败 |
| `CLOSED` | Runtime / Scope / Session 已关闭 |
| `QUEUE_FULL` | 等待队列达到上限 |
| `QUEUE_TIMEOUT` | 等待准入达到截止时间 |
| `STARTUP_TIMEOUT` | Worker 启动或握手达到截止时间 |
| `EXECUTION_TIMEOUT` | 执行阶段达到截止时间 |
| `BUDGET_EXCEEDED` | 资源额度达到上限 |
| `ABORTED` | 任务取消 |
| `WORKER_FAILED` | Worker 端点故障 |
| `PROTOCOL_ERROR` | 协议校验失败 |
| `UNKNOWN_TASK` | Worker Host 缺少任务实现 |
| `SESSION_LOST` | Session 对应 Worker 失效 |
| `HARD_CANCEL_DENIED` | Pool 配置未启用终止式取消 |
| `RESULT_RELEASED` | ResultLease 已释放 |
| `REMOTE_ERROR` | Worker 任务执行错误 |

## 资源契约与恢复接口

```ts
runtime.resourceDiagnostics(); // 定位持有任务、租约和 Session 的所有者
await runtime.retryTermination(); // 重试物理终止失败的隔离 Worker
await runtime.disposeWithin(5000); // 限制等待时间；超时后后台清理继续进行
```

`scope.disposeWithin(ms)` 同样只约束等待时间，Session 使用 dispose。协议 v7 的 request 携带 Packet payload、maxOutputBytes、maxOutputBlobBytes 和 maxScratchBytes；progress、Scope 释放和 cache-control 均使用 ACK。v7 的 Packet 数组支持按位置编码，主线程包与 Worker bundle 必须同步更新。

详见 [资源与调度契约](resources.md)，包括元数据限制、`budgetWaitMs`、`releaseTimeoutMs`、进度 ACK、Scope 释放确认、`withScope`、`disposeWithin`、`resourceDiagnostics` 和 `retryTermination`。

### TaskOptions.blobLimits

```ts
scope.enqueue('open', {
  pool: 'reader',
  budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 0 },
  blobLimits: { inputBytes: file.size, outputBytes: 0 },
  prepare: () => ({ payload: file }),
});
```

可选 `{ inputBytes: number; outputBytes: number }`，省略时均为 0；提供对象时两项都必填。限制每包 File/Blob 逻辑大小，重复对象仅计一次，与全局 inputBytes/outputBytes 额度独立。4096 只是此短文件名示例的元数据上限，超长文件名需要更高预算。`packetByteLength(file)` 返回引用与元数据费用，file.size 由 blobLimits 单独校验。Scope 与 Session 任务均支持，附件通过克隆传递。语义及限制见 [资源契约](resources.md#fileblob-附件)。

## 异步输入准备

Scope 与 Session 都提供 `enqueuePrepared(name, options)`，返回原有 TaskHandle：

```ts
const task = scope.enqueuePrepared('convert', {
  pool: 'compute',
  budget: { inputBytes: 8 * MiB, scratchBytes: 16 * MiB, outputBytes: 8 * MiB },
  preparationScratchBytes: 4 * MiB,
  preparationTimeoutMs: 30_000,
  executionTimeoutMs: 120_000,
  prepareAsync: async ({ signal }) => {
    const bytes = await loadInput(signal);
    signal.throwIfAborted();
    return { payload: bytes, transfer: transferBuffers(bytes) };
  },
});
await consumeResult(task, consumeOutput);
```

示例中的 loadInput 和 consumeOutput 由应用提供，输入需满足声明的大小与所有权。`PreparedTaskOptions` 保留 TaskOptions 的预算、优先级、group、affinity、取消、超时和 progress 配置，使用 prepareAsync 构造输入。Session 使用 `SessionPreparedTaskOptions`，由其绑定关系提供 pool 和亲和性。

Runtime 配置 `maxPreparingTasks`（默认 2）限制生产中及等待发送的输入数量。完整任务额度在 prepareAsync 执行前预留，scratch 按准备与执行的较大上界计费；准备阶段按结果消费速度接续执行，Worker 在执行准入时绑定。状态依次为 queued、preparing、prepared、starting、running，取消和失败沿用 TaskHandle 的生命周期。

prepareAsync 在调用线程执行，适合等待 fetch、Blob.arrayBuffer 等异步 I/O；async 不会把同步解析或大数组构造移到 Worker。同一优先级、Scope/group、Pool/Session 通道按 FIFO 准入，因此 maxPreparingTasks 是总窗口上限，不保证同组同时准备两项。需要重计算时让 prepare/prepareAsync 只返回小型指令或输入附件，由 handler 处理。

`queueMs` 记录首次准入前的等待，`prepareMs` 记录生产回调时间，`startupMs` 记录绑定 Worker 后的启动等待；`totalMs` 包括准备完成后等待 Worker 的时间。取消后通过 result 获知逻辑结果，通过 settled 等待生产与任务的物理完成。额度和计时细节见 [资源契约](resources.md#异步准备阶段)。

## 远端业务错误

```ts
try {
  await task.result;
} catch (error) {
  if (error instanceof RuntimeError && error.code === 'REMOTE_ERROR') {
    console.log(error.remoteError?.name, error.remoteError?.code);
    console.log(error.remoteError?.message, error.remoteError?.details);
  }
}
```

`remoteError` 类型为 `Readonly<RemoteErrorInfo>`，支持 name、字符串 code、message，以及可选 stack、details、truncated、detailsOmitted。details 类型为递归的 ErrorDetail。Worker 抛出的 Error 可通过普通数据属性携带业务 code/details。字段与大小规则见 [业务错误契约](resources.md#业务错误)。

## 分块结果迭代

```ts
import { iterateResults, consumeResult } from '@mapseekai/tasklane';

const chunks = iterateResults({
  signal,
  next: (signal) => session.enqueue('next', {
    budget: chunkBudget,
    prepare: () => ({ payload: { cursorId } }),
    signal,
  }),
  isDone: (value) => value.done,
  close: () => consumeResult(session.enqueue('closeCursor', closeOptions), () => {}),
});
for await (const chunk of chunks) {
  await consumeChunk(chunk);
}
await chunks.closed;
```

示例使用应用定义的 next/closeCursor 任务、cursorId、chunkBudget、closeOptions 与 consumeChunk，并借用 session；closeOptions 应使用可执行清理的信号。helper 自建资源时可将 close 配置为所属 Scope/Session 的 dispose。

`iterateResults<T>(ResultIterationOptions<T>)` 返回 `ResultIterator<T>`，支持 AsyncIterableIterator、dispose()、retryCleanup() 与 closed。isDone 为 true 的结束标记在内部释放；其他值逐块交给消费者。下一次 next、return、dispose 或 AbortSignal 都会结束当前租约。并发 next 以 INVALID_ARGUMENT 拒绝，消费者逐次请求即可保持单块在途。

break 和消费者异常通过迭代器 return 清理；手工 next 使用 finally + dispose。终止时取消当前请求并等待 settled，再调用 close。closed 保存首次清理尝试的结果，失败可通过该 Promise 观察。显式调用 retryCleanup() 可重试失败的 close；重试期间 dispose() 与其他 retryCleanup() 共用进行中的尝试，成功后的调用直接完成。重试只执行资源清理，迭代保持结束；close 应支持部分完成后的重复调用。原 closed Promise 保留首次结果，以 retryCleanup() 返回值确认恢复。消费者保留块引用时需接管其内存预算。

## 0.2 资源与准入 API

- `runtime.resources.acquire({ kind: 'resident', bytes })` / `scope.resources.acquire(...)` / `session.resources.acquire(...)` 返回 ResourceLease（bytes、released、resize、release）。
- `scope.acquireSession(pool, { mode?, timeoutMs?, signal?, reclaimable?, reclaimPriority?, residentBytes?, priority? })` 联合预留 Worker 和可选常驻额度；`session.resident` 返回该 ResourceLease。省略 residentBytes 时不创建租约；`scope.session(pool, options?)` 保留惰性行为。
- `session.reclaimed` 标识可回收 Session 的关闭原因。
- `runtime.diagnostics()` 返回 RuntimeDiagnostics；即时容量错误为 SessionAdmissionError（CAPACITY_UNAVAILABLE）。
- `TaskOptions.affinity` 支持字符串或 `{ keys: readonly string[] }`。
- `ctx.cache.setResource` 返回 CacheResourceLease（bytes、released、resize、异步 release）。
- `ctx.cache.setBinary(key, view)` 保存共享 backing store 的新原生视图，保留类型、offset、length，丢弃附加属性和子类行为，按完整 backing store 计费；无索引枚举或数据复制。需要保存附加字段时使用 set/setPinned。
- `runtime.stats.cacheStats` / `runtime.diagnostics().pools[i].cacheStats` 提供累计 hits/misses/evictions；仅统计 CacheStore，Worker 更换后累计值保留，任务响应和释放/控制 ACK 更新快照。
- `scope.sessionGroup(sessions)` 借用同源、同 Scope/Pool/准入类别的已绑定 Session；enqueue/enqueuePrepared 根据资源 footprint 选择空闲成员。
- `CacheResourceLease.report(snapshot)` 上报资源内部 hits/misses/evictions、usedBytes 和完整 keys；setResource 第五参数可提供异步 trim 回调。
- `interactiveReserve` 和 Pool.interactiveWorkers 预留交互任务的数量/字节资源；默认 0。资源和 Session 的 priority 默认 foreground。
- `runtime.resizePool(name, { size?, cacheBytes? })`、`trim({ pool?, cacheBytesPerWorker?, workersPerPool?, reclaimSessions? })`、`setMemoryPressure('normal' | 'moderate' | 'critical')` 返回 Promise<MaintenanceReport>，包含成功回收数量、归还 cache 额度与逐 Worker failures。并发维护调用应串行等待。
- Pool.adaptive 可配置 minWorkers/minCacheBytes/sampleMs/idleMs/missRatio；省略时不自动调整。
- `stats.resourceCacheStats`、逐池 resources/reclaim 和 `stats.scheduler` 提供 reader 缓存与回收/阻塞索引诊断。默认值、上限和完整示例见 [资源调度指南](resource-scheduling.md)。
- `ctx.outputLimit` 提供当前任务已准入输出字节上界。
- 主入口导出 `iterateSizedResults`、`SizedResultOptions`、`ChunkDescriptor`；host 入口导出 `createSizedResultSource` 和对应类型。
- `transferOwnedBuffers` 为 `transferBuffers` 的明确所有权别名。

详细参数上限、释放顺序、失败重试与完整迁移示例见 [emap 升级指南](emap-upgrade.md)。
