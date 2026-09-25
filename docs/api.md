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
| `maxActiveTasks` | `min(pool capacity, maxWorkers)` | 同时处于启动、准备或执行阶段的任务数 |
| `maxQueuedTasks` | 1024 | 等待准入的任务数量 |
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

Pool 的 `cacheBytes` 默认 0，`cacheEntries` 默认 4096，`allowHardCancel` 默认 false。`idleTimeoutMs` 默认 30000，设置 0 禁用空闲过期；Session 占用的 Worker 不会因空闲而自动回收。每个存活 Worker 都预留该 Pool 的完整 cacheBytes。

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

示例中的 `createPacket()` 应返回最多 8 MiB backing store 和不超过 4096 字节协议元数据的普通包，输出也需满足对应上限；这不是所有对象图通用的固定附加费用。

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

默认任务优先级为 foreground。Runtime 默认 priorityPolicy 为 strict；只有显式设置 ageing 才会跨优先级提升等待任务。两者都不抢占正在执行的任务。

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

`settled` 永不拒绝，只表示物理生命周期结束，不代表执行成功，也不会释放普通成功结果。无需结果时设置 `discardResult: true`；需要获知错误仍应观察 `result`。该模式成功时 result 返回已释放租约，不能读取其 value。

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
  // 使用临时工作区；不要让缓存或外部引用依赖它。
  temporary.fill(0);
} finally {
  ctx.scratch.release(buffer); // detach 全部 view，归还 arena 内部可用空间
}
```

`ctx.scratch.bytes` 为当前 arena 已分配字节，`limit` 为任务 scratch 额度。任务结束自动关闭 arena；任务级账本的整笔预留直到物理任务结束才释放。普通 JS 分配和 JSON.parse 不受 arena 度量。

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
ctx.cache.setPinned(key, value, bytes)
await ctx.cache.delete(key)
```

普通缓存采用 LRU。bytes 不得低于 `dataByteLength(value, { resident: true })`；保存后通过外部引用扩容必须重新申报。

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

`binaryByteLength` 只统计二进制 backing store，不能用其返回值申报一般任务预算。

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

`packetByteLength` 会编码整个值，适合已经存在的小型输入；大型数据应先声明上限，准入后再同步构造。它只有 value 参数，不接受 TraversalLimits。`dataByteLength` 接受与 binaryByteLength 相同的 limits，缓存计算应传 resident: true；计入标量/属性名和每个非二进制对象 16 字节的结构费用。任何一个计数都不代表真实 JS 堆占用。

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
await runtime.disposeWithin(5000); // 限制等待时间；拒绝并不意味着清理已完成
```

`scope.disposeWithin(ms)` 同样只约束等待时间，Session 使用 dispose。协议 v3 的 request 携带 Packet payload、maxOutputBytes 和 maxScratchBytes；progress 使用 ACK，Scope 释放也需要 ACK。

详见 [资源与调度契约](resources.md)，包括元数据限制、`budgetWaitMs`、`releaseTimeoutMs`、进度 ACK、Scope 释放确认、`withScope`、`disposeWithin`、`resourceDiagnostics` 和 `retryTermination`。Runtime 与 host 必须使用匹配的协议版本。
