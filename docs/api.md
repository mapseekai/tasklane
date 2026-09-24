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
| `maxActiveTasks` | `min(pool capacity, 2)` | 同时处于准备或执行阶段的任务数 |
| `maxQueuedTasks` | 1024 | 等待准入的任务数量 |
| `budgets.inputBytes` | 64 MiB | 输入数据总额度 |
| `budgets.scratchBytes` | 128 MiB | 算法暂存总额度 |
| `budgets.outputBytes` | 64 MiB | 执行中和待消费结果总额度 |
| `budgets.cacheBytes` | 128 MiB | Worker 长驻缓存总额度 |
| `startupTimeoutMs` | 10000 | Worker 启动与协议握手超时 |
| `queueTimeoutMs` | 120000 | 等待准入超时 |
| `executionTimeoutMs` | 120000 | 执行阶段超时 |
| `ageingMs` | 2000 | 等待任务优先级老化间隔 |
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

用于 Node.js `worker_threads`。

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
    inputBytes: 8 * MiB,
    scratchBytes: 16 * MiB,
    outputBytes: 8 * MiB,
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

### priority

```ts
'tinteractive' | 'foreground' | 'background'
```

典型用途：

| priority | 场景 |
| --- | --- |
| interactive | 当前视口、拖动后的补数据、实时交互 |
| foreground | 用户主动分析、导入、导出 |
| background | 预取、缓存构建、后台索引 |

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

`prepare()` 在任务获得准入后调用。

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

`settled` 用于等待物理任务生命周期收敛，适合资源关闭、页面退出和任务替换流程。

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
  consume(lease.value);
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

结果额度在 `release()` 后归还 Runtime。

## 8. Session

```ts
const session = scope.session('database');
```

Session 独占一个物理 Worker，适合状态型执行环境。

```ts
await session.enqueue('open', {
  budget,
  prepare: () => ({ payload: config }),
}).result;

await session.enqueue('query', {
  budget,
  prepare: () => ({ payload: sql }),
}).result;
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
ctx.progress(value)
await ctx.checkpoint()
```

### progress

```ts
ctx.progress({ completed, total });
```

进度消息按物理任务进行节流，适合大循环和分阶段算法。

### checkpoint

```ts
for (let i = 0; i < items.length; i++) {
  process(items[i]);

  if ((i & 8191) === 0) {
    await ctx.checkpoint();
  }
}
```

适合实现协作式取消。

## 11. Worker Cache

```ts
ctx.cache.get(key)
ctx.cache.set(key, value, bytes)
ctx.cache.setPinned(key, value, bytes)
ctx.cache.delete(key)
```

普通缓存采用 LRU。

典型缓存：

- 三角化结果
- 索引
- 解码块
- 字体数据
- 计算中间结构

Session 可使用 `setPinned()` 保存长期状态。

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

## 13. binaryByteLength

```ts
binaryByteLength(value)
```

用于统计任务包中的二进制 backing store 大小。

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
leases
workerStarts
workerTerminations
inputBytes
outputBytes
cacheUsedBytes
reserved
peakReserved
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
