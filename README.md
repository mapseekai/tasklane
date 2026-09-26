# tasklane

面向浏览器 Web Worker 与 Node.js `worker_threads` 的高性能任务运行时，适合 GIS、图形处理、数据转换、WASM 计算、栅格/矢量预处理、文件解析等 CPU 密集型与大规模二进制数据场景。

核心目标是统一管理 **Worker 生命周期、任务调度、资源预算、数据所有权、取消、亲和性、结果背压与运行指标**，让业务代码专注于任务实现。

## 核心能力

- **统一 Runtime**：应用级 Runtime、嵌套 Scope、统一生命周期和资源释放。
- **有界 Worker Pool**：统一控制 Worker 数量、活跃任务数和等待队列长度。
- **任务调度**：支持 `interactive / foreground / background` 严格优先级、可选等待老化和组间公平调度。
- **延迟输入准备**：任务获得执行和内存额度后才调用 `prepare()`，适合大文件、大数组和分块计算。
- **Transferable 支持**：显式转移 `ArrayBuffer` 所有权，降低大二进制数据在线程间传递的复制成本。
- **结果背压**：通过 `ResultLease` 保留结果额度，消费者释放后再允许后续任务继续占用对应资源。
- **三种取消模式**：协作取消、结果丢弃、终止 Worker，适配不同算法的可中断能力。
- **Worker Affinity**：支持可重建缓存的软亲和性，提高长期驻留 Worker 的缓存复用率。
- **Session**：为数据库句柄、WASM 实例、长期状态等提供独占 Worker 的严格亲和性。
- **Worker 本地缓存**：按 Scope / Session 隔离的有界 LRU，支持固定状态和可淘汰缓存。
- **故障与代际管理**：协议握手、Worker epoch、超时、崩溃、迟到响应和会话失效处理。
- **运行指标**：记录排队、启动、输入准备、Worker 执行、往返、总耗时及资源预留峰值。
- **浏览器与 Node.js**：同一任务模型支持 Web Worker 和 `worker_threads`。
- **零运行时依赖**：核心运行时保持轻量，便于集成到现有应用与插件体系。

详细说明见：

- [API 参考](docs/api.md)
- [资源与调度契约](docs/resources.md)
- [架构设计](docs/architecture.md)
- [测试与验收](docs/testing.md)
- [性能测试](docs/performance.md)

## 适用场景

### 大规模二进制数据转换

适合几十 MiB 到 GiB 级的分块数据处理，例如：

- Float64 / Float32 坐标布局转换
- 投影计算
- 几何平坦化
- 二进制格式解析与编码
- Arrow / Parquet / FlatGeobuf 等数据预处理

运行时通过输入、暂存、结果额度和 Worker 数量限制形成有界执行链路。

### GIS 与图形计算

适合将高成本 CPU 工作放到 Worker：

- 几何三角化
- Buffer / Clip / Intersect 等空间计算
- 空间索引构建
- 栅格块解码和重投影
- 矢量瓦片解析
- 字体 shaping、字形生成

### WASM 与状态型计算

Session 适合需要长期绑定同一 Worker 的运行时：

- DuckDB / SQLite
- GDAL
- 长驻 WASM 实例
- 数据库连接或数据集句柄
- 带内部缓存的解析器和计算引擎

### 高交互应用

优先级和公平调度适合同时存在交互任务与后台任务的应用：

- 当前视口数据：`interactive`
- 用户主动执行的分析：`foreground`
- 预取、缓存构建：`background`

## 安装与验证

当前开发版本为 `0.2.0-beta.1`，适合业务试点验证；发布状态以 registry 为准。仓库构建和 Node 示例需要 Node.js 22+ 与 pnpm，浏览器端使用 Web Worker。

```sh
npm install @mapseekai/tasklane@beta
```

源码构建与示例验证：

```sh
git clone git@github.com:mapseekai/tasklane.git
cd tasklane
pnpm install --frozen-lockfile
pnpm build
```

完整验证：

```sh
pnpm typecheck
pnpm test
pnpm exec playwright install chrome firefox webkit
pnpm test:browser
pnpm test:stress
pnpm verify
```

性能测试：

```sh
pnpm benchmark
node benchmarks/node.mjs --quick
pnpm benchmark:browser
pnpm benchmark:scheduler
pnpm benchmark:cache
```

浏览器示例：

```sh
pnpm dev
```

打开：

```text
http://127.0.0.1:4196
```

示例可选择 64 MiB、256 MiB、1 GiB 累计输入，以及 1 / 2 / 4 个 Worker，观察吞吐量、进度和任务资源统计。

## 浏览器使用

```ts
import {
  browserWorker,
  createWorkerRuntime,
  consumeResult,
  transferBuffers,
} from '@mapseekai/tasklane';
import type { TaskType } from '@mapseekai/tasklane';

type Tasks = {
  convert: TaskType<Float64Array, Float32Array>;
};

const MiB = 1024 ** 2;

const runtime = createWorkerRuntime<Tasks>({
  pools: {
    compute: {
      factory: browserWorker(new URL('./compute.worker.js', import.meta.url)),
      size: 2,
      cacheBytes: 8 * MiB,
    },
  },
  maxWorkers: 2,
  maxActiveTasks: 2,
  budgets: {
    inputBytes: 32 * MiB,
    scratchBytes: 32 * MiB,
    outputBytes: 32 * MiB,
    cacheBytes: 16 * MiB,
  },
});

try {
  await runtime.withScope('dataset', async (scope) => {
    const handle = scope.enqueue('convert', {
      pool: 'compute',
      priority: 'interactive',
      group: 'layer-roads',
      affinity: 'dataset-a/shard-1',
      // 根 TypedArray 按完整 backing store 计费。
      budget: { inputBytes: 8 * MiB, scratchBytes: 0, outputBytes: 4 * MiB },
      prepare: ({ signal }) => {
        signal.throwIfAborted();
        const input = new Float64Array(MiB);
        return { payload: input, transfer: transferBuffers(input) };
      },
    });
    await consumeResult(handle, (value) => {
      console.log('转换后的元素数', value.length);
    });
  });
} finally {
  await runtime.dispose();
}
```

Worker：

```ts
import {
  browserHost,
  output,
  serve,
} from '@mapseekai/tasklane/host';
import type { TaskType } from '@mapseekai/tasklane';

type Tasks = {
  convert: TaskType<Float64Array, Float32Array>;
};

serve<Tasks>(browserHost(self), {
  convert(input) {
    const result = Float32Array.from(input);
    return output(result, [result.buffer]);
  },
});
```

Node.js 使用 `@mapseekai/tasklane/node` 中的 `nodeWorker()` 和 `nodeHost()`，任务模型保持一致。仓库提供可直接运行的 `examples/node.mjs` 和 `examples/node-worker.mjs`：

```sh
pnpm build
node examples/node.mjs
```

示例演示复合数据包计账、显式转移，以及消费失败时仍释放结果、Scope 和 Runtime。

## 资源管理模型

运行时提供四类预算：

```text
inputBytes
scratchBytes
outputBytes
cacheBytes
```

其中：

- `inputBytes`：任务输入包
- `scratchBytes`：算法执行暂存
- `outputBytes`：执行中及等待消费的结果
- `cacheBytes`：Worker 长驻缓存

配合 `maxWorkers`、`maxActiveTasks`、`maxQueuedTasks` 和 `maxResultLeases`，可以限制任务并发、等待队列、结果租约数量和声明额度。输入输出的二进制 backing store 与传输元数据都会做大小校验；实际 JS 堆、结构化克隆副本和算法暂存需要应用另外约束。详见 [资源与调度契约](docs/resources.md)。

默认 `maxActiveTasks = min(pool capacity, maxWorkers)`，同步 prepare 占一个 active 槽位；异步 prepareAsync 使用独立的 maxPreparingTasks 窗口。消费结果推荐 `consumeResult(handle, consume)`；仅需完成通知时设置 `discardResult: true`。Scope 可使用 `runtime.withScope()` 自动关闭。

## 推荐起点

交互式浏览器应用可以从以下配置开始：

```text
Worker 数：2
单任务分块：4–16 MiB
interactive：当前视口 / 当前操作
foreground：用户主动分析
background：预取 / 缓存构建
Transferable：用于任务自有 ArrayBuffer
Session：用于长期状态型运行时
```

实际 Worker 数可结合算法复杂度、数据规模、内存带宽和主线程响应性通过基准测试调整。

## 数据预算与执行边界

任务预算包括字符串、普通数组和协议元数据，使用 `packetByteLength(value)` 计算传输计费量；复合 TypedArray 包还需预留少量元数据空间。结果在首次读取 `lease.value` 时解码。默认采用严格优先级，需要跨优先级老化时设置 `priorityPolicy: 'ageing'`。

`prepare` 用于同步输入构造；`enqueuePrepared` 支持受预算约束的异步 prepareAsync，计算密集的准备工作适合在 Worker handler 内执行。`ctx.scratch` 提供受额度限制的临时 ArrayBuffer；普通 JS 分配和外部资源仍由算法管理。Runtime 面向可信 Worker，管理任务准入、协议数据与显式申报的资源额度。完整语义见 [资源与调度契约](docs/resources.md)。

## License

MIT

File/Blob 可作为受约束附件传入或返回，通过任务 `blobLimits` 声明逻辑大小上限，并支持在 Worker 内按范围读取。详见 [文件与分块使用指南](docs/file-and-session.md)。

通过 `scope.enqueuePrepared()` / `session.enqueuePrepared()` 可先取得额度再异步准备输入；`RuntimeError.remoteError` 保留业务错误信息；公共 `iterateResults()` 提供逐块拉取与确定性清理。用法见 [准备与消费 API](docs/api.md#异步输入准备)。

## emap 资源运行时升级

0.2.0-beta.1 新增常驻资源额度、Session 准入/副本回收与 footprint 组路由、交互资源预留、阻塞索引、内存压力回收、可选自适应扩缩容，以及按实际 chunk 上界准入的分块接口。原有任务 API 保持可用；Worker 协议升为 v6，主线程包与 Worker bundle 必须一起更新。见 [资源调度指南](docs/resource-scheduling.md) 和 [评估与迁移指南](docs/emap-upgrade.md)。
