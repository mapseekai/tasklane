# @mapseekai/worker-runtime

面向交互式应用与大规模二进制数据处理的独立 TypeScript Worker 运行时。**零运行时依赖**，支持浏览器 Web Worker 与 Node.js `worker_threads`。

这不是地图渲染器，也不包含 GPU 设备或 GIS 数据模型。当前版本没有接入 emap，没有改变任何现有项目。WebGL2/WebGPU 的资源所有权与上传调度应由后续集成中的 RenderEndpoint 负责。

## 已实现

- 应用级 Runtime、嵌套 Scope 和幂等释放；不同地图可以共享执行资源，但任务、缓存和结果按作用域隔离。
- 全局 Worker/活跃任务/排队数量上限；任务优先级、组间公平调度及等待老化；懒启动、空闲回收、跨池空闲容量再分配。
- 在 `prepare()` 前原子预留输入、暂存和结果额度；消费者持有 ResultLease 时，结果额度不释放，后续任务受到背压。
- 显式 Transferable 所有权转移；不自动 detach 调用者数据。独立 Worker 代际、协议握手、能力检查、错误和超时。
- 协作取消、只丢弃结果、终止线程三种策略；调用取消与物理计算结束分别跟踪。
- 软缓存亲和性；独占 Worker 的严格 Session；会话丢失明确失败，不偷偷换线程。
- Worker 内按作用域命名的有界 LRU 与不可静默淘汰的会话状态；详细执行计时和资源预留统计。

完整定义见 [API](docs/api.md)、[架构与边界](docs/architecture.md)、[测试方法](docs/testing.md)、[实测性能报告](docs/performance.md)。

## 构建与验证

当前为仓库中的 `0.1.0`，**尚未发布到 npm**。需要 Node.js 22+ 和 pnpm。

```sh
git clone git@github.com:mapseekai/worker-runtime.git
cd worker-runtime
pnpm install --frozen-lockfile
pnpm build
pnpm typecheck
pnpm test
pnpm exec playwright install chrome firefox webkit
pnpm test:browser
pnpm test:stress
pnpm verify
```

`pnpm test:browser` 默认运行 Chrome、Firefox、WebKit；单独验证可用 `BROWSERS=chrome pnpm test:browser`。WebKit 测试不等于在所有 Safari/iOS 设备上完成验收。

```sh
pnpm benchmark                 # Node：完整矩阵，每组 3 次
node benchmarks/node.mjs --quick
pnpm benchmark:browser         # Chrome：完整矩阵，每组 3 次
pnpm dev                       # 浏览器大数据转换示例，端口 4196
pnpm pack                      # 生成本地可安装 tarball，不发布 npm
```

原始性能输出在被 git 忽略的 `benchmark-results/`，已验收的报告快照在 `docs/results/`。压力测试分别覆盖单个 256 MiB ArrayBuffer、逻辑总量 1 GiB 的有界分块流和取消风暴，不以“累计 1 GiB”冒充“单次导入 1 GiB 文件”。

## 浏览器使用

安装本地构建的 tarball 后，主线程：

```ts
import { browserWorker, createWorkerRuntime, transferBuffers } from '@mapseekai/worker-runtime';
import type { TaskType } from '@mapseekai/worker-runtime';

type Tasks = { convert: TaskType<Float64Array, Float32Array> };
const MiB = 1024 ** 2;
const runtime = createWorkerRuntime<Tasks>({
  pools: {
    geometry: {
      factory: browserWorker(new URL('./geometry.worker.js', import.meta.url)),
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
const scope = runtime.createScope('map');
const handle = scope.enqueue('convert', {
  pool: 'geometry',
  priority: 'interactive',
  group: 'roads',
  affinity: 'source-a/shard-1',
  budget: { inputBytes: 8 * MiB, scratchBytes: 0, outputBytes: 4 * MiB },
  prepare: ({ signal }) => {
    signal.throwIfAborted();
    // 真正获得额度后才创建输入。不要在排队之前构造巨量数据。
    const input = new Float64Array(MiB);
    return { payload: input, transfer: transferBuffers(input) };
  },
});
const result = await handle.result;
try {
  console.log(result.value.length);
  // 后续接 GPU 时，要等上传端真正消费数据，而不是刚入上传队列就 release。
} finally {
  result.release();
}
await scope.dispose();
await runtime.dispose();
```

`geometry.worker.ts`：

```ts
import { browserHost, output, serve } from '@mapseekai/worker-runtime/host';
import type { TaskType } from '@mapseekai/worker-runtime';

type Tasks = { convert: TaskType<Float64Array, Float32Array> };
serve<Tasks>(browserHost(self), {
  convert(input) {
    const result = Float32Array.from(input);
    return output(result, [result.buffer]);
  },
});
```

上例展示运行时调用，不把直接 Float32 转换当作测绘精度方案。性能用例另外提供 Float64→高低位 Float32 布局、经纬度→Web Mercator→高低位布局，以及 GeoJSON 几何平坦化。

Node.js 对应入口为 `@mapseekai/worker-runtime/node`：主线程用 `nodeWorker(url)`，工作线程用 `nodeHost()` 配合相同 `serve()`。

## 必须理解的边界

**预算约束的是声明并由运行时管理的资源，不是浏览器总内存硬上限。** JavaScript 对象、第三方 WASM 内部分配、GC 延迟、结构化克隆的临时副本及 GPU 驱动内存不可能只靠几个 TypedArray 字节数准确覆盖。原始数据仍在调用者持有时也另计。

**取消 Promise 不代表 Worker 空闲。** 用 `handle.settled` 等待物理结束。同步算法不能被取消消息中途抢占；需要分块/checkpoint 或明确授权的硬取消。

**ResultLease.release() 不是强制回收外部引用。** 它撤销租约访问并释放运行时计账；调用者自行保留的数组引用仍然占内存。

**这里没有自动线程数越多越快的承诺。** 转换可能受内存带宽、主线程准备、消费者处理或算法串行部分限制。报告同时列出单线程、协作式主线程、clone/transfer、1/2/4 Worker 及裸 Worker 对照。

## 许可

MIT。项目不复制 MapLibre/emap 源码，也不依赖它们。
