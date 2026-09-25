# 文件与分块使用指南

## 按范围读取 File/Blob

任务用 `blobLimits` 声明文件内容的逻辑大小上限，用 `budget.inputBytes` 为消息元数据预留空间。协议以附件形式传递 File/Blob，支持在 Worker 内按范围读取。引用计费规则见 [资源契约](resources.md#fileblob-附件)。

仓库包含可运行的 [Worker handlers](../examples/file-chunks/handlers.mjs)、[浏览器入口](../examples/file-chunks/worker.mjs) 与 [拉取客户端](../examples/file-chunks/read.mjs)。先执行 `pnpm build`，再执行 `pnpm dev`；在示例页浏览器控制台可运行：

```js
const { createWorkerRuntime, browserWorker } = await import('/dist/index.js');
const { readFileChunks } = await import('/examples/file-chunks/read.mjs');
const runtime = createWorkerRuntime({
  pools: {
    files: {
      size: 1,
      cacheBytes: 128,
      factory: browserWorker('/examples/file-chunks/worker.mjs'),
    },
  },
});
try {
  const file = new File([new Uint8Array(200_000)], 'example.bin');
  for await (const chunk of readFileChunks(runtime, file)) {
    console.log(chunk.byteLength); // 每次最多 64 KiB
  }
} finally {
  await runtime.dispose();
}
```

公共 iterateResults 可直接从 npm 包导入，示例使用它管理拉取和租约。示例每个文件创建一个 Session，用 setResource 保存文件引用与游标；每次只发出一个 next 任务。消费完当前块才释放租约并请求下一块，break、消费者异常和 AbortSignal 取消都会关闭 Scope。文件读取使用 Blob.slice().arrayBuffer()；取消会等待已开始的原生读取返回，再检查取消信号。检查通过后才推进游标并交付结果。

示例为游标和文件引用申报 128 字节费用；文件存储和 reader 的实际内存由应用另行评估。读取产生的 64 KiB buffer 属于该任务预留的输出空间；真实解码器的额外数组、WASM 内存和缓存要另计。主线程保留已消费块时，由接收方接管这些引用的预算和释放责任。

实现分块数据传输时：

- 使用 header、序号、总数和最终完整性校验确保数据完整。
- 同时限制块的字节数和对象数量；单条超大记录需拆分或明确拒绝。
- Worker 内源数据、中间状态、在途块与主线程累计结果分别计账。
- 消费者停止后通过 return 关闭迭代器；手工调用 next 时也应在 finally 中完成关闭。
- Session lost 后由业务新建 Session、重新打开文件或重新导入；有副作用的任务按业务恢复策略决定是否重试。

progress 用于可合并的状态快照，数据块通过带 ResultLease 的任务结果传递。

## 大型数值缓存

普通 cache 检查 TypedArray 自定义属性时会枚举索引；大型视图可能触发遍历上限。纯数值数据可以保存完整 ArrayBuffer，并在读取时创建视图：

```js
const values = new Float32Array(1_000_001);
ctx.cache.set('values', values.buffer, values.buffer.byteLength);
const buffer = ctx.cache.get('values');
if (buffer) {
  const cachedValues = new Float32Array(buffer);
  // cachedValues 与缓存共享 backing buffer，其所有权保留在 Worker。
}
```

部分视图仍按整个 backing buffer 计费；需要还原偏移或元素类型时，另存有限描述信息并计入元数据预算。普通条目可能被 LRU 淘汰，必须处理未命中。buffer 的附加数据需要计入条目预算。同一 backing store 宜集中保存在一个条目中，供多个视图复用；各条目独立计费。

WASM、数据库和 reader 实例使用 Session setResource，按业务上界申报并提供 disposer。其容量增长由业务限制，资源在 Session 内固定保存，直至显式清理或 Session 关闭。

## 常驻 Session 与共享 Runtime

Session 创建时是 unbound；第一次绑定 Worker 后持续独占该槽位，直至 Session 关闭或 Worker 失效。Pool size 设置该 Pool 的 Worker 数量上限，全局容量由 Runtime 协调。任务优先级用于安排可准入任务的执行顺序。

例如应用允许最多 8 个 Worker，可以将常驻任务 Pool size 设为 5，并在业务层限制绑定的 Session 总数最多 5，为短任务留下最多 3 个槽位的空间。任务准入同时要求满足 maxActiveTasks、全局 cacheBytes 等额度。

打开常驻资源前先取得业务准入名额，关闭并完成物理释放后才归还名额。达到上限时选择等待或显式关闭可重建的资源，保持既有 Session 的固定归属。

Runtime 由应用持有，各业务模块仅销毁自己的 Scope。Runtime 统计并限制其管理的 Worker；第三方自行创建的线程由应用统一纳入容量规划。

## 异步打包与大对象图

同步构造使用 prepare；异步打包使用 enqueuePrepared，在 prepareAsync 启动前取得准备窗口名额和任务额度。通过 preparationScratchBytes 声明准备临时内存，通过 budget.inputBytes 声明跨阶段保留的输入。计算密集的打包适合移入 Worker。

大型数据优先使用二进制编码，并为输入、输出两条路径分别声明预算和缓冲区所有权。classic Worker 需显式配置 `{ type: 'classic' }` 并将 host 打入 Worker 产物。

## 公共拉取接口

从 `@mapseekai/tasklane` 导入 `iterateResults`，配置 next、isDone 和 close。next 返回 TaskHandle，isDone 判断结束标记，close 定义本次消费的清理范围。自建 Scope/Session 时在 close 中 dispose；借用 Session 时关闭本次游标，Session 的所有权保留给调用方。详细示例见 [API](api.md#分块结果迭代)。

每次 next 在归还上一块租约后发起一个请求。return、dispose 和 AbortSignal 都结束当前租约；中止在 yield 暂停期间也会启动清理。调用方保留的数据引用随之由调用方计账。消费者应逐次 await next，使用 for await 可在 break 或异常时自动 return；手工拉取应在 finally 中 dispose。closed 提供物理请求完成和清理结果，清理失败会拒绝；业务失败与清理失败同时发生时通过 AggregateError 保留两者。
