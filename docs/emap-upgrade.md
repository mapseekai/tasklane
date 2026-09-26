# emap 资源运行时升级评估与迁移

本次以 Tasklane `0.1.0-beta.4` 和 emap 当前调用代码为基线，升级为 `0.2.0-beta.2`。保留原有 enqueue、enqueuePrepared、惰性 Session 和 ResultLease 契约，新增显式资源生命周期与 Session 准入。当前版本在缓存控制与资源上报的基础上新增数组按位置编码，协议为 v7，主线程包和 Worker bundle 必须一起更新。

## 对意见的判断

| 意见 | 核对结论与处理 |
| --- | --- |
| reservation 应依据物理可运行性及有效优先级 | 合理，但 beta.4 已修复并覆盖回归；沿用现有实现。 |
| 长期保留业务结果需要独立预算 | 合理。consumeResult/taskValue 释放的是任务输出额度，无法清除调用方引用；新增 resident ResourceLease。 |
| Session 容量应由 Runtime 统一准入 | 合理。acquireSession 联合检查 pool/global/cache 与可选 residentBytes；无需应用统计 endpoint terminate。 |
| 可重建副本可由 Runtime 回收 | 合理且必须显式 opt-in；仅无任务、无结果租约、无清理中的空闲 Session 可回收。 |
| Opaque cache resource 应能 resize | 合理。setResource 返回可 resize/release 的资源句柄，异步销毁失败保留额度。 |
| TypedArray 元素不应消耗 metadata maxEntries | 合理且已修复；通用路径仍检查附加属性。纯二进制缓存新增 setBinary，保存新原生视图，避免枚举原 view 的索引和附加字段。 |
| 多 key footprint affinity | 合理。普通池保留历史 locality；Session 组依据资源报告的实际 footprint，在已打开的等价 reader 副本间选择。 |
| transferOwnedBuffers 能强化所有权表达 | 新增兼容别名；无法证明其他 view/外部引用不存在，仍是调用方所有权承诺。 |
| structured streaming output | 整合 describe → take 两阶段分块 helper；在编码前预留所宣布的 outputBytes。 |
| blocked queue 索引、自动 cache/worker 扩缩容、memory pressure hook | 已实现按阻塞资源唤醒、显式 trim/pressure/resize 与可选自适应策略；扩缩容受硬上限和清理确认约束。 |
| 为 interactive 预留物理容量 | 已提供 Worker、active/preparing、结果租约和五类字节预算预留；仍不抢占正在执行任务或必需 Session，不提供延迟 SLA。 |

意见中的 COG 毫秒数、miss 数量和占位引用未在本次重跑验证，不能作为此次升级的性能结论。emap 的 `worker-capacity.ts`、Tasklane runtime adapter 中的 SessionCapacity、cache lease 和 live worker descriptor 是本次抽象的直接代码依据。

## 常驻结果与缓存额度

```ts
const runtime = createWorkerRuntime({
  pools,
  budgets: { residentBytes: 256 * 1024 ** 2 },
  maxResourceLeases: 4096,
});
const source = runtime.createScope('raster-source');
const allocation = source.resources.acquire({ kind: 'resident', bytes: initialBytes });
allocation.resize(nextBytes); // 精确、原子；不足时抛 BUDGET_EXCEEDED，旧额度不变
allocation.release();       // 幂等
```

`runtime.resources` 的租约由 Runtime 持有；`scope.resources` 的租约由所属 Scope 持有。`session.resources` 可为 reader/decoded cache 声明全局共享额度，随 Session 成功关闭或回收归还；关闭失败和仍未物理完成的任务保持额度。Session 丢失后仍需 dispose/Scope 清理以结束这类声明租约。Scope 关闭成功后归还其额度，并等待原有任务/子 Scope/Worker 释放屏障。默认 residentBytes 为 128 MiB。所有 Scope 与 Runtime 直接持有的租约共享预算和数量上限，零字节租约也计数。

这些是调用方声明的额度，不会自动释放 TypedArray、GPU 对象或检测引用。扩容应先申请再分配；缩容/释放应先销毁业务数据再归还额度。业务数据若比 Scope 活得更久，应使用更长生命周期的 owner。任务输出接管示例：

```ts
const result = await handle.result;
let allocation;
try {
  allocation = source.resources.acquire({ kind: 'resident', bytes: declaredResidentBytes });
  // cache.put 必须接管数据和 allocation，并在驱逐时释放 allocation。
  cache.put(key, result.value, allocation);
} catch (error) {
  allocation?.release();
  throw error;
} finally {
  result.release();
}
```

declaredResidentBytes 按业务驻留表示计算，不应盲目等同于协议包的 lease.byteLength。cacheBytes、residentBytes 和任务预算是各自独立的上限；不要将它们的简单相加当作真实 RSS。若用 resident lease 给 Worker 内部 decoded block cache 施加更紧的共享额度，该分配同时受 pool.cacheBytes 的完整 Worker 预留约束，这属于双重准入策略，不代表两份物理分配。

Worker 内部不透明资源可动态申报：

```ts
const resource = ctx.cache.setResource('reader', reader, initialBytes, r => r.close());
resource.resize(nextBytes); // 可驱逐普通 LRU 条目，不驱逐其他 pinned resource
await resource.release();  // 与 await ctx.cache.delete('reader') 一样可观察、重试销毁失败
```

resize 不改变 pool.cacheBytes 的固定上限和全局 Worker cache 预留。销毁期间禁止 resize；失败不返还额度；旧句柄不能修改或删除后来同名的新资源。Worker cacheUsedBytes 在任务响应/释放 ACK 时更新，不是对内部状态的实时采样。

## primary / replica 准入

```ts
const primary = await source.acquireSession('geotiff', {
  mode: 'wait', timeoutMs: 5000, signal, residentBytes: 64 * 1024 ** 2,
});
primary.resident!.resize(96 * 1024 ** 2); // 声明 residentBytes 后有租约；业务扩容前先申请额度
const replica = await source.acquireSession('geotiff', {
  mode: 'immediate', reclaimable: true, reclaimPriority: 10, residentBytes: 32 * 1024 ** 2,
});
```

- acquireSession 成功时握手已完成、Worker 已独占绑定，尚未执行任何业务任务。容量预留不依赖第一个 open 任务或 prepareAsync；老的 scope.session(pool) 保持惰性绑定。
- 可选 residentBytes 与 Worker 联合准入，通过 session.resident 读取、resize/release。等待 Worker 或 resident 额度期间不持有部分资源；启动期间已预留额度，取消/失败后在物理清理完成时归还。显式 0 也占用一个租约；省略时不创建租约。释放或缩减额度会唤醒等待请求。
- immediate 只使用当前可分配/可创建的槽位，不触发回收、不等待其他业务释放；仍等待新 Worker 的握手。不可用时抛 SessionAdmissionError（CAPACITY_UNAVAILABLE），带 pool/reasons。
- wait 默认使用 queueTimeoutMs，可用 timeoutMs/signal 约束等待与启动。Session 请求和任务共享 maxQueuedTasks 的队列数量限制。
- 只有必需 Session 的准入会回收可回收 Session；普通任务不会驱逐显式 Session。同池的必需请求优先于等待中的可回收副本；副本不会互相驱逐。
- 先回收其他池的普通空闲 Worker，再按 reclaimPriority 从小到大、最后按最近使用顺序选择空闲 Session。池已满时只回收该池。
- resident 额度或租约数量不足时，即使还有空闲 Worker 容量，也可回收持有对应资源的空闲 replica。resident 是全局预算，此类候选不限于请求所在池。immediate 和可选 replica 请求不会主动回收。
- Worker 上有执行/排队/准备中任务、未释放结果或待完成清理时不可回收。正在获得 Session 的调用也受保护。
- 回收等待异步 disposer 和物理终止确认。失败保留占用并发出 onDiagnostic；应用可调用原 Session/Scope.dispose 重试。终止失败使用 runtime.retryTermination。
- 终止失败的 Worker 保持隔离和占用，但不阻止回收其他健康空闲 Worker。
- factory 同步触发取消、Scope/Runtime 关闭时，清理会等待返回的 Worker；订阅或握手同步失败后，resident/cache 额度仍保留到物理终止完成。
- 被回收的句柄 state 为 closed、reclaimed 为 true，不自动重建业务 reader。emap 可在下一次需要副本时重新申请。

这可取代 emap 自行模拟的 SessionCapacity 和 live Worker 计数。Worker URL、reader open/close、业务副本数策略仍归 emap 管理。

## locality 与诊断

```ts
scope.enqueue('decode', {
  ...options,
  affinity: { keys: ['source-1/block-A', 'source-1/block-B'] },
});
const snapshot = runtime.diagnostics();
```

keys 在提交时复制并去重，每任务最多 128 个、每 key 最长 1024 字符。成功任务记录该 Scope/Pool 的历史位置；共享 key 可记住多个 Worker。选择空闲 Worker 时以命中历史 key 数量计分，同分采用原有最近使用顺序；不等待忙碌 Worker，也不让 locality 跨越 Scope、Pool 或任务优先级。历史条目总数受 maxAffinityEntries 约束，Worker 终止和 Scope 清理删除对应记录。缓存驱逐可能使历史过时。

GeoTIFF 的同源 primary/replica 可以组成 scope.sessionGroup，并通过 resource.report 上报 reader 缓存 key，由组选择空闲副本。业务仍负责 open/close 和成员等价性，Session 本身不会迁移；普通池 handler 必须能够在选中的 Worker 上重建资源。完整配置与边界见 [资源调度指南](resource-scheduling.md)。

`diagnostics()` 返回 limits、每池 Worker/Session/cache/等待数量，以及每个等待任务或 Session 的 reasons；它是只读快照，不预留未来容量。`stats` 新增 resourceLeases、sessionsReclaimed、reserved/peakReserved.residentBytes。`resourceDiagnostics()` 的 owners 提供各 Scope 的资源租约数和 residentBytes。

`stats.cacheStats` 与每池 `cacheStats` 提供累计 hits/misses/evictions，随任务成功、失败、取消响应和释放/控制 ACK 更新，已回收 Worker 的历史保留。evictions 仅计自动 LRU/trim 驱逐，不计替换、delete 或 owner 关闭；失败的缓存准入不会驱逐条目。这些指标不包含 reader 内部 decoded block cache；后者使用独立的 resourceCacheStats 与 resources footprint 上报。查询 reader 对象本身的命中不代表 block 命中。

## 可变大小分块

客户端从主入口导入 iterateSizedResults，Worker 从 host 入口导入 createSizedResultSource。Source 最多保存一份 chunk plan：describe 返回 done 或 token/outputBytes，take 验证 token 和预算后才 encode。plan 的驻留内存应计入 Session resource，dispose 释放 plan 自有临时状态，不能破坏交给消费者的结果缓冲区。

```ts
// Worker：注册 source.close 为 cache.setResource 的 disposer。
const source = createSizedResultSource({
  maxChunkBytes,
  plan: ctx => reader.planNext(ctx.signal),
});
// handler: describe => source.describe(ctx); take => source.take(token, ctx)

// Main：describeOptions 需覆盖小型 descriptor 的协议元数据。
const chunks = iterateSizedResults({
  session, task: 'take', maxChunkBytes,
  budget: { inputBytes: 1024, scratchBytes },
  describe: signal => session.enqueue('describe', { ...describeOptions, signal }),
  prepare: chunk => ({ payload: chunk.token }),
  close: () => session.dispose(),
});
for await (const chunk of chunks) await consumeChunk(chunk);
await chunks.closed;
```

outputBytes 是包含元数据的完整协议包上界。错误 token、超过 maxChunkBytes、编码结果超过声明及不足的已准入 output budget 都被拒绝；取消时先等物理任务结束再关闭游标。close 可重试，消费者 break 不预取下一块。Blob 附件不属于此 helper 的分块契约；需要 Blob 时继续使用普通任务的 blobLimits。

take 在 plan.dispose 之前编码一次元数据快照；Host 复用快照并重新检查字节上界，不再重复遍历应用对象图。二进制存储仍遵守原有共享/transfer 所有权规则，plan.dispose 不得破坏输出缓冲区。

## 迁移顺序与验证边界

先将 Tasklane 新包与 Worker bundle 一同更新；移除已被 beta.4 吸收的 reservation patch。用 acquireSession 替换主/副本容量准入，再将长期业务缓存交给明确 owner 的 ResourceLease。最后替换 descriptor/live worker 统计，按需要接入 sized chunks。保持 emap 的错误转换、AbortError、Scope 所有权和清理失败重试。

本仓库的 Node、真实浏览器 Worker 和安装包检查验证通用 API；不等于 emap 已完成依赖升级或真实 COG 延迟/命中率已改善。emap 迁移后仍需覆盖多源并发、视口取消、关闭数据源、副本重建和 GeoPackage 游标回收。

## 升级完成状态

| 能力 | 当前边界 |
| --- | --- |
| 多 Session footprint 选路 | 已实现 SessionGroup 与 reader 资源快照上报，支持忙碌回退和失效成员排除。 |
| interactive 预留容量 | 已实现全局/池 Worker、active/preparing、结果租约和字节预算隔离。 |
| blocked queue 索引 | 已实现按资源登记与事件唤醒，保留公平性、FIFO 和老化。 |
| memory-pressure shrink | 已实现 resizePool、trim、setMemoryPressure 和资源 trim 回调，按 ACK 归还额度。 |
| adaptive cache/Worker sizing | 已实现可选采样控制、增长上限与空闲滞后缩容，压力期间暂停。 |
| 更细粒度遥测 | 已实现 reader 内部计数/footprint、逐池回收尝试/成功/失败/原因及调度索引计数。 |
| emap 落地迁移 | 按本次要求未修改。仍需更新依赖与 Worker bundle、移除旧 patch/容量实现、接入报告/trim，并验证真实 COG 与 GeoPackage。 |

以上六项能力的 API、默认值、失败处理和适用边界见 [资源调度指南](resource-scheduling.md)。
