# 资源调度、缓存反馈与内存压力

这些能力属于 Tasklane `0.2.0-beta.1`。Worker 协议升级为 **v6**，主线程包和 Worker bundle 必须一起更新。默认不启用交互资源预留和自适应扩缩容；原有 enqueue、惰性 Session 和 ResultLease API 保持可用。

## Session 组与真实 footprint

```ts
const primary = await scope.acquireSession('raster');
const replica = await scope.acquireSession('raster', { reclaimable: true });
// 应用先分别执行 open，确保两个 reader 对应同一业务源。
const readers = scope.sessionGroup([primary, replica]);
const handle = readers.enqueue('decode', {
  budget: { inputBytes: 4096, scratchBytes: 1024 ** 2, outputBytes: 1024 ** 2 },
  affinity: { keys: ['source/level/block-1', 'source/level/block-2'] },
  prepare: () => ({ payload: request }),
});
```

Session 组最多包含 128 个已经绑定、属于同一 Runtime/Scope/Pool、准入优先级相同的 Session。组借用这些成员，支持 enqueue/enqueuePrepared，不拥有其生命周期，也不会自动打开或复制业务 reader。普通 Session 的任务仍严格绑定原 Worker。

选择时只考虑空闲、未清理或缩容的成员，按当前上报 footprint 与请求 key 的交集数量排序，同分使用最久未使用的 Worker。命中最多的成员忙碌时立即选择其他可用成员；成员关闭或丢失后选择存活成员，全部失效时返回 SESSION_LOST。待执行的组任务会保护其候选副本，防止它们被当作空闲副本回收。

Worker 内部通过不透明资源租约报告 reader 自有缓存：

```ts
const resource = ctx.cache.setResource('reader', reader, reservedBytes, r => r.close(), {
  trim: async targetBytes => {
    await reader.trimCache(targetBytes);
    resource.report(reader.cacheReport());
    return reader.reservedBytes; // 清理之后仍需保留的额度
  },
});
resource.report({
  hits: 120, misses: 8, evictions: 3,
  usedBytes: 512 * 1024,
  keys: ['source/level/block-1', 'source/level/block-2'],
});
```

report 是完整替换快照，计数是从该资源创建起累计、单调不减的安全整数；usedBytes 不得超过资源已申请额度。每资源最多 128 个 key，每 key 最长 1024 字符，每 Worker 最多上报 64 个资源、共 1024 个 key。上报数据在任务终态、Scope 释放 ACK 和缓存控制 ACK 时发送，不是持续轮询或对后台变动的实时观测。trim 后未提供新快照时清空旧 key；关闭/终止会删除对应驻留记录。

普通 Scope 任务的 affinity 仍是历史执行位置偏好；只有显式 Session 组采用上述 reader footprint。应用负责 source/version/block key 的语义以及成员资源等价性。

## 交互任务的资源预留

```ts
const runtime = createWorkerRuntime({
  pools: {
    raster: { factory, size: 4, cacheBytes: 16 * 1024 ** 2, interactiveWorkers: 1 },
  },
  maxWorkers: 4, maxActiveTasks: 4, maxPreparingTasks: 2, maxResultLeases: 16,
  interactiveReserve: {
    workers: 1, activeTasks: 1, preparingTasks: 1, resultLeases: 2,
    budgets: { outputBytes: 16 * 1024 ** 2, residentBytes: 32 * 1024 ** 2 },
  },
});
const session = await scope.acquireSession('raster', { priority: 'interactive' });
```

所有预留默认 0。foreground/background 合计只能使用总上限减去预留的额度；interactive 可使用全部总额，但不能超出总上限。预算支持 inputBytes、scratchBytes、outputBytes、cacheBytes、residentBytes。池内 interactiveWorkers 与全局 workers 分别生效。预留执行槽位不会自动创建或预留 Worker，因此需要物理隔离时应同时配置 Worker 和适当的缓存额度。

Session 准入和 resources.acquire 可指定 priority，默认 foreground；Session 内的任务和资源默认继承其优先级。启用 Worker/cache 隔离时，受保护的 interactive Session 不接受降为非 interactive 的任务。任务老化只改变排队顺序，不改变其资源类别。显式预留大于总量会被拒绝；任务自身超过所属类别字节上限会立即抛 BUDGET_EXCEEDED。

预留不抢占正在执行的算法、不驱逐必需 Session，也不承诺交互延迟 SLA。若全部容量被配置为 interactive，非交互任务可持续等待到其超时。持有的输出租约继续占用所属类别额度，直到 release。

## 阻塞队列的定向唤醒

调度器为被阻塞的通道头部登记 pool、Worker、预算、执行槽位、准备窗口、结果租约、预算 reservation 等依赖；对应事件发生后才重新参与选择。新增同通道队尾任务不会反复检查仍然阻塞的头部。取消头部、准备完成和优先级老化会更新索引，其他通道继续保持公平性与 FIFO 语义。

`stats.scheduler` 暴露 blockedBuckets、累计 eligibilityChecks 和 wakeups。初次登记仍需检查候选；共享预算或执行槽位变化也会唤醒相关等待者。Session 准入队列仍按必需性和优先级检查，不能将这项优化理解为所有运行时操作都具有常数复杂度。

`pnpm benchmark:blocked` 使用真实 Node Worker 比较不同数量的被占满池通道与同样数量的独立池任务，打印候选检查增量和耗时；检查计数验证索引行为，耗时依赖本机环境。

## 手动扩缩容与内存压力

```ts
const resized = await runtime.resizePool('raster', { size: 2, cacheBytes: 8 * 1024 ** 2 });
const pressured = await runtime.setMemoryPressure('moderate');
await runtime.setMemoryPressure('critical');
await runtime.setMemoryPressure('normal');
const trimmed = await runtime.trim({
  pool: 'raster', cacheBytesPerWorker: 0, workersPerPool: 1, reclaimSessions: true,
});
```

- size/cacheBytes 是可调准入目标，构造时的 pool.size/cacheBytes 是硬上限；size 至少为 1 和池 interactiveWorkers。降低 size 不终止忙碌 Worker或必需 Session，因此实际 Worker 数可能暂时高于目标。
- moderate 将进入压力前的容量约减半、缓存额度减半；critical 将准入容量降到池预留允许的最小值、缓存目标降到 0，并尝试回收全部空闲普通 Worker 和可回收 Session。normal 恢复进入压力前保存的目标，不预热 Worker，也不重建已关闭 reader。
- trim 默认作用于全部池，cacheBytesPerWorker/workersPerPool 默认 0，reclaimSessions 默认 true；指定 false 保留所有 Session。trim 调低未来准入目标，但不暂停后续任务。压力期间的临时 resize/trim 不覆盖 normal 要恢复的原目标。
- 回收遵守任务、结果租约、Session 准入与物理清理屏障。优先普通空闲 Worker，再按 reclaimPriority/最近使用时间选择允许回收的 Session。
- 每次实际回收前重新确认候选仍然空闲；等待其他副本清理期间接到任务或持有结果的候选会被跳过。
- cache 缩容先驱逐普通 LRU，再调用显式注册的资源 trim。回调应先销毁数据，再返回不大于原值的剩余额度；不要在回调中另行 resize。未注册回调的 pinned 资源保留。存活 pinned 状态超过目标时该 Worker 缩容失败，原 cache 预留保留。
- 缩容收到 Host ACK 才归还全局 cache 额度；扩容先预留差额再发送控制消息。活跃任务结束前不会启动控制 ACK 超时。清理开始后未在 releaseTimeoutMs 内确认会隔离并终止该 Worker；物理终止失败仍保留全部占用。
- 每次调用返回 MaintenanceReport：成功回收的 Worker 数、实际归还的 cache 额度与逐 Worker failures。部分失败不回滚已经完成的清理，目标与实际占用可通过 diagnostics 对照。并发维护调用被拒绝，应 await 前一次；Runtime 关闭后不接受新调用。

压力级别由应用提供。Tasklane 不猜测浏览器可用内存，也不会扫描或释放应用未申报的主线程对象、GPU 内存。

## 可选自适应控制

```ts
const pool = {
  factory, size: 8, cacheBytes: 32 * 1024 ** 2,
  adaptive: {
    minWorkers: 2, minCacheBytes: 4 * 1024 ** 2,
    sampleMs: 1000, idleMs: 30000, missRatio: 0.2,
  },
};
```

只在配置 adaptive 时启用。初始准入目标为下限，不预热。默认 minWorkers 是 max(1, interactiveWorkers)，minCacheBytes 为硬上限的四分之一，sampleMs 为 1000，idleMs 为 30000，missRatio 为 0.2。

每池按 sampleMs 采样：队列持续有等待时逐步增加一个 Worker 准入额度；有 miss 且发生驱逐或容量已满、miss 比例达到阈值时，cache 目标增加约 50%，不超过硬上限；空闲达到 idleMs 后回到下限，并依照同样的安全条件回收资源。反馈使用普通 CacheStore 与 reader 自报计数；缺少有效上报时无法判断 reader 的内部缓存收益。多池分别遵守自己的采样周期。

同一轮中各池独立发起调整。自动缓存调整遇到忙碌或启动中的 Worker 会延后，待其空闲后再补齐到当前目标，避免一个长业务任务拖住其他池的采样。目标与实际 cacheReservedBytes 在此期间可以不同；显式 resize/pressure 仍等待相关清理完成。

该策略是有边界的启发式控制，不是最优配置计算。每次维护完成后才进行下一轮。非 normal 压力级别暂停自动调整，手动维护期间也不会启动重叠操作。失败通过 onDiagnostic 暴露，仍持有的资源可从诊断中查看。

## 遥测与失败诊断

- `stats.cacheStats` / 每池 cacheStats：普通 CacheStore 操作的 hits/misses/evictions。
- `stats.resourceCacheStats` / 每池 resourceCacheStats：reader 自报的内部缓存累计值，与查询 reader 对象本身的 CacheStore 命中分开。
- 每池 `resources`：资源 ID、Scope/Session、resource 名、reservedBytes、usedBytes、keys 和计数快照。
- `stats.reclaim` / 每池 reclaim：资源准入与维护回收的 attempts/succeeded/failed，byReason 区分 capacity、resident、pressure、resize、adaptive。普通 dispose、空闲过期和失败后应用显式重试物理终止不重复计入；全部物理终止另见 stats.workerTerminations。
- 每池 capacity/cacheBytesPerWorker 表示目标，maxCapacity/maxCacheBytesPerWorker 表示硬上限，workers/cacheReservedBytes 表示实际占用。
- 等待原因增加 interactive-reserve、maintenance；Runtime 诊断带 memoryPressure。

计数在 Worker 更换后保留，资源 footprint 随资源销毁删除。diagnostics 返回独立快照。资源数、key 数和输入长度均有上限，终止后不会继续累积历史 footprint。

本仓库验证这些通用契约。emap 依赖升级、reader 上报/trim 回调接入、业务副本策略和真实 COG/GeoPackage 性能验证仍由单独的 emap 迁移完成。

## 本次验证记录

2026-09-26：完整 verify 通过，包括 214 项 Node 测试、Chrome/Firefox/WebKit 合计 63 项真实 Worker 用例、类型/格式/lint 与离线安装 tarball 检查；另有 4 项大数据/取消压力测试通过。新增回归覆盖控制超时、终止失败保留额度、活跃任务与维护屏障、保护额度和副本失效回退。

时间相关的 Node 回归使用可控时钟，验证多池独立采样、40 轮压力切换，以及 Session/Scope/Runtime 与缓存维护交错关闭。测试复现并修复了异步清理后的回收候选过期、忙碌 Worker 阻塞其他池自适应两处问题。

浏览器持续用例默认至少运行 12 秒并执行至少 24 轮突发负载，包含任务取消、缓存反馈、moderate/critical/normal 切换、空闲收缩和最终额度归零。通过环境变量可延长到最多一小时：

```sh
TASKLANE_SOAK_MS=60000 pnpm exec playwright test test/browser/maintenance.spec.mjs
```

另外每个浏览器验证 12 次在 reader trim 开始后关闭 Session/Scope/Runtime 的清理屏障。该测试验证资源契约与有界持续运行，不等于生产环境长期 RSS 测量或实际 GIS 延迟评估。

另行执行每个浏览器至少一分钟的持续测试，全部通过。合计 1367 轮负载、15856 个成功结果、548 次取消与 457 次压力循环；各浏览器结束时 Worker 数、任务/结果/资源租约与预留字节均归零。

| 浏览器 | 持续负载轮数 | 成功结果 | 取消 | 压力循环 |
| --- | ---: | ---: | ---: | ---: |
| Chrome | 455 | 5278 | 182 | 152 |
| Firefox | 430 | 4988 | 172 | 144 |
| WebKit | 482 | 5590 | 194 | 161 |

本机 blocked 基准：100、1000、4000 个阻塞通道下，独立池各执行 100 个任务，候选检查增量均为 100；本次耗时分别为 8.59、7.51、9.99 ms。这说明该场景没有随阻塞通道数反复全量检查，不代表 emap 的端到端性能结果。
