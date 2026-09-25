# 资源与调度契约

## 额度和数据边界

`inputBytes` 和 `outputBytes` 按协议包大小预留和检查，字符串、数字、普通数组与对象图不再免费。根标量按 UTF-16 字符串字节、8 字节数字/64 位 BigInt、1 字节布尔值计费，null/undefined 为 0；根 buffer/view 按完整 backing store 计费。复合数据编码为有界的扁平图元数据和独立 buffer 列表，费用为元数据字符串 UTF-16 字节加唯一 backing store 字节。`packetByteLength(value)` 可以计算准确申报量；生成大型输入前应先声明安全上限，避免为了估算提前分配整个输入。

发送方在 postMessage 前编码并检查额度。Runtime 接收结果只检查封装和字节数，不再遍历应用对象图；`lease.value` 首次访问时解码，后续复用同一值。没有访问 value 就 release/discard 的结果不会解码。非法图在消费时抛错并自动释放租约。结果 byteLength 表示传输计费量，不表示解码后的真实 JS 堆大小。

图编码限制为 100,000 个对象、1,000,000 条遍历边、4096 个 backing store，以及 64 MiB UTF-16 元数据。数组长度、Map/Set 条目数在遍历前检查；每次加入对象或属性都检查工作量，不创建整层 Object.keys/工作栈。访问器、自定义实例和 Proxy 不属于支持的数据模型。原生枚举和字符串编码仍可能产生引擎内部临时分配；这不是分配沙箱。

`binaryByteLength(value, limits)` 仍是单独的二进制工具，不用于任务预算；`dataByteLength(value, limits)` 统计普通驻留数据的 backing store、字符串、数字与属性名，另为每个非二进制对象收取 16 字节结构费用，普通 cache 使用后者检查低报。

`scratchBytes` 是准入声明。Worker 可通过 `ctx.scratch.allocate(bytes)` 分配受度量的 ArrayBuffer，超过任务 scratch 上限会拒绝。`ctx.scratch.release(buffer)` 会 detach 所有别名；任务结束时 arena 自动关闭。不得将仍需保存的 arena 内存留给缓存；应创建独立常驻资源。普通 new、JSON.parse、WASM/native/GPU 内存与 Worker 固定开销不受 arena 监控，算法必须自行约束。额度不代表 JS 堆或进程 RSS 硬上限。

Worker 必须运行可信代码。直接绕过 serve 的 endpoint 可以在任何校验前发送巨型消息，Web Worker/worker_threads 通道无法阻止接收侧反序列化；协议或 ACK 都不能把同进程 Worker 变成内存沙箱。不可信算法应部署在带操作系统资源限制的独立进程/服务中。

大型 GIS 数据宜使用 TypedArray 和有限字段元数据，并按结果消费速度分块提交。

## 调度

默认 `maxActiveTasks = min(pool capacity, maxWorkers)`，包括启动、准备和执行。等待队列默认最多 1024 项；生产者使用有限提交窗口可以控制业务侧 Promise 和闭包数量。

调度器按优先级、Scope/group 的服务历史选择任务，同一优先级、组、Pool/Session 通道内保持 FIFO。不同 Pool/Session 的阻塞头部互不遮挡。默认 `priorityPolicy: 'strict'`，等待再久的 background 也不会越过可准入的 interactive。显式设置 `priorityPolicy: 'ageing'` 后，每 `ageingMs` 提升一级，后台最终可与交互任务同级。两种策略都不会抢占正在执行的任务；严格优先级可能使低优先级任务长期等待。空闲组历史最多保留 4096 项，新组以当前服务时钟初始化，流式补充不会重置为绝对最高优势。

预算不足且等待达到 `budgetWaitMs`（默认 1000 ms）的可运行候选会阻止继续消耗其短缺额度的任务插队。其他额度上的工作仍可执行。等待用户释放结果、不可用 Session 或不可抢占的算法仍可能使任务超时。

## 结果与 Scope

`settled` 只表示任务的物理生命周期结束；成功结果由 `ResultLease` 持有。消费时使用 `consumeResult(handle, async value => ...)`，无论消费成功或抛错都会归还额度。调用者另存的引用仍由调用者负责。

无需结果的任务可以设置 `discardResult: true`。成功后立即释放值和输出额度，`result` 解析为已释放租约，`settled` 仍可用于等待完成；需要获知失败时应观察 `result`。

`maxResultLeases` 默认 1024，约束保留租约加已准入工作，零二进制字节结果也受数量限制。显式丢弃结果的任务无需额外租约容量。

`runtime.withScope(label, async scope => ...)` 在回调结束后销毁 Scope。`maxScopes` 默认 4096，包含子 Scope，达到上限时创建操作以 `BUDGET_EXCEEDED` 失败；销毁成功后返还名额。`stats.scopes`、`stats.leases` 和 `resourceDiagnostics()` 可用于定位未关闭 Scope、未消费结果、仍在 prepare 的任务和隔离 Worker。

`scope.dispose()` 等待任务、子 Scope、Session 以及对应 Worker 的释放确认。释放 ACK 按 Scope ID 和 Worker epoch 匹配，默认 `releaseTimeoutMs = 10000`；缺失 ACK 或资源清理失败会拒绝，调用方可以再次尝试 Scope 销毁。确认物理 Worker 已退出也会完成该实例的释放屏障。

`runtime.disposeWithin(ms)` / `scope.disposeWithin(ms)` 只限制调用者等待时间；后台清理继续运行。超时不能证明内存已释放。`prepare` 只允许同步、短小的输入构造；返回 Promise/thenable 会立即以 INVALID_ARGUMENT 拒绝并归还准入额度，不等待它完成。异步加载和昂贵计算必须放到 Worker handler 内。运行时不能撤销违规 callback 已启动的异步副作用，也不能抢占主线程同步死循环。同步构造返回后还会检查执行截止时间。

物理终止失败会保留槽位和额度，`stats.quarantinedWorkers` 可观察该状态。外部故障解除后调用 `runtime.retryTermination()`，确认终止成功后才清理；随后可以再次调用 `runtime.dispose()`。

## Worker 本地资源

`cache.set` / `setPinned` 保存可检查的数据，显式 bytes 必须覆盖 dataByteLength 的二进制和元数据计费量。保存后不能通过外部引用扩大条目而不重新申报。驻留 Map/Set、Date/RegExp 和 buffer/view 的附加可枚举字段也会检查；检查 view 附加属性需要枚举其索引，索引数量受条目限制；因此大 view 更适合无附加字段的任务结果或显式常驻资源。

Session 可用 `cache.setResource(key, value, bytes, disposer)` 保存数据库类实例、WASM 实例等不透明资源。此路径信任声明的 bytes，资源固定在 Session Worker 内，不能被 LRU 淘汰。

```ts
ctx.cache.setResource('db', database, estimatedBytes, async db => {
  await db.close();
});
await ctx.cache.delete('db');
```

资源销毁进行中仍占额度，清理失败保留条目以便重试。替换资源前必须 delete 并等待完成。Scope 释放 ACK 在资源 disposer 完成后发送；空闲 Session/Runtime 的正常关闭也等待清理。正在执行的非协作任务、硬取消和异常终止可以直接终止 Worker，应用不能依赖这些路径执行异步外部资源清理；外部事务必须具备自身恢复策略。

## Progress 与取消

progress 使用普通对象、数组、有限数字、字符串、布尔值和空值作为小型控制消息：最多 64 个对象、256 条遍历边、4 KiB 元数据计费量，不允许二进制数据。Host 每个任务最多一条在途 progress，等待 ACK 时只保留最新快照；发送间隔至少 16 ms。数据块和预览图使用有结果额度的任务传输。

任务结束后 context.progress 不再发送，checkpoint 拒绝。checkpoint 让出真实事件循环以接收取消消息，在 Node 使用 setImmediate，浏览器优先 scheduler.yield，回退 MessageChannel。

逻辑取消与物理阶段独立：启动期取消仍受 execution deadline 约束；已经发送的 Worker 任务到期会强制终止；prepare 采用同步构造契约。

协议版本为 3，payload/result 使用 Packet 封装，request 携带 maxScratchBytes。Runtime 和 host 应使用同一构建版本，自定义 endpoint 必须支持 progress ACK 与 Scope release ACK。底层传输和异步 disposer 的失败不会被当作成功释放。
