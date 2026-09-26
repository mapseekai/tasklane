# 资源与调度契约

## 额度和数据边界

`inputBytes` 和 `outputBytes` 按协议包大小预留和检查，计费覆盖字符串、数字、普通数组与对象图。根标量按 UTF-16 字符串字节、8 字节数字/64 位 BigInt、1 字节布尔值计费，null/undefined 为 0；根 buffer/view 按完整 backing store 计费。复合数据编码为有界的扁平图元数据、独立 buffer 列表和 File/Blob 附件表；费用为元数据字符串 UTF-16 字节、唯一 backing store 字节及附件引用费用。`packetByteLength(value)` 可以计算准确申报量；生成大型输入前应先声明安全上限，避免为了估算提前分配整个输入。

发送方在 postMessage 前编码并检查额度。Runtime 接收结果时检查封装和字节数；`lease.value` 首次访问时解码，后续复用同一值。结果可直接 release/discard，解码按 value 的访问需求进行。非法图在消费时抛错并自动释放租约。结果 byteLength 表示传输计费量；解码后的实际 JS 堆占用由应用结合运行环境测量。

图编码限制为 100,000 个对象、1,000,000 条遍历边、4096 个 backing store、256 个 File/Blob 附件，以及 64 MiB 元数据与附件引用费用。数组长度、Map/Set 条目数在遍历前检查；对象和属性逐项遍历并检查工作量。消息支持标量、普通数据属性、数组、Map/Set、Date/RegExp、二进制 buffer/view 及 File/Blob 附件；自定义资源实例通过 Session setResource 管理。原生枚举和字符串编码产生的引擎临时内存由应用纳入实际内存评估。

`binaryByteLength(value, limits)` 统计二进制 backing store；任务预算采用 `packetByteLength(value)` 的协议计费规则。`dataByteLength(value, limits)` 统计普通驻留数据的 backing store、字符串、数字与属性名，另为每个非二进制对象收取 16 字节结构费用，普通 cache 使用后者检查低报。

`scratchBytes` 是准入声明。Worker 可通过 `ctx.scratch.allocate(bytes)` 分配受度量的 ArrayBuffer，超过任务 scratch 上限会拒绝。`ctx.scratch.release(buffer)` 会 detach 所有别名；任务结束时 arena 自动关闭。跨任务保存的数据应分配为独立常驻资源。普通 new、JSON.parse、WASM/native/GPU 内存与 Worker 固定开销由算法管理；实际 JS 堆和进程 RSS 应结合运行环境测量。

Runtime 面向可信 Worker。使用 serve 在发送前校验协议消息和额度；自定义 endpoint 需自行遵守相同的数据边界，接收侧反序列化发生在 Runtime 校验之前。运行不可信算法时，应使用带操作系统资源限制的独立进程或服务。

大型 GIS 数据宜使用 TypedArray 和有限字段元数据，并按结果消费速度分块提交。

## 调度

等待预算的大任务在具备物理槽位准入条件时获得防饥饿保护；槽位条件变化后重新评估保留。预算保留约束同级和更低有效优先级任务，更高优先级任务仍可使用可用额度。ageing 模式按提升后的优先级比较。

默认 `maxActiveTasks = min(pool capacity, maxWorkers)`，包括 Worker 启动、同步 prepare 和执行。等待队列默认最多 1024 项；生产者使用有限提交窗口可以控制业务侧 Promise 和闭包数量。

调度器按优先级、Scope/group 的服务历史选择任务，同一优先级、组、Pool/Session 通道内保持 FIFO。不同 Pool/Session 的阻塞头部互不遮挡。默认 `priorityPolicy: 'strict'`，可准入的 interactive 始终优先于 background。显式设置 `priorityPolicy: 'ageing'` 后，每 `ageingMs` 提升一级，后台最终可与交互任务同级。两种策略均在任务准入时决定顺序，已执行任务持续运行至完成或取消；严格优先级下低优先级任务可能长期等待。空闲组历史最多保留 4096 项，新组以当前服务时钟初始化，流式补充沿用服务历史参与公平调度。

预算不足且等待达到 `budgetWaitMs`（默认 1000 ms）的可运行候选会阻止继续消耗其短缺额度的任务插队。其他额度上的工作仍可执行。等待用户释放结果、不可用 Session 或不可抢占的算法仍可能使任务超时。

## 结果与 Scope

`settled` 在任务物理生命周期结束后兑现；成功结果由 `ResultLease` 持有。消费时使用 `consumeResult(handle, async value => ...)`，无论消费成功或抛错都会归还额度。调用者另存的引用仍由调用者负责。

无需结果的任务可以设置 `discardResult: true`。成功后立即释放值和输出额度，`result` 解析为已释放租约，`settled` 仍可用于等待完成；需要获知失败时应观察 `result`。

`maxResultLeases` 默认 1024，约束保留租约加已准入工作，零二进制字节结果也受数量限制。显式丢弃结果的任务无需额外租约容量。

`runtime.withScope(label, async scope => ...)` 在回调结束后销毁 Scope。`maxScopes` 默认 4096，包含子 Scope，达到上限时创建操作以 `BUDGET_EXCEEDED` 失败；销毁成功后返还名额。`stats.scopes`、`stats.leases` 和 `resourceDiagnostics()` 可用于定位未关闭 Scope、未消费结果、仍在 prepare 的任务和隔离 Worker。

`scope.dispose()` 等待任务、子 Scope、Session 以及对应 Worker 的释放确认。释放 ACK 按 Scope ID 和 Worker epoch 匹配，默认 `releaseTimeoutMs = 10000`；缺失 ACK 或资源清理失败会拒绝，调用方可以再次尝试 Scope 销毁。确认物理 Worker 已退出也会完成该实例的释放屏障。

`runtime.disposeWithin(ms)` / `scope.disposeWithin(ms)` 只限制调用者等待时间；后台清理继续运行。清理完成以 dispose 的最终结果或物理终止确认为准。`prepare` 用于同步、短小的输入构造；返回 Promise/thenable 会立即以 INVALID_ARGUMENT 拒绝并归还准入额度。异步输入准备使用 enqueuePrepared 的 prepareAsync；计算密集工作适合放在 Worker handler 内。回调自行负责已启动的异步副作用；主线程同步代码需及时返回以保持事件循环响应。同步构造返回后还会检查执行截止时间。

物理终止失败会保留槽位和额度，`stats.quarantinedWorkers` 可观察该状态。外部故障解除后调用 `runtime.retryTermination()`，确认终止成功后才清理；随后可以再次调用 `runtime.dispose()`。

## Worker 本地资源

`cache.set` / `setPinned` 保存可检查的数据，显式 bytes 必须覆盖 dataByteLength 的二进制和元数据计费量。通过外部引用扩大条目时需要重新申报。驻留 Map/Set、Date/RegExp 和 buffer/view 的附加可枚举字段也会检查；检查 view 附加属性需要枚举其索引，索引数量受条目限制；因此大 view 更适合无附加字段的任务结果或显式常驻资源。

Session 可用 `cache.setResource(key, value, bytes, disposer)` 保存数据库类实例、WASM 实例等不透明资源。此路径信任声明的 bytes，资源固定在 Session Worker 内，直至显式清理或 Session 关闭。

```ts
ctx.cache.setResource('db', database, estimatedBytes, async db => {
  await db.close();
});
await ctx.cache.delete('db');
```

资源销毁进行中仍占额度，清理失败保留条目以便重试。替换资源前必须 delete 并等待完成。Scope 释放 ACK 在资源 disposer 完成后发送；空闲 Session/Runtime 的正常关闭也等待清理。正在执行的非协作任务、硬取消和异常终止可以直接终止 Worker，这些路径的外部资源清理由应用恢复策略负责，外部事务应具备恢复机制。

## Progress 与取消

progress 使用普通对象、数组、有限数字、字符串、布尔值和空值作为小型控制消息：最多 64 个对象、256 条遍历边、4 KiB 元数据计费量。Host 每个任务最多一条在途 progress，等待 ACK 时只保留最新快照；发送间隔至少 16 ms。数据块和预览图使用有结果额度的任务传输。

context.progress 的发送生命周期随任务结束而关闭，结束后的 checkpoint 调用会被拒绝。checkpoint 让出真实事件循环以接收取消消息，在 Node 使用 setImmediate，浏览器优先 scheduler.yield，回退 MessageChannel。

逻辑取消与物理阶段独立：启动期取消仍受 execution deadline 约束；已经发送的 Worker 任务到期会强制终止；prepare 采用同步构造契约。

协议版本为 5，payload/result 使用 Packet 封装，request 携带 maxScratchBytes。自定义 endpoint 必须支持 progress ACK 与 Scope release ACK。底层传输和异步 disposer 的失败会传递给释放调用方，供其处理或重试。

## File/Blob 附件

File/Blob 使用独立附件表；每个消息最多 256 个不同对象，重复引用按对象身份计一次，不同切片对象分别计费。`blobLimits.inputBytes/outputBytes` 默认 0，分别限制一个输入/输出消息中附件的逻辑大小总和，必须为非负安全整数。这些限制用于每包准入校验；同时保留的文件数量、逻辑大小总和及实际内存由应用管理。

`packetByteLength` 与 inputBytes/outputBytes 计入图元数据、每附件 64 字节引用费用、MIME 字符串和二进制 backing store，附件内容的逻辑字节数由 blobLimits 单独校验。文件名与 lastModified 保存在图元数据中。编码通过附件引用保留文件内容；实际存储和克隆成本取决于浏览器或 Node 的实现。内存构造的 Blob 原有存储仍由应用负责。

File 的 name/type/lastModified 和重复引用得到保留；Blob/File 按原生文件属性传递，额外业务字段应放在外层普通对象中；附件通过克隆传递，transfer list 用于可转移对象。Node 通道通过 Blob 附件和显式 File 元数据恢复 File。File 解码要求运行环境提供 File 构造器。文件数据通过任务输入或结果传递；常驻文件引用用 Session setResource，声明引用/reader 开销并单独管理逻辑文件大小。

读取后产生的 ArrayBuffer、解码临时内存和跨任务缓存仍需各自申报。ResultLease.release 清除租约持有的引用并返还消息额度；调用方另存的 File/Blob 由其持有者负责释放引用，底层存储由运行环境回收。完整示例见 [文件与分块使用指南](file-and-session.md)。

## 异步准备阶段

`enqueuePrepared` 在调用 prepareAsync 前，原子预留任务 inputBytes、outputBytes 和 `max(preparationScratchBytes, budget.scratchBytes)`，并预占结果租约名额。该完整额度持续持有至物理任务结束，输出部分随后交给 ResultLease。准备临时数据在回调完成前释放，跨阶段保留的数据计入 inputBytes。此方式为准备到执行提供连续额度，业务使用声明的上界约束自身分配。

`maxPreparingTasks` 默认 2，合计约束正在生产和已准备、等待 Worker 的输入。Worker 在执行准入时绑定。准备和等待发送仍计入 stats.queued 与 maxQueuedTasks；同一优先级、Scope/group、Pool/Session 通道按 FIFO 准入，其他通道可独立准备。

queueTimeoutMs 约束首次准入前的等待；executionTimeoutMs 从准备准入开始覆盖生产、等待 Worker、启动和执行；preparationTimeoutMs 单独约束生产回调，默认采用 executionTimeoutMs。取消/超时立即拒绝 result，回调物理结束后释放准备资源；迟到输入保持原缓冲区所有权。回调应通过 signal 协作退出并完成其临时资源清理。持续未完成的回调保持额度与清理屏障，可通过 disposeWithin 和 resourceDiagnostics 观察。

stats.preparing / prepared 分别统计生产中和等待发送的输入；preparationReserved 展示这些输入持有的完整任务额度，已包含在 reserved 中。resourceDiagnostics 的 preparing / prepared 提供对应任务 ID。settled 在生产或 Worker 的物理生命周期结束后兑现。

## 迭代器清理重试

iterateResults 的 close 失败通过首次 closed 和触发清理的调用传播。retryCleanup() 显式再次调用 close，等待已提交任务 settled 后执行；并发重试共用同一次尝试。迭代停止后保持结束，业务任务和已消费块保持原状态。成功后的 dispose() 兑现；首次 closed 的失败记录保持可观察，重试完成以 retryCleanup() 为准。close 使用 Scope/Session 的释放接口时，常驻缓存与物理 Worker 额度仍由底层生命周期持有至释放确认。块租约在终止消费时释放，应用保留的块引用由应用管理。

## 业务错误

远端失败以 RuntimeError 传递，code 表示 Runtime 错误类别；remoteError 保存业务 name、字符串 code、message 及可选 stack/details。基本字段从数据属性读取；name/code 最多 128 字符、message 1024 字符、stack 4096 字符，截断时标记 truncated。details 接受有限数字、字符串、布尔值、null、普通对象和数组，最多 8 层、128 次计费访问和 4 KiB 计费量；编码超限或类型不符时标记 detailsOmitted，并保留基本错误。整个错误消息使用独立的有界控制额度。

Host 在发送前整理错误，Runtime 接收时复核错误码、字段和限额。业务失败使用 REMOTE_ERROR，物理 Worker 故障与取消分别使用 WORKER_FAILED 和 ABORTED。业务根据 remoteError 重建自己的错误类型。
