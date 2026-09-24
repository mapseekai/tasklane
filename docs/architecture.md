# 架构、资源所有权与集成边界

## 分层

`WorkerRuntime → 准入/公平调度 → 物理执行槽 → 版本化协议 → serve/任务函数`。

通用运行时只认识任务名、作用域、预算、优先级、会话和二进制消息。源版本、图层顺序、过滤、几何缓存键、编辑历史、渐进场景完整性属于宿主，不能为了共享线程而搬入通用协议。

核心运行时在 `src/runtime/runtime.ts`；资源准入、缓存、结果分别在 `src/resources/`；协议与 Host 在 `src/protocol.ts`、`src/host.ts`；浏览器和 Node 端点在 `src/adapters/`。Node 专属导入不进入浏览器入口。

## 任务的两个生命周期

调用生命周期：等待结果 → resolve/reject。物理生命周期：queued → starting → preparing → running → terminal。

取消会立即拒绝调用者的 `result`，但 preparing/running 可能继续占用资源。`settled` 仅在实际完成或确认端点停止后 resolve，永不 reject。排队任务直接移除；已执行任务根据取消策略处理。主线程 prepare 回调不能由本库强行抢占，必须最终返回或观察 signal。

执行截止时间从获得准入开始，包含初始化等待、prepare 和工作线程计算。排队截止时间独立计算。结果进入租约后不再属于执行超时范围；消费者必须自行使用 finally 释放。

端点终止返回 Promise 时，运行时等它完成才释放物理容量。终止失败则隔离该槽、保留相关额度，并让 dispose 报错，不假装线程已停止。用户自定义适配器必须真实表达物理终止语义。

## 有界准入和背压

入队只保留轻量任务描述；同时满足活跃任务数、可用执行槽、输入/暂存/结果额度后才运行 prepare。

结果额度预留的是上限，成功返回后持续保留到 ResultLease.release。即使实际输出较小，也不会提前把预留剩余值贷给其他任务。因此不会出现结果已到主线程、预算却已经腾空导致无限堆积的窗口。

`binaryByteLength` 对同一对象图中的 ArrayBuffer backing store 去重；切片视图按完整 backing store 计，防止 1 字节视图遮蔽巨大底层内存。输入和结果验证在发送前/接收后分别进行。非二进制对象内存、跨消息共享对象、GC 及引擎开销不能据此精确估算。

scratchBytes 是调用者声明的保守暂存预算，本库不会注入第三方分配器。使用结构化克隆、JSON.parse 或 WASM 时，应为可能的临时复制和对象膨胀留出余量，辅以进程/浏览器观测。

## 线程数与公平性

maxWorkers 约束整个 Runtime 的常驻与正在关闭的槽；maxActiveTasks 约束同时准备/计算的任务；每槽最多一个物理任务。等待队列有 maxQueuedTasks 上限，绝不提前把大量任务发到 Worker 内部排队。

任务按优先级、等待老化、作用域/组上次服务时间和入队顺序选择。亲和性只是缓存偏好，不会让有空闲资源时的可重建任务永久等待指定 Worker。执行中的同步计算不可抢占，所谓优先级只作用于后续准入。

缓存亲和性记录数有上限；组历史在组内任务全部结束后清理。空闲 Worker 会按设置回收；不同池在全局容量耗尽时可以让出未绑定会话的空闲槽。

## 会话与缓存

Session 是严格亲和性：一个会话独占一个物理 Worker。适用于数据库句柄、不能随便迁移的状态或需保持隔离的 WASM 实例。会话失去 Worker 后转为 lost，后续调用立即报错；由宿主重建会话及其内容。

Host 的 cache.get/set 自动命名空间隔离。常规条目采用 LRU；setPinned 仅在 Session 内有效，不允许为了腾空间静默删除数据库等必需状态。缓存同时限制字节数和条目数，零字节元数据也不能无限增长。

Scope 关闭释放自身结果、子作用域、会话和 Worker 侧缓存；不会销毁其他作用域仍在使用的普通 Worker。Session 关闭同时释放自身结果租约。CPU 源数据/GPU 资源不由本库自动接管。

## 协议与安全

协议版本为 1，包含 tag/version/epoch 和 request id、scope id。先 hello/ready 协商任务能力，再发送 request。ready 列出的任务是 Host 预注册函数，不通过 eval 或任意函数字符串执行。

旧 epoch、不同 id/作用域的迟到响应不能覆盖新任务。非法协议、启动失败、反序列化错误、计算超时都有明确失败路径。Worker URL 与脚本属于可信应用配置；Web Worker 不是隔离恶意插件的完整安全沙箱。不得将不可信代码和敏感凭据放入同一可信域后声称已被本库安全隔离。

推荐同源 module Worker 与严格 CSP。部署须让主包和 Worker 使用匹配构建，缓存/CDN 的错误旧资源通过握手显式失败。示例 HTTP 服务只绑定 loopback，不提供目录遍历、任意文件类型或隐藏文件访问。

## 后续 emap / luma 集成

当前版本未接入 emap，没有定义第二套图层、拓扑或编辑模型。推荐由 Cordis 根作用域持有一个 Runtime，各地图/数据源申请子 Scope。

第一迁移对象应是现有 GeometryTransport 注入缝，继续使用 GeometryPacket/GeometryResult。三角化、平坦化等任务返回自有 TypedArray；源拓扑不能被自动 transfer 分离。Glyph、栅格、mapshaper、DuckDB 应按各自状态/取消能力设计适配器，不能直接塞进同一个无状态任务池。

GPUBuffer、Texture、Pipeline、Device 不进入消息协议。本库不声称这些资源可通过 Transferable 在普通池中轮转。主线程或专用 Render Worker 持有唯一 RenderEndpoint，消费 ResultLease、控制上传额度并保证绘制顺序。CPU 任务可以乱序完成，绘制顺序由宿主决定。

sourceRevision、selectionRevision、viewSequence、deviceEpoch 由宿主分别校验。设备丢失不等于源数据失效；几何 Worker 重启也不等于现有 GPU 缓冲失效。

专用 OffscreenCanvas Render Worker、同 Device 内 WebGPU Compute、完整双向服务 RPC 和第三方自主管理 Worker 的外部执行器都不在 0.1 实现范围内。端点协议适配接口已开放，但不能把接口存在当作这些集成已经验收。
