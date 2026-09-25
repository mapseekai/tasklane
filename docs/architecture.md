# tasklane 架构设计

## 1. 设计目标

tasklane 面向高计算量、大数据和高交互应用，统一管理：

- Worker 生命周期
- 任务调度
- 资源准入
- 数据所有权
- 取消与超时
- 缓存亲和性
- 状态型 Session
- 结果背压
- 故障恢复与运行指标

整体结构：

```text
Application
    │
    ▼
WorkerRuntime
    │
    ├─ Scope / Session
    ├─ Admission Controller
    ├─ Scheduler
    ├─ Budget Ledger
    ├─ Affinity Registry
    └─ Metrics
          │
          ▼
      Worker Pool
          │
          ├─ Worker Slot
          ├─ Worker Slot
          └─ Worker Slot
                │
                ▼
             Worker Host
                │
                ├─ Task Registry
                ├─ Local Cache
                └─ Task Context
```

## 2. Runtime 与 Scope

`WorkerRuntime` 是应用级执行器，负责多个 Worker Pool 的统一资源管理。

`Scope` 用于划分业务所有权，例如：

```text
runtime
├─ map-a
│  ├─ source-roads
│  └─ source-buildings
└─ map-b
   └─ source-raster
```

Scope 提供：

- 唯一身份
- 子 Scope
- 任务集合
- ResultLease 集合
- Worker 本地缓存命名空间
- 生命周期级清理

这种模型适合多地图、多文档、多数据集或多插件共享一个 Runtime。

## 3. 有界调度

调度分成两步：

```text
Task enqueue
    ↓
Admission
    ↓
prepare()
    ↓
Worker execute
    ↓
ResultLease
    ↓
consumer release
```

任务进入等待队列时只保存轻量描述。满足以下条件后才进入执行阶段：

- 活跃任务数有空位
- 对应 Pool 有可用 Worker Slot
- 输入额度充足
- 暂存额度充足
- 结果额度充足
- 非丢弃任务有可用租约名额

prepare 只执行短小的同步输入构造，异步准备由 Worker handler 完成。队列中的闭包仍可能捕获用户数据，生产者也应采用有限提交窗口。预算管理申报量和协议数据，实际 JS 堆占用由应用结合运行环境测量。

## 4. 优先级与公平调度

支持三个任务优先级：

```text
interactive
foreground
background
```

典型场景：

| 优先级 | 场景 |
| --- | --- |
| interactive | 当前视口、拖动后的补数据、实时计算 |
| foreground | 用户主动执行的分析、导入、导出 |
| background | 预取、缓存构建、低优先级索引 |

调度器同时使用：

- 优先级
- 显式启用 ageing 时的等待提升
- Scope / group 上次服务序号
- 入队顺序

默认采用严格优先级；`priorityPolicy: 'ageing'` 才允许后台随等待时间提升到交互级别，两者均不抢占执行中的任务。

每个优先级/组/Pool 或 Session 通道维护 FIFO 索引堆，准入通过索引选择任务。空闲组历史有界保留，新组使用当前服务时钟，连续补交任务沿用服务历史参与公平调度。预算不足的等待者达到 `budgetWaitMs` 后，会保护其短缺额度，阻止小任务无限插队。

## 5. Worker Slot

每个物理 Worker 对应一个 Slot：

```text
Slot
├─ id
├─ epoch
├─ state
├─ current task
├─ optional session
├─ cache usage
└─ idle lifecycle
```

Slot 支持：

- 懒启动
- 协议握手
- 单物理任务执行
- 空闲回收
- Worker 代际
- 故障隔离

`epoch` 用于区分不同物理 Worker 实例，避免迟到消息与新实例任务发生关联。

## 6. 数据所有权与 Transferable

高性能路径使用任务自有 `ArrayBuffer`：

```text
prepare()
   │
   ▼
owned ArrayBuffer
   │ transfer
   ▼
Worker
   │ transform
   ▼
owned Result Buffer
   │ transfer
   ▼
ResultLease
```

`transferBuffers()`：

- 对完整 `ArrayBuffer` 去重
- 接受完整 backing store 的 TypedArray
- 显式表达所有权转移

适合坐标数组、像素块、压缩数据、二进制文件分片等大数据结构。

## 7. 资源预算

Runtime 使用 `BudgetLedger` 管理四类资源：

```text
inputBytes
scratchBytes
outputBytes
cacheBytes
```

### 输入预算

输入预算在同步调用 `prepare()` 前预留，发送前检查 `packetByteLength`：包含二进制 backing store、字符串以及扁平对象图编码的元数据。复合 TypedArray 包需要同时申报 buffer 和元数据空间。

### 暂存预算

`scratchBytes` 在准入时预留。`ctx.scratch.allocate(bytes)` 提供受限的临时 ArrayBuffer，release 或任务结束会 detach 全部别名。以下直接分配由应用估算和管理：

- JSON 解析对象
- WASM heap
- 解压缩工作区
- 几何中间数组

### 结果预算

结果预算在任务准入时预留，Host 编码、校验后才发送，Runtime 接收时只检查封装和字节数。首次访问 `lease.value` 才解码对象图；调用 `release()` 归还额度，`discardResult` 可不解码直接释放。

这种设计让消费者速度参与上游调度，形成自然的结果背压。

### 缓存预算

`cacheBytes` 用于 Worker 内的长期缓存：

- 三角化结果
- 解码块
- 字体数据
- 索引
- WASM 运行时缓存

## 8. ResultLease

任务成功后返回：

```ts
interface ResultLease<T> {
  readonly value: T;
  readonly byteLength: number;
  readonly released: boolean;
  release(): void;
}
```

ResultLease 将“任务完成”和“消费者完成使用结果”分开。

典型链路：

```text
Worker finished
    ↓
ResultLease
    ↓
consumer processing
    ↓
release()
    ↓
output budget available
```

使用 `consumeResult(handle, consume)` 确保消费结束后释放，包括消费者抛错的路径。`settled` 只等待物理完成；仅需完成通知应显式 `discardResult: true`。`maxResultLeases` 和 `maxScopes` 分别限制未释放结果与 Scope 数量。

## 9. 取消模型

提供三种取消策略。

### cooperative

主线程发送取消消息，Worker 任务通过 `AbortSignal` 和 `checkpoint()` 响应。

适合：

- 可分块循环
- 异步解析
- 分阶段算法
- 可周期性检查信号的 WASM 封装

### discard

调用方立即结束等待，物理任务继续运行直到完成，结果随后丢弃。

适合：

- 执行时间较短
- 算法本身同步
- Worker 缓存结果仍具有潜在复用价值

### terminate

终止任务所在的物理 Worker，并通过新 Worker 继续后续任务。

适合：

- 长时间同步算法
- 可安全重建 Worker 状态
- 大计算需要快速释放 CPU

## 10. Affinity

软亲和性用于提高 Worker 本地缓存命中率：

```ts
affinity: 'dataset-a/shard-17'
```

调度器优先选择此前处理过相同 affinity 的 Worker。

适合：

- 分块几何缓存
- 字体缓存
- 解码缓存
- 数据分片缓存

## 11. Session

Session 提供独占 Worker 的严格亲和性：

```ts
const session = scope.session('database');
```

生命周期：

```text
unbound
   ↓ first task
bound
   ↓ worker lost
lost

bound
   ↓ dispose
closed
```

适合：

- DuckDB / SQLite
- GDAL dataset
- 长驻 WASM instance
- 数据库连接
- 带内部状态的解析器

Session Worker 专属于一个会话，使状态与执行环境保持稳定对应。

## 12. Worker 本地缓存

Host 提供：

```ts
ctx.cache.get(key)
ctx.cache.set(key, value, bytes)
ctx.cache.setPinned(key, value, bytes)
await ctx.cache.delete(key)
```

普通缓存采用有界 LRU。

`setPinned()` 仅保存可检查的普通数据。数据库、WASM、数据集句柄等不透明实例使用 `setResource(key, value, bytes, disposer)`，声明资源费用并提供异步清理函数。`await cache.delete(key)` 在清理完成后归还额度；失败保留条目，允许重试。硬终止路径的外部资源清理由应用恢复策略负责。

缓存同时控制：

- 总字节数
- 条目数量
- Scope / Session 命名空间

## 13. 协议

协议版本为 4，Runtime 与 Host 必须使用同一版本。消息还携带 Worker epoch：

```text
hello
ready
request
progress
progress-ack
result
error
cancelled
cancel
release-scope
released
```

握手阶段由 Worker 返回支持的任务列表，Runtime 根据任务能力进行准入。

请求消息包含：

```text
tag
version
epoch
id
scope
session (optional)
task
payload (Packet)
maxOutputBytes
maxScratchBytes
```

progress 仅承载 4 KiB 内的小型控制数据，最多单条在途，收到 progress-ack 后才能继续发送；阻塞期间只保存最新快照。任务结束后 context 关闭。

Scope 销毁按 scope/epoch 等待 released 确认，超时或清理失败会拒绝；Session 正常关闭等待 disposer，失败保留 Worker 供重试。物理 terminate 失败保持隔离和额度，通过 `retryTermination()` 重试，资源释放以物理完成确认为准。

自定义 endpoint 应运行可信代码，并在发送前遵守协议与额度约束。接收侧先完成原生消息反序列化，再执行协议校验；进程级内存限制由运行环境提供。

## 14. 可观测性

每个任务提供：

```text
queueMs
startupMs
prepareMs
roundTripMs
workerMs
totalMs
```

Runtime 统计包括：

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
reserved
peakReserved
cacheUsedBytes
```

这些指标适合定位：

- Worker 数量配置
- 排队拥塞
- 输入准备成本
- 数据通信成本
- 算法执行成本
- 消费者背压
- 缓存占用

## 15. 推荐的应用结构

```text
Application
│
├─ Runtime
│  │
│  ├─ compute pool
│  │  ├─ geometry
│  │  ├─ projection
│  │  └─ format conversion
│  │
│  ├─ raster pool
│  │  ├─ decode
│  │  └─ resample
│  │
│  └─ stateful sessions
│     ├─ database
│     └─ wasm runtime
│
└─ consumers
   ├─ UI
   ├─ renderer
   ├─ storage
   └─ export pipeline
```

该结构适合持续扩展插件、数据格式和计算能力，同时保持 Worker 管理方式统一。
