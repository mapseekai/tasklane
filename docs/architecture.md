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

这使大文件处理可以保持稳定的在途数据量。

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
- 等待时间老化
- Scope / group 上次服务时间
- 入队顺序

适合一个应用内同时存在交互任务与大批量后台任务的情况。

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

输入预算在调用 `prepare()` 前预留，适合控制批量任务的数据包生成速度。

### 暂存预算

`scratchBytes` 用于声明算法执行期间的预计临时空间，例如：

- JSON 解析对象
- WASM heap
- 解压缩工作区
- 几何中间数组

### 结果预算

结果预算在任务准入时预留，在 `ResultLease.release()` 时释放。

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

特别适合流水线式处理、异步上传、分块写入和渐进式数据消费。

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
ctx.cache.delete(key)
```

普通缓存采用有界 LRU。

Session 中的固定状态可通过 `setPinned()` 保存，例如数据库实例、数据集句柄或大型运行时对象。

缓存同时控制：

- 总字节数
- 条目数量
- Scope / Session 命名空间

## 13. 协议

协议使用固定版本和 Worker epoch：

```text
hello
ready
request
progress
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
protocolVersion
workerEpoch
requestId
scopeId
sessionId
Task name
payload
maxOutputBytes
```

这种协议适合独立打包的 Worker 脚本与主包协同运行。

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
