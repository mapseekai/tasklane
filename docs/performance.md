# 性能验证与基准

输入输出包含元数据计费，结果首次读取 value 时解码，调度默认严格优先级。基准脚本已按这些规则预留额度。

仓库 `docs/results/node.json`、`browser.json` 和 `stress.json` 保存的是 2026-09-24 修复前的历史快照。这些吞吐量、资源峰值和 Runtime/裸 Worker 对比适用于该历史版本。评估当前实现需重新运行完整的 168 次计时矩阵；原始数据保留用于复现和对照，当前功能验收见 [测试与验收](testing.md)。

## 数组编解码基准

```sh
pnpm benchmark:packet
```

脚本对 10 万个递增数字、5 万组 `[i, i]` 坐标、10 万个元素的 Float64Array 及包含该 TypedArray 的小对象分别测量编码、解码和协议计费量。预热 3 次，正式测量 11 次，取中位数；不含输入构造、跨线程传输或 handler 执行。需要比较旧实现时，构建后运行 `node benchmarks/packet.mjs /path/to/baseline/packet.js`，并保留旧模块依赖的相对导入文件。

2026-09-26，Apple M4、Node 22.23.1，同一进程对本次修改前后实现的测量：

| 输入 | 编码：修改前 → 修改后 | 解码：修改前 → 修改后 | 协议计费：修改前 → 修改后 |
| --- | ---: | ---: | ---: |
| 10 万个数字 | 19.0 → 11.4 ms | 45.6 → 1.7 ms | 3.16 → 1.18 MB |
| 5 万组坐标 | 44.5 → 28.8 ms | 44.4 → 12.5 ms | 8.31 → 6.13 MB |

普通数组省去了连续元素的字符串下标，解码复用 JSON 解析得到的数组，仍需遍历和 JSON 编解码。协议 MB 为十进制计费单位，其中元数据按字符串长度乘 2 计算，不代表实际堆占用。耗时随机器、JIT 和 GC 波动，回归测试检查较小预算下的数据正确性，不设置耗时门槛。

大规模坐标优先放入 TypedArray 字段：本次两个 TypedArray 用例的编解码均低于 0.01 ms，协议计费为 800,000 字节及额外 286 字节对象元数据。该结果不包含 buffer 的克隆或转移开销；转移需显式提供 transfer list。普通数组的对象数和遍历边上限继续生效。

## 当前调度与缓存基准

```sh
pnpm benchmark:scheduler
pnpm benchmark:cache
```

调度基准使用 1 个预热后的 Node Worker、8 个 interactive 组、一次性入队，每档运行 3 次。本次修复验证记录：

| 任务数 | 批次总耗时中位数 | 三次运行最大定时器延迟 |
| ---: | ---: | ---: |
| 500 | 19.3 ms | 1.9 ms |
| 1000 | 24.0 ms | 1.4 ms |
| 2000 | 47.6 ms | 2.2 ms |
| 8000 | 148.5 ms | 12.3 ms |

批次墙钟时间覆盖整个批次的执行过程；连续主线程阻塞和 CPU 时间需采用对应的测量方法，并在相同测量口径下比较。功能回归还检查 8000 项可运行任务的选择次数，避免单纯依赖受机器负载影响的时间阈值。

缓存基准只测 key 构造：100 万次操作，预热后取 3 次中位数。此次 JSON tuple 为 69.0 ms，命名空间前缀为 7.9 ms；测量范围为 key 构造，端到端吞吐评估还需覆盖缓存查找、数据检查和驱逐。

## 大数据基准矩阵

```sh
pnpm benchmark
pnpm benchmark:browser
```

Node 与 Chrome 分别运行 4 组负载、7 种执行方式、3 次重复，每个环境 84 次。Node 每个用例启动独立进程，Chrome 每个用例使用新 context；每组正式计时前完成 3 个完整块的预热。

负载包括：

- 64 MiB Float64 XY → Float32 high/low 布局
- 64 MiB、256 MiB 经纬度 → Web Mercator → high/low 布局
- 100,000 个 LineString、1,600,000 个顶点、约 55.5 MiB UTF-8 GeoJSON → 平坦几何数组

执行方式包括同步主线程、协作式主线程、Runtime clone/单 Worker、Runtime transfer/1/2/4 Worker、裸 Worker transfer/2 Worker。

正式总时间包含确定性输入生成、传输、调度和全量输出 checksum 消费，排除启动、预热和磁盘 I/O。所有模式的完整输出指纹必须一致。读取 lease.value 产生的解码工作计入消费阶段；timing.totalMs 记录任务完成前的耗时。

指标区分：

| 指标 | 语义 |
| --- | --- |
| 基准 inputBytes / outputBytes | 业务二进制数组字节，用于计算吞吐量 |
| runtimeStats.inputBytes / outputBytes | 成功发送的输入与成功结果的协议计费量，含元数据 |
| runtimeStats.peakReserved | 准入声明额度的峰值 |
| prepareMs | 同步 prepare 回调本身的执行时间 |
| roundTripMs | dispatch 到终态消息的往返时间，不含主线程结果解码 |
| workerMs | Host 从执行开始到结果发送前的时间，含输入解码和输出编码 |
| maxTimerLagMs | 8 ms 心跳的最大延迟 |
| Node RSS | 包含 Worker 的进程采样峰值，可能漏掉采样间瞬时峰值 |

浏览器基准不测 RSS。GeoJSON 的 JSON.parse 和算法直接创建的对象不由 scratch arena 自动度量；其 scratchBytes 仍是算法声明。

快速检查使用更小矩阵和 1 次计时，不代替完整性能统计：

```sh
pnpm build
node benchmarks/node.mjs --quick
node benchmarks/browser.mjs --quick
```

## 大数据正确性与背压

```sh
pnpm test:stress
```

4 项测试覆盖 1 GiB 累计分块流、单个 256 MiB buffer、约 55.5 MiB GeoJSON，以及 1000 请求取消风暴。1 GiB 流使用 2 个 Worker、每块 16 MiB 和模拟慢消费者；当前基准为元数据预留额外空间，断言 input 峰值不超过 32 MiB + 8192 bytes，output 不超过 32 MiB + 16384 bytes。

这些测试验证数据正确性、声明额度和生命周期收敛，不证明相同数据量的整个 JS 对象图或进程内存受同样上限控制。

## 参数选择

仓库浏览器演示使用 4 MiB 分块，可选 1/2/4 Worker。应用应结合实际算法、设备与消费速度比较吞吐量、定时器延迟、queueMs、资源预留和进程内存；通过相同负载下的实测选择 Worker 数量。

任务自有 ArrayBuffer 可使用 Transferable，业务仍持有的数据则需保留原值或在同步 prepare 中复制当前分块。普通复合包还需申报元数据。生产者应使用有限提交窗口，避免把大量业务对象捕获在排队闭包中。

## 原始结果与更新流程

历史快照位于[源码仓库的 docs/results](https://github.com/mapseekai/tasklane/tree/main/docs/results)，原始 JSON 通过源码仓库提供。本地仓库中也可直接读取这些文件；修复验证清单见 [review-resolution.md](review-resolution.md)。

重新生成完整性能快照：

```sh
pnpm benchmark
pnpm benchmark:browser
pnpm test:stress
node scripts/report.mjs
```

脚本复制 benchmark-results 下的 node/browser/stress JSON，并打印选定统计。本页与 verification.json 由维护者同步更新。同步时应核对每个文件的运行时间、代码版本和重复次数，并按对应版本整理验收结论。
