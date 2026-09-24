# 大数据转换性能

验收日期：2026-09-24。

测试环境：

```text
CPU: Apple M4
Logical CPUs: 10
Memory: 16 GiB
Architecture: arm64
Node.js: v22.23.1
Chrome: 153.0.8010.53
```

## 1. 测试目标

性能测试用于评估 tasklane 在以下场景中的表现：

- 大规模二进制数据转换
- Float64 → Float32 high/low 布局
- 经纬度 → Web Mercator
- GeoJSON → 平坦二进制几何
- structured clone 与 Transferable 对比
- 1 / 2 / 4 Worker 扩展
- Runtime 调度与裸 Worker 对比
- 主线程响应性
- 资源峰值

## 2. 测量方法

Node 与 Chrome 各执行：

```text
28 configurations
× 3 repeats
= 84 measurements
```

两种环境合计 168 次计时。

每组执行 3 个完整计算块预热，正式统计记录：

- 总耗时
- 输入 MiB/s
- 8ms 页面心跳最大延迟
- 输入生成时间
- 结果消费时间
- prepare 时间
- Worker 时间
- 消息往返时间
- Runtime 资源预留峰值
- Node 进程 RSS 峰值

表格采用三次运行中位数，同时保留最小值和最大值。

## 3. 工作负载

### layout

```text
Float64 XY
    ↓
Float32 high / low XY
```

适合图形、GIS 和高精度坐标上传前的数据布局准备。

### project

```text
longitude / latitude
    ↓
Web Mercator
    ↓
Float32 high / low XY
```

适合投影和渲染前坐标转换。

### geojson

```text
GeoJSON FeatureCollection
    ↓
JSON parse
    ↓
flat XY / offsets / feature ranges
```

测试数据：

```text
100,000 LineString features
1,600,000 vertices
~55.5 MiB UTF-8
```

## 4. Chrome：64 MiB layout

| 执行方式 | Worker | 总耗时 ms | 输入 MiB/s | 心跳延迟 ms |
| --- | ---: | ---: | ---: | ---: |
| 同步主线程 | 0 | 108.8 | 588.2 | 101.0 |
| 协作式主线程 | 0 | 124.6 | 513.6 | 20.3 |
| Runtime / clone | 1 | 114.4 | 559.4 | 11.3 |
| Runtime / transfer | 1 | 108.8 | 588.2 | 8.2 |
| Runtime / transfer | 2 | 97.4 | 657.1 | 21.6 |
| Runtime / transfer | 4 | 98.3 | 651.1 | 42.6 |
| 裸 Worker / transfer | 2 | 97.9 | 653.7 | 22.0 |

**适合配置：** 大块坐标布局可从 1–2 个 Worker 起步。双 Worker 在该机器上取得最高中位吞吐量。

## 5. Chrome：64 MiB Web Mercator

| 执行方式 | Worker | 总耗时 ms | 输入 MiB/s | 心跳延迟 ms |
| --- | ---: | ---: | ---: | ---: |
| 同步主线程 | 0 | 149.2 | 429.0 | 141.6 |
| 协作式主线程 | 0 | 163.6 | 391.2 | 30.6 |
| Runtime / clone | 1 | 153.4 | 417.2 | 13.2 |
| Runtime / transfer | 1 | 150.0 | 426.7 | 10.4 |
| Runtime / transfer | 2 | 105.2 | 608.4 | 20.6 |
| Runtime / transfer | 4 | 103.6 | 617.8 | 42.9 |
| 裸 Worker / transfer | 2 | 106.3 | 602.1 | 19.7 |

**适合配置：** 投影计算具有较高 CPU 密度，2 个 Worker 可以兼顾吞吐量和页面响应性；更高并行度适合通过实际硬件基准决定。

## 6. Chrome：256 MiB Web Mercator

| 执行方式 | Worker | 总耗时 ms | 输入 MiB/s | 心跳延迟 ms |
| --- | ---: | ---: | ---: | ---: |
| 同步主线程 | 0 | 594.8 | 430.4 | 587.2 |
| 协作式主线程 | 0 | 729.3 | 351.0 | 29.7 |
| Runtime / clone | 1 | 453.6 | 564.4 | 14.4 |
| Runtime / transfer | 1 | 413.0 | 619.9 | 11.2 |
| **Runtime / transfer** | **2** | **225.4** | **1135.8** | **19.2** |
| Runtime / transfer | 4 | 317.1 | 807.3 | 42.0 |
| 裸 Worker / transfer | 2 | 225.4 | 1135.8 | 21.1 |

该组数据体现了 tasklane 在大型 CPU 密集转换中的主要价值：

- Transferable 提高二进制数据传递效率
- 双 Worker 提升整体吞吐量
- 主线程心跳保持更短延迟
- Runtime 调度性能接近相同算法下的裸 Worker

**推荐起点：2 Workers + 4–16 MiB 分块 + Transferable。**

## 7. Chrome：GeoJSON 平坦化

| 执行方式 | Worker | 总耗时 ms | 输入 MiB/s | 心跳延迟 ms |
| --- | ---: | ---: | ---: | ---: |
| 同步主线程 | 0 | 300.5 | 184.7 | 292.8 |
| 协作式主线程 | 0 | 401.7 | 138.2 | 18.8 |
| Runtime / clone | 1 | 308.1 | 180.2 | 5.7 |
| Runtime / transfer | 1 | 292.3 | 189.9 | 5.1 |
| **Runtime / transfer** | **2** | **170.7** | **325.2** | **12.2** |
| Runtime / transfer | 4 | 169.6 | 327.3 | 26.2 |
| 裸 Worker / transfer | 2 | 170.1 | 326.4 | 12.7 |

**适合配置：** JSON 解析和几何平坦化可采用 2 个 Worker 作为常用起点。数据源天然可分块时，可以进一步基于机器核数和内存压力调节。

## 8. Node.js：256 MiB Web Mercator

| 执行方式 | Worker | 总耗时 ms | 输入 MiB/s | 心跳延迟 ms | RSS 峰值 MiB |
| --- | ---: | ---: | ---: | ---: | ---: |
| 同步主线程 | 0 | 989.7 | 258.7 | 989.4 | 217.2 |
| 协作式主线程 | 0 | 1033.5 | 247.7 | 32.2 | 176.9 |
| Runtime / clone | 1 | 915.1 | 279.8 | 20.0 | 257.9 |
| Runtime / transfer | 1 | 807.9 | 316.9 | 17.9 | 194.8 |
| Runtime / transfer | 2 | 747.0 | 342.7 | 40.8 | 278.8 |
| Runtime / transfer | 4 | 751.2 | 340.8 | 90.4 | 360.2 |
| 裸 Worker / transfer | 2 | 749.6 | 341.5 | 41.0 | 293.0 |

Node.js 环境下，双 Worker 仍提供较高吞吐量。多 Worker 同时增加常驻内存，适合根据服务器资源预算配置。

## 9. 1 GiB 有界分块流

压力参数：

```text
1 GiB cumulative input
67,108,864 points
64 chunks
16 MiB / chunk
2 Workers
2 ms simulated consumer delay
```

实测：

| 指标 | 结果 |
| --- | ---: |
| 总耗时 | 2331.0 ms |
| 输入吞吐量 | 439.3 MiB/s |
| input 预留峰值 | 32 MiB |
| output 预留峰值 | 32 MiB + 64 bytes |
| 采样 RSS 峰值 | 243.5 MiB |

这组测试体现了**累计处理量与同时在途数据量解耦**的能力。

适合：

- 超大文件分块转换
- 流式解码
- 栅格块处理
- 矢量块构建
- 分块导入/导出

## 10. 单个 256 MiB Buffer

```text
256 MiB Float64 input
16,777,216 points
single transferable buffer
```

完成一次所有权转移和高低位布局转换，并对全部点执行数值校验。

适合验证：

- 大 ArrayBuffer 转移
- 大型单任务
- Worker 端连续数组计算
- 大结果返回

## 11. 单个约 55.5 MiB GeoJSON

```text
100,000 features
1,600,000 vertices
~55.5 MiB UTF-8
```

完成单次 JSON 解析、几何平坦化和全数组校验。

适合验证：

- 大文本解析
- JSON 对象构建
- 几何数组生成
- 属性/几何格式插件的 Worker 化基础

## 12. Worker 数量建议

当前 Apple M4 测试结果可作为浏览器交互应用的起点：

```text
1 Worker
适合：轻量计算、内存敏感应用、串行状态型任务

2 Workers
适合：大多数 CPU 密集型转换、GIS 几何处理、投影、格式解析

4 Workers
适合：高并行算法、后台批量处理、核数和内存资源充足的场景
```

对高交互应用，建议同时观察：

```text
throughput
maxTimerLagMs
peakReserved
RSS / memory
queueMs
workerMs
```

Worker 数量可以通过运行时基准自动选择或作为应用配置项开放。

## 13. clone 与 transfer

性能测试同时提供两条数据通路：

```text
structured clone
Transferable ownership
```

对于任务自有的大型 `ArrayBuffer`，Transferable 更适合作为高吞吐数据通路。

对于仍由主线程业务模型持有的数据，可以在 `prepare()` 中构造独立任务包，再将该任务包通过 Transferable 交给 Worker。

## 14. Runtime 与裸 Worker

双 Worker 对照使用相同：

- 转换算法
- 数据块大小
- Transferable
- 结果校验

Runtime 额外提供：

- 有界准入
- Scope
- Session
- 优先级
- 公平调度
- 资源预算
- 结果背压
- 取消语义
- Worker epoch
- 统计指标

在主要测试组中，Runtime 的墙钟性能与裸 Worker 接近，适合将这些运行时能力作为统一基础设施使用。

## 15. 原始数据

完整原始结果：

- [Node.js](results/node.json)
- [Chrome](results/browser.json)
- [Stress](results/stress.json)
- [Verification](results/verification.json)

重新运行基准并同步原始结果快照：

```sh
pnpm benchmark
pnpm benchmark:browser
pnpm test:stress
node scripts/report.mjs
```
