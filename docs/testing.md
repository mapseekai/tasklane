# 测试与验收

## 1. 当前验收结果

2026-09-25 本机验收（协议 v3）：

| 范围 | 通过 | 失败 | 跳过 |
| --- | ---: | ---: | ---: |
| 单元、故障注入、真实 Node Worker 集成 | 114 | 0 | 0 |
| Chrome / Firefox / WebKit 浏览器 | 39 | 0 | 0 |
| 大数据与取消压力 | 4 | 0 | 0 |

同时通过：

- TypeScript 类型检查
- 公共 API 类型约束
- Biome format
- Biome lint
- 构建
- npm pack dry-run
- 独立目录安装 tarball
- tarball 中真实 Node Worker 冒烟

GitHub Actions 配置覆盖 Node.js 22/24 与浏览器；上述是本机结果，本次未核验远端 CI 运行状态。

结构化摘要位于仓库 `docs/results/verification.json`；原始结果通过源码仓库提供。

## 2. 核心资源测试

`test/resources.test.mjs` 覆盖：

- BudgetLedger 原子预留
- 幂等释放
- ArrayBuffer backing store 去重
- TypedArray 切片计账
- 循环对象图
- Map / Set
- SharedArrayBuffer 计账
- Transferable 所有权校验
- LRU 缓存
- 缓存条目数上限
- Scope 缓存隔离
- Session pinned state
- ResultLease 生命周期

## 3. Runtime 集成测试

`test/runtime.test.mjs` 使用真实 `worker_threads`，覆盖：

- Worker 惰性启动
- Worker 复用
- Transferable
- structured clone
- 输入准入
- scratch 额度
- 结果背压
- Scope 生命周期
- 子 Scope
- 三种取消模式
- 物理任务收敛
- queue timeout
- execution timeout
- Worker crash
- Worker respawn
- 软 affinity
- Worker cache
- Session
- Session lost
- 优先级
- 默认严格优先级（可选老化由调度器回归测试覆盖）
- 公平调度
- 多 Pool 资源再分配
- 大量并发请求关联

## 4. 协议与故障注入

`test/faults.test.mjs` 覆盖：

- Worker epoch
- 跨 Scope 隔离
- 非法协议消息
- 结果字节校验
- 异步 terminate
- terminate 失败隔离
- startup timeout
- Worker factory error
- prepare error
- Session ResultLease 释放
- 调度元数据回收
- 配置快照

该测试使用 `structuredClone` 语义的同域端点，适合稳定构造协议级故障。

`test/regressions.test.mjs` 和 `test/packet.test.mjs` 另覆盖：

- 8000 项调度选择、流式组公平性、有界历史和预算饥饿保护
- 跨 Pool 回收数量、启动期取消、同步握手失败与提前响应
- Packet 对象图身份、元数据计费、延迟解码与消费失败释放
- 20 万元素数字数组与超宽输入提前拒绝
- 普通缓存附加属性、实例资源、异步 disposer 和失败重试
- Scope 释放 ACK、租约/Scope 数量上限与 terminate 重试
- 永不结束的 Promise prepare 立即拒绝，不阻塞销毁
- Node 错误监听窗口的确定性注入和 20 次真实 Worker 竞态

## 5. 大规模数值正确性

`test/conversion.test.mjs` 使用确定性坐标输入验证：

### 100 万点 layout

```text
Float64 XY
    ↓
Float32 high / low XY
```

对每一个点重新组合高低位，与独立数值参考逐项比较。

### 100 万点 Web Mercator

```text
longitude / latitude
    ↓
Web Mercator
    ↓
Float32 high / low XY
```

每个点都与独立公式计算结果逐项比较。

同时覆盖：

- 空输入
- 非有限坐标
- XY 长度校验
- 投影纬度范围
- JSON 解析
- Polygon hole
- MultiPolygon
- feature / polygon / path offsets
- 末尾字节校验

## 6. 浏览器测试

Playwright 运行：

```text
Chrome
Firefox
WebKit
```

每个浏览器运行 13 项测试，组合覆盖：

- 元数据低报、20 万数字数组、scratch 超限与 Promise prepare 拒绝
- progress 大小/ACK、完成后 context、丢弃结果和异步资源清理
- 8 MiB Transferable
- structured clone
- ResultLease 背压
- Scope 隔离
- Session
- 错误关联
- cooperative cancellation
- discard cancellation
- terminate cancellation
- 100 万点转换
- module Worker 配置
- 大数据示例完成
- 示例取消与资源收敛

浏览器测试通过真实 Worker URL 和同源 HTTP 服务运行。

## 7. 大数据压力测试

### 1 GiB 逻辑坐标流

参数：

```text
67,108,864 points
1 GiB cumulative input
16 MiB / chunk
2 Workers
64 chunks
```

验证：

- 全量数据处理
- 双 Worker 调度
- 完整输出指纹
- 慢消费者背压
- 输入额度峰值
- 输出额度峰值（2 个通道预留 input <= 32 MiB + 8192 bytes，output <= 32 MiB + 16384 bytes，含协议元数据）
- Worker 生命周期收敛

### 单个 256 MiB ArrayBuffer

参数：

```text
16,777,216 points
256 MiB Float64 input
single transferable buffer
```

验证：

- 单大包 Transferable
- 完整转换
- 16,777,216 点逐项数值校验
- Worker 释放

### 单个约 55.5 MiB GeoJSON

参数：

```text
100,000 LineString features
1,600,000 vertices
~55.5 MiB UTF-8
```

验证：

- JSON 解析
- feature 顺序
- path offsets
- geometry flatten
- 全量 XY 数据
- 结果数量

### 1000 请求取消风暴

参数：

```text
1000 requests
666 cancelled
334 completed
```

验证剩余任务结果一一对应，并检查最终：

```text
queued = 0
active = 0
leases = 0
```

## 8. 性能测试矩阵

完整基准命令为 Node 与 Chrome 分别安排：

```text
4 workload / size groups
× 7 execution modes
× 3 repeats
```

每个环境 84 次，合计 168 次。完整矩阵通过独立基准命令运行；pnpm verify 执行功能回归。此处 168 次计时对应本节记录的环境与版本。

执行方式：

```text
同步主线程
协作式主线程
Runtime + clone + 1 Worker
Runtime + transfer + 1 Worker
Runtime + transfer + 2 Workers
Runtime + transfer + 4 Workers
裸 Worker + transfer + 2 Workers
```

记录：

- 总耗时
- 输入吞吐量
- 页面心跳滞后
- startupMs
- warmupMs
- prepareMs
- workerMs
- roundTripMs
- 输入/输出字节
- Runtime 资源峰值
- Node RSS 峰值
- 完整结果指纹

详细结果见 [performance.md](performance.md)。

## 9. 复现

完整验证：

```sh
pnpm install --frozen-lockfile
pnpm verify
pnpm test:stress
```

浏览器安装：

```sh
pnpm exec playwright install chrome firefox webkit
```

性能测试：

```sh
pnpm benchmark
pnpm benchmark:browser
pnpm benchmark:scheduler
pnpm benchmark:cache
node scripts/report.mjs
```

快速基准：

```sh
pnpm build
node benchmarks/node.mjs --quick
node benchmarks/browser.mjs --quick
```

包安装验证：

```sh
pnpm test:package
```

独立示例：

```sh
pnpm build
node examples/node.mjs
pnpm dev
```

Node 示例校验转换结果和最终资源计数；浏览器示例的完成与取消路径由 Playwright 覆盖。`scripts/report.mjs` 复制原始性能快照并打印摘要；testing.md、performance.md 和 verification.json 由维护者同步更新，并核对版本和运行日期。
