# 测试与验收

## 本机结果（2026-09-24）

| 范围 | 通过 | 失败 | 跳过 |
| --- | ---: | ---: | ---: |
| 单元、故障注入、真实 Node Worker 集成 | 78 | 0 | 0 |
| Chrome / Firefox / WebKit 浏览器 | 33 | 0 | 0 |
| 大数据和取消压力 | 4 | 0 | 0 |

类型检查（含预期失败的公共类型用例）、格式检查、lint、构建、npm pack dry-run 以及独立目录安装 tarball 后的真实 Worker 冒烟均通过。结构化摘要见 [verification.json](results/verification.json)。CI 工作流已提供；本机通过不能冒充所有 CI 平台均已执行成功。

## 每类测试验证什么

`test/resources.test.mjs`：预算原子性和幂等释放；重复 backing store、切片、循环引用、Map/Set 的计账；accessor/custom class 防漏计；SAB 不可被错误转移；缓存字节与条目双限制；固定会话状态不被 LRU 偷删；ResultLease 失效。

`test/runtime.test.mjs`：真实 worker_threads 的惰性初始化、输入 transfer/clone、在 prepare 前准入、慢消费者背压、三种取消、物理占用、超时、异常、原始缓冲不误 detach、多作用域/父子作用域清理、软缓存亲和性、独占会话、会话丢失、优先级、公平性、跨池容量回收、200 请求关联。

`test/faults.test.mjs`：用真正 structuredClone 的同域端点进行协议/生命周期故障注入，包括旧 epoch、错误 scope、非法包、缺失握手、工厂失败、异步终止、终止失败隔离、prepare 抛错、Session 租约清理和调度元数据增长。此端点不是性能或并行能力证据。

`test/conversion.test.mjs`：两种内核各 100 万点，对独立数学表达式重建坐标逐项校验；高低位合成误差阈值 1e-6 源/投影单位。检查空输入、奇数 XY、非有限数、超域纬度、非法 JSON、未支持几何类型、面洞、MultiPolygon 分组、要素顺序、末尾不足 4 字节的校验。

浏览器每个引擎 11 项：真实二进制转移/克隆、慢消费准入、多 Scope/会话/错误关联、三种取消、百万点输出、带名称的 module Worker、独立示例完成和取消。三个引擎合计 33 项。浏览器任务通过本地同源服务器和真实 Worker URL 运行，没有用假的 Canvas/Worker 替代。

## 大数据压力输入

**1 GiB 逻辑坐标流**：67,108,864 点，16 MiB 块；双 Worker、完整输出指纹、人工慢消费。预留上限证明和计时分别记录。这不是先在内存里创建完整 1 GiB 文件后再隐藏文件内存。

**单个 256 MiB ArrayBuffer**：16,777,216 点，一次转移并转换，高低位输出全部点都与确定性输入公式校验。这个用例证明大单包正确性，不代表推荐所有应用使用大单包；交互应用优先分块。

**单个约 55.5 MiB UTF-8 GeoJSON**：100,000 个 LineString，1,600,000 点，单次解析/平坦化，全部 XY 数组和要素计数校验。只测试二维几何，不处理完整属性表或拓扑构建。

**1000 请求取消风暴**：取消其中 666 个，剩余 334 个全部一一对应，没有重复/串包；最终队列、物理活跃任务和结果租约归零。

输入均在测试中确定性生成，没有使用或上传用户 GIS 原始数据。测试生成器和转换算法不属于核心 Runtime 的格式兼容承诺。

## 性能矩阵

见 [performance.md](performance.md)。Node 与 Chrome 分别执行 4 个工作负载/规模 × 7 种执行配置 × 3 次重复，共 168 次计时。包括同步主线程、主动让出的主线程、单 Worker clone、1/2/4 Worker transfer、双裸 Worker transfer。

没有硬编码“必须比主线程快 N 倍”的测试断言，因为 CI 硬件、调度和 GC 都会变化。正确性、预算、释放和取消语义是硬断言；时间结果用完整数据与环境说明报告。

## 复现

```sh
pnpm install --frozen-lockfile
pnpm verify
pnpm test:stress
pnpm benchmark
pnpm benchmark:browser
node scripts/report.mjs
```

浏览器缺少可执行文件时先运行 `pnpm exec playwright install chrome firefox webkit`，Linux CI 使用 `--with-deps`。浏览器实际缺失必须报失败，不通过统一 skip 隐藏。

部分检查：

```sh
pnpm build
node --test test/faults.test.mjs
node --test test/conversion.test.mjs
pnpm test:package
BROWSERS=chrome pnpm test:browser
node benchmarks/node.mjs --quick
node benchmarks/browser.mjs --quick
```

## 尚未证明的内容

未执行 emap 集成、真实 GPU 上传、WebGL2/WebGPU 渲染、OffscreenCanvas 生命周期、DuckDB/GeoTIFF/GDAL 等第三方格式适配或所有移动设备测试。WebKit 引擎回归不是完整 Safari/iOS 产品矩阵。预算测试也不是浏览器/操作系统内存硬隔离证明。

Node 运行时代码无需 DOM；当前 TypeScript 声明同时提供浏览器接口，显式裁剪 lib 的 Node TypeScript 项目应包含 `ES2022` 与 `DOM` 类型库。这不要求 Node 存在真实 DOM 对象。
