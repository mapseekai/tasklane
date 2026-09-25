# 审查问题闭环清单

本清单合并多轮报告中的重复项。首个预发布版本为 0.1.0-beta.1；协议已升级为 v3，不保留旧协议或异步 prepare 兼容层。

| # | 问题 | 当前处理与验证 |
| --- | --- | --- |
| 1 | 调度反复复制、排序队列，完成时扫描 Scope | 改用按优先级、组和 Pool/Session 通道组织的索引堆；入队缓存 groupKey；完成直接删除索引，不再扫描同组任务。8000 项选择计数回归和真实 Worker 基准覆盖。 |
| 2 | 跨 Pool 重复回收空闲 Worker | 回收期间禁止重复选 victim；4 个空闲 Worker/目标 Pool 大小 1 的回归确认仅终止 1 个。 |
| 3 | preparing 阶段响应导致监听器抛异常 | phase 与 postedAt 前置检查，异常消息转为 Worker 退役；同步 prepare 中注入响应覆盖。 |
| 4 | 字符串、普通数组、scratch 可绕过预算 | 输入输出采用含元数据计费的 Packet；普通缓存使用 dataByteLength；scratch arena 拒绝超额分配。任意 JS/native 分配不属于可强制监测范围，契约明确说明。 |
| 5 | 遍历先分配整层再拒绝、原始值占对象配额 | 数组/集合提前检查尺寸，逐项限制对象、边和工作栈；数字不占对象数。20 万数字数组往返、超宽数组拒绝前零 getter 调用覆盖。 |
| 6 | Worker/主线程重复遍历结果，收到后才验证额度 | Host 编码并检查后才发送；主线程只检查封装字节，首次 value 访问才解码。绕过 Host 的任意恶意消息无法由同进程传输层在反序列化前拦截，明确只运行可信 Worker。 |
| 7 | progress 无大小限制和背压 | 4 KiB 小型控制消息、单条在途 ACK、只保留最新快照；Node 和三个浏览器验证。 |
| 8 | 不结束的异步 prepare 占额度、阻塞 dispose | API 改为同步构造；Promise/thenable 立即拒绝并归还额度；异步工作在 Worker 内执行。同步主线程死循环仍不能抢占，违规回调的外部副作用也不能撤销。 |
| 9 | terminate 失败永久隔离 | 保留真实额度，提供 retryTermination 和隔离诊断；故障恢复后重试验证无双重释放。 |
| 10 | 忘记 release 的结果驻留 | consumeResult 保证 finally 释放，discardResult 支持仅等 settled；maxResultLeases 限制积累。仍需保留的租约采用显式所有权，不以不确定的 GC 回调作为正确性保证。 |
| 11 | 未关闭 Scope 长期积累 | withScope 自动结束作用域，maxScopes 限制数量，resourceDiagnostics 定位所有者；计数和异常消费回归覆盖。 |
| 12 | 大预算任务被小任务饿死 | budgetWaitMs 后保护等待者短缺额度；持续重叠小任务流回归验证大任务完成。 |
| 13 | ageing 将后台提升到交互前面 | 默认 strict；仅显式 ageing 策略可跨优先级提升。老后台/新交互回归覆盖。 |
| 14 | checkpoint 定时器钳制 | Node setImmediate；浏览器 scheduler.yield/MessageChannel。真实 Worker 协作取消测试覆盖。 |
| 15 | size 较大仍默认只有 2 路执行 | 默认 min(pool capacity, maxWorkers)，4 路并发回归覆盖。 |
| 16 | Node 移除 error 监听至退出的窗口 | 物理 exit 前保留错误保护监听；确定性 EventEmitter 注入和 20 次真实 Worker 竞态覆盖。 |
| 17 | 热点 cache key JSON.stringify | 缓存命名空间数字前缀；新增隔离 key 构造基准，不把结果误称为端到端缓存吞吐。 |
| 18 | 发布 access、包体、CHANGELOG/tag | public access 和文件白名单；打包安装后真实 Worker smoke。CHANGELOG 记录实际 beta 版本和日期；版本 tag 对应发布提交。正式发布步骤见下文。 |
| 19 | 流式提交清空组服务历史 | 有界空闲历史和当前服务时钟初始化新组；持续提交与 backlog 的公平性回归覆盖。 |
| 20 | Session 类/WASM 资源无法缓存或清理 | setResource 独立声明计账并注册异步 disposer；清理失败保留资源/Worker 供重试。硬终止不能保证外部 disposer 执行。 |
| 21 | 启动中取消绕过执行截止时间 | 独立物理 phase；启动不应答后取消仍按执行截止时间回收的测试覆盖。 |
| 22 | hello 同步失败后 Session 绑定已关闭槽位 | spawn 失败抛错、绑定前检查状态；后续任务不再永久排队。 |
| 23 | Scope dispose 不等待释放 ACK | 按 Scope/epoch 跟踪确认、releaseTimeoutMs 限制等待；缺 ACK 和延迟 ACK 回归覆盖。 |
| 24 | 常驻内建对象附加属性漏计 | 驻留数据检查集合、Date/RegExp、buffer/view 的附加字段，包含 DataView 数字属性与 shadow accessor 回归。 |
| 25 | 任务完成后 context 仍发 progress | finally 关闭 context，清除 timer/pending；完成后的 progress 不发送，checkpoint 拒绝。 |

## 验证命令

```sh
pnpm typecheck
pnpm format:check
pnpm lint
pnpm test
pnpm test:browser
pnpm test:stress
pnpm test:package
pnpm benchmark:scheduler
pnpm benchmark:cache
```

调度基准使用 1 个预热 Node Worker、8 个 interactive 组、一次性提交，每档 3 次。此次 500/1000/2000/8000 项批次总耗时中位数为 19.3/24.0/47.6/148.5 ms；最大定时器延迟分别为 1.9/1.4/2.2/12.3 ms。这不是单次连续主线程阻塞，也不能直接与其他脚本的 CPU profile 时间相除计算加速比。

缓存 key 构造每档 100 万次、预热后 3 次，此次 JSON tuple 中位数 69.0 ms，命名空间前缀 7.9 ms；不包含缓存查找、验证或驱逐开销。

## 发布边界

beta 使用 npm 的 beta dist-tag 与 GitHub prerelease。发布前验证最终内容，版本 tag 必须对应发布提交；发布后核对注册表版本、下载包与 GitHub Release。包内不携带原始基准 JSON。

资源预算控制协议数据、已声明暂存和常驻资源，不承诺 JS 堆、RSS、操作系统句柄或调用者外部引用的硬上限。相关边界是明确的执行合同，不是待实现的隐式内存沙箱。
