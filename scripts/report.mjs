import { readFile, writeFile, mkdir, copyFile } from 'node:fs/promises';
import { assertMatches } from '../benchmarks/matrix.mjs';
const MiB = 1024 ** 2;
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const fmt = (n) => n.toFixed(1);
const documents = {};
for (const name of ['node', 'browser', 'stress'])
  documents[name] = JSON.parse(await readFile(`benchmark-results/${name}.json`, 'utf8'));
assertMatches(documents.node.rows);
assertMatches(documents.browser.rows);
await mkdir('docs/results', { recursive: true });
for (const name of ['node', 'browser', 'stress'])
  await copyFile(`benchmark-results/${name}.json`, `docs/results/${name}.json`);
const names = {
  main: '同步主线程',
  'cooperative-main': '协作式主线程',
  'runtime-clone': 'Runtime / clone',
  'runtime-transfer': 'Runtime / transfer',
  'direct-transfer': '裸 Worker / transfer',
};
let text = `# 大数据转换性能实测\n\n验收日期：2026-09-24。CPU ${documents.node.environment.cpu}，${documents.node.environment.logicalCpus} 逻辑处理器，${documents.node.environment.totalmem / 1024 ** 3} GiB 内存，${documents.node.environment.platform}/${documents.node.environment.arch}。Node ${documents.node.environment.node}；Chrome ${documents.browser.environment.browser}。\n\n## 测量口径\n\n每种环境 28 个配置，每配置重复 3 次，共 84 次；Node 与 Chrome 合计 168 次计时。表中时间/吞吐量为三次中位数，范围为最小值—最大值。没有删掉不利样本，也没有把多线程累计 workerMs 当作墙钟时间。\n\nNode 每个配置/重复都使用独立子进程；Chrome 每个配置/重复使用新的浏览器上下文。每个执行环境先预热 3 个完整计算块，启动与预热分别记录，不纳入下表总耗时。不同重复确定性轮换配置顺序；没有 CPU 固频或专用隔离机器，因此小差异不构成统计显著结论。\n\n总耗时包括确定性输入生成、实际转换、Worker 通信、调度和完整结果指纹消费，不包括磁盘/网络 I/O、启动、预热或 GPU 上传。吞吐量分子是输入字节，不是输入输出相加。它不能直接当作真实文件读取速度或纯算法极限。\n\n心跳滞后：8ms 定时器在每次运行中的最大延迟，再取三次运行的中位数；不是 INP、rAF 帧率、鼠标响应或 GPU 时间。主线程生成输入和验证输出仍会形成阻塞，多 Worker 不等于主线程零工作。\n\nNode RSS 是计时阶段采样到的进程总驻留下界，包含同进程 Worker，不是 JS 堆，也不包含启动/预热峰值。GC 和采样可能漏掉瞬时峰值；Chrome 未测 RSS，不能把未测值说成零。运行时预留字节另见原始 JSON，与 RSS 不可等同。\n\n## 工作负载与正确性\n\n- layout：Float64 XY → Float32 高低位 XY；不进行坐标简化。\n- project：经纬度 → Web Mercator → Float32 高低位，纬度输入严格在支持域内。\n- geojson：100000 个 LineString、1600000 个顶点，UTF-8 累计输入约 55.5 MiB，按 4096 个要素组成独立 FeatureCollection 计算块。只测几何平坦化，不输出业务属性表，不冒充完整无损 GIS 文件转换器。\n\n普通矩阵数值块为 8 MiB；所有模式使用同一算法、同一输入/输出数量和相同块边界。每个输出数组全部字节参与非密码学指纹（含尾字节），同一引擎内全部配置与重复均匹配。独立数值参考、百万点逐项误差检查、256 MiB 单包逐点检查及面孔洞/多面成员边界另有测试，不只依赖一个弱校验值。\n\n`;
for (const env of ['browser', 'node']) {
  text += `## ${env === 'browser' ? 'Chrome 浏览器' : 'Node.js'}完整矩阵\n\n`;
  const groups = new Map();
  for (const row of documents[env].rows) {
    const key = `${row.workload}/${row.inputBytes}`;
    if (!groups.has(key)) groups.set(key, new Map());
    const variants = groups.get(key),
      variant = `${row.mode}/${row.workers}`;
    if (!variants.has(variant)) variants.set(variant, []);
    variants.get(variant).push(row);
  }
  for (const [key, variants] of groups) {
    const first = [...variants.values()][0][0];
    text += `### ${first.workload} · ${fmt(first.inputBytes / MiB)} MiB 输入\n\n`;
    text +=
      '| 执行方式 | Worker | 总耗时 ms | 最小—最大 ms | 输入 MiB/s | 心跳滞后 ms |' +
      (env === 'node' ? ' 采样 RSS 峰值 MiB |' : '') +
      '\n';
    text += '| --- | ---: | ---: | ---: | ---: | ---: |' + (env === 'node' ? ' ---: |' : '') + '\n';
    for (const rows of variants.values()) {
      if (rows.length !== 3) throw new Error(`Expected 3 measured samples for ${key}`);
      const row = rows[0],
        times = rows.map((r) => r.totalMs);
      text += `| ${names[row.mode]} | ${row.mode.includes('main') ? '0' : row.workers} | ${fmt(median(times))} | ${fmt(Math.min(...times))}—${fmt(Math.max(...times))} | ${fmt(median(rows.map((r) => r.throughputMiBs)))} | ${fmt(median(rows.map((r) => r.maxTimerLagMs)))} |`;
      if (env === 'node') text += ` ${fmt(median(rows.map((r) => r.peakMemoryBytes / MiB)))} |`;
      text += '\n';
    }
    text += '\n';
  }
}
const stress = documents.stress.rows.find((row) => row.mode === 'runtime-transfer');
text += `## 大数据与资源压力\n\n1 GiB **逻辑分块流**使用 64 个 16 MiB 块，共 67108864 点；双 Worker 加 2ms 消费者等待，完整指纹与协作式主线程匹配。该次运行总耗时 ${fmt(stress.totalMs)} ms，输入吞吐量 ${fmt(stress.throughputMiBs)} MiB/s，输入预留峰值 ${stress.runtimeStats.peakReserved.inputBytes} 字节、结果预留峰值 ${stress.runtimeStats.peakReserved.outputBytes} 字节；采样 RSS 峰值 ${fmt(stress.peakMemoryBytes / MiB)} MiB。该压力用例为单次运行，不能当作三次中位数；同一压力测试进程还执行参考转换，RSS 不宜与独立进程矩阵横比。\n\n另有单个 256 MiB ArrayBuffer 的转移/布局转换（16777216 点）及逐点校验、单个约 55.5 MiB GeoJSON 的 100000 要素/1600000 顶点全数组校验，以及 1000 请求取消风暴。它们是功能/资源压力验收，不是 256 MiB 真实 GIS 文件导入或 GPU 渲染测试。\n\n## 结论和推荐配置\n\n本机双 Worker 是吞吐与主线程负担之间较合适的起点。四 Worker 在部分配置继续改善总时间，但心跳滞后和资源占用也可能增加；不能把四线程当成默认必胜配置。单 Worker 有时与同步主线程接近甚至更慢，主要价值也可能是响应性而非缩短墙钟时间。\n\n裸 Worker 对照使用相同二进制内核、块大小、Transferable 和消费校验，但没有 Runtime 的资源准入/作用域/会话等保障。二者差距应连同三次样本波动判断，不能把负差或几毫秒噪声宣传成调度器让算法变快。\n\nclone 与 transfer 都保留结果校验；transfer 避免显式所有权数据的结构化克隆副本，但不消除输入包生成、算法输出分配、缓存副本或未来 CPU→GPU 上传。由于原始模型不能随便 detach，emap 集成仍需自有包准备。\n\n下一步集成应先对齐相同数据、画质、过滤与缓存策略，再独立测 prepare、worker、待上传积压、GPU 上传及真实交互。当前结果不能外推为 luma/WebGL2/WebGPU 渲染已经变快。\n\n## 复现与原始证据\n\n\`pnpm benchmark\`、\`pnpm benchmark:browser\` 和 \`pnpm test:stress\` 重建输出到 benchmark-results。\`node scripts/report.mjs\` 根据已完成的完整矩阵更新本报告与 docs/results 快照。原始每次计时、输入/输出数量、完整块指纹、启动、预热、生成、消费、累计 Worker 时间、预留高水位保留在 [node.json](results/node.json)、[browser.json](results/browser.json)、[stress.json](results/stress.json)。\n`;
await writeFile('docs/performance.md', text);
console.log('Wrote measured report and source JSON snapshots');
