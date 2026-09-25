import { browserWorker, createWorkerRuntime, transferBuffers } from '../dist/index.js';
import { coordinates, checksum } from '../benchmarks/workloads.mjs';
const MiB = 1024 ** 2;
const start = document.querySelector('#start'),
  cancel = document.querySelector('#cancel');
const status = document.querySelector('#status'),
  progress = document.querySelector('#progress');
let controller;
let heartbeats = 0;
setInterval(() => {
  document.querySelector('#heartbeat').textContent =
    `页面心跳 ${++heartbeats} · 主线程仍需承担输入准备与结果消费`;
}, 100);
cancel.addEventListener('click', () => controller?.abort());
start.addEventListener('click', async () => {
  start.disabled = true;
  cancel.disabled = false;
  progress.value = 0;
  controller = new AbortController();
  const workers = Number(document.querySelector('#workers').value),
    totalMiB = Number(document.querySelector('#size').value);
  // Fixed small metadata schema: xy, operation and result bounds/count.
  // The allowance is specific to this demo, not a universal object-graph estimate.
  const metadataBytes = 1024;
  const chunkBytes = 4 * MiB,
    chunks = totalMiB / 4;
  const runtime = createWorkerRuntime({
    pools: {
      cpu: {
        factory: browserWorker(new URL('./worker.mjs', import.meta.url)),
        size: workers,
        allowHardCancel: true,
      },
    },
    maxActiveTasks: workers,
    budgets: {
      inputBytes: (chunkBytes + metadataBytes) * workers,
      outputBytes: (chunkBytes + metadataBytes) * workers,
    },
  });
  const scope = runtime.createScope('demo');
  let next = 0,
    completed = 0,
    fingerprint = '';
  const begin = performance.now();
  try {
    await Promise.all(
      Array.from({ length: workers }, async () => {
        while (next < chunks && !controller.signal.aborted) {
          const chunk = next++;
          const lease = await scope.enqueue('convert', {
            pool: 'cpu',
            cancellation: 'terminate',
            signal: controller.signal,
            budget: {
              inputBytes: chunkBytes + metadataBytes,
              scratchBytes: 0,
              outputBytes: chunkBytes + metadataBytes,
            },
            prepare: () => {
              const xy = coordinates(chunkBytes / 16, (chunk * chunkBytes) / 16);
              return { payload: { xy }, transfer: transferBuffers(xy) };
            },
          }).result;
          try {
            fingerprint = checksum(lease.value.vertices);
            completed++;
          } finally {
            lease.release();
          }
          progress.value = (completed / chunks) * 100;
          status.textContent = JSON.stringify(
            {
              completedMiB: completed * 4,
              totalMiB,
              elapsedMs: Math.round(performance.now() - begin),
              lastChunkChecksum: fingerprint,
              ...runtime.stats,
            },
            null,
            2,
          );
        }
      }),
    );
    status.textContent = `完成：${completed * 4} MiB，${Math.round(performance.now() - begin)} ms\n${status.textContent}`;
  } catch (error) {
    status.textContent = `${error.code || error.name}: ${error.message}\n已消费 ${completed * 4} MiB`;
  } finally {
    await runtime.dispose();
    status.textContent += `\n已释放 Worker：${runtime.stats.workers}；活跃任务：${runtime.stats.active}`;
    start.disabled = false;
    cancel.disabled = true;
  }
});
