import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { nodeWorker } from '../dist/adapters/node.js';
const factory = nodeWorker(new URL('../test/fixtures/node-worker.mjs', import.meta.url));
const options = (i) => ({
  pool: 'cpu',
  group: `g${i % 8}`,
  priority: 'interactive',
  budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
  prepare: () => ({ payload: i }),
});
for (const count of [500, 1000, 2000, 8000]) {
  const samples = [];
  for (let run = 0; run < 3; run++) {
    const rt = createWorkerRuntime({
      pools: { cpu: { factory, size: 1, idleTimeoutMs: 0 } },
      maxQueuedTasks: count,
    });
    try {
      const scope = rt.createScope();
      await consumeResult(scope.enqueue('ping', options(0)), () => {});
      let last = performance.now(),
        maxDelay = 0;
      const timer = setInterval(() => {
        const now = performance.now();
        maxDelay = Math.max(maxDelay, now - last - 1);
        last = now;
      }, 1);
      const start = performance.now();
      try {
        await Promise.all(
          Array.from({ length: count }, (_, i) =>
            consumeResult(scope.enqueue('ping', options(i)), () => {}),
          ),
        );
      } finally {
        clearInterval(timer);
      }
      samples.push({ ms: performance.now() - start, maxTimerDelayMs: maxDelay });
    } finally {
      await rt.dispose();
    }
  }
  samples.sort((a, b) => a.ms - b.ms);
  console.log(
    JSON.stringify({
      count,
      medianMs: samples[1].ms,
      maxTimerDelayMs: Math.max(...samples.map((s) => s.maxTimerDelayMs)),
    }),
  );
}
