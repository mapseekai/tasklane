import { createWorkerRuntime, consumeResult } from '../dist/index.js';
import { nodeWorker } from '../dist/adapters/node.js';
const factory = nodeWorker(new URL('../test/fixtures/node-worker.mjs', import.meta.url));
const traffic = 100;
for (const count of [100, 1000, 4000]) {
  const runtime = createWorkerRuntime({
    pools: {
      blocked: { factory, size: 1, idleTimeoutMs: 0 },
      free: { factory, size: 1, idleTimeoutMs: 0 },
    },
    maxQueuedTasks: count + traffic,
  });
  const scope = runtime.createScope();
  const options = (pool, group) => ({
    pool,
    group,
    budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 4096 },
    prepare: () => ({ payload: 1 }),
  });
  try {
    await scope.acquireSession('blocked');
    await consumeResult(scope.enqueue('ping', options('free')), () => {});
    const waiting = Array.from({ length: count }, (_, i) =>
      scope.enqueue('ping', options('blocked', String(i))),
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    const before = runtime.stats.scheduler.eligibilityChecks,
      start = performance.now();
    for (let i = 0; i < traffic; i++)
      await consumeResult(scope.enqueue('ping', options('free')), () => {});
    console.log(
      JSON.stringify({
        blockedBuckets: runtime.stats.scheduler.blockedBuckets,
        traffic,
        eligibilityChecks: runtime.stats.scheduler.eligibilityChecks - before,
        ms: performance.now() - start,
      }),
    );
    waiting.forEach((handle) => handle.cancel());
    await Promise.all(waiting.map((handle) => handle.settled));
  } finally {
    await runtime.dispose();
  }
}
