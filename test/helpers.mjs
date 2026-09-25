import { createWorkerRuntime } from '../dist/index.js';
import { nodeWorker } from '../dist/adapters/node.js';
export const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));
export function options(payload, extra = {}) {
  return {
    pool: 'cpu',
    budget: { inputBytes: 4096, scratchBytes: 0, outputBytes: 4096 },
    prepare: () => ({ payload }),
    ...extra,
  };
}
export function runtime(extra = {}) {
  return createWorkerRuntime({
    pools: {
      cpu: {
        factory: nodeWorker(new URL('./fixtures/node-worker.mjs', import.meta.url)),
        size: 2,
        cacheBytes: 1024,
        allowHardCancel: true,
        idleTimeoutMs: 0,
      },
    },
    maxActiveTasks: 2,
    startupTimeoutMs: 3000,
    executionTimeoutMs: 3000,
    ...extra,
  });
}
export async function take(handle) {
  const lease = await handle.result;
  try {
    return lease.value;
  } finally {
    lease.release();
  }
}
export async function until(predicate, limit = 2000) {
  const started = performance.now();
  while (!predicate()) {
    if (performance.now() - started > limit) throw new Error('Condition timed out');
    await sleep(2);
  }
}
