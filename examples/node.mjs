import assert from 'node:assert/strict';
import {
  consumeResult,
  createWorkerRuntime,
  packetByteLength,
  transferBuffers,
} from '../dist/index.js';
import { nodeWorker } from '../dist/adapters/node.js';

const runtime = createWorkerRuntime({
  pools: {
    compute: { factory: nodeWorker(new URL('./node-worker.mjs', import.meta.url)), size: 1 },
  },
});
try {
  await runtime.withScope('example', async (scope) => {
    // This small input already exists. For large inputs, declare an upper bound and
    // allocate inside synchronous prepare, after admission.
    const values = new Float64Array([1, 2, 3]);
    const payload = { values, scale: 2 };
    const handle = scope.enqueue('scale', {
      pool: 'compute',
      budget: {
        inputBytes: packetByteLength(payload), // Includes the object packet's metadata.
        scratchBytes: 0,
        outputBytes: values.byteLength, // The output is a root TypedArray.
      },
      prepare: () => ({ payload, transfer: transferBuffers(values) }),
    });
    await consumeResult(handle, (result) => {
      assert.deepEqual(Array.from(result), [2, 4, 6]);
      assert.equal(values.byteLength, 0);
      console.log('Converted:', Array.from(result));
    });
  });
} finally {
  await runtime.dispose();
}
assert.equal(runtime.stats.workers, 0);
assert.equal(runtime.stats.leases, 0);
assert.equal(runtime.stats.scopes, 0);
console.log('Released all workers, leases and scopes.');
