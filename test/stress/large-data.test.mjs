import assert from 'node:assert/strict';
import test from 'node:test';
import { writeFile, mkdir } from 'node:fs/promises';
import { runCase } from '../../benchmarks/runner.mjs';
import { nodeWorker } from '../../dist/adapters/node.js';
import { createWorkerRuntime, transferBuffers } from '../../dist/index.js';
import { coordinates, checksum } from '../../benchmarks/workloads.mjs';
import { options, take, runtime } from '../helpers.mjs';
const MiB = 1024 ** 2;
const factory = nodeWorker(new URL('../fixtures/node-worker.mjs', import.meta.url));
const rows = [];

test('1 GiB logical stream: full checksum parity with bounded 16 MiB packets', {
  timeout: 120000,
}, async () => {
  const config = { workload: 'project', inputMiB: 1024, chunkMiB: 16, workers: 2 };
  const main = await runCase(
    { ...config, mode: 'cooperative-main' },
    factory,
    null,
    () => process.memoryUsage().rss,
  );
  const worker = await runCase(
    { ...config, mode: 'runtime-transfer', slowConsumerMs: 2 },
    factory,
    null,
    () => process.memoryUsage().rss,
  );
  assert.deepEqual(worker.hashes, main.hashes);
  assert.equal(worker.count, 67108864);
  assert.equal(worker.inputBytes, 1024 * MiB);
  assert.ok(worker.runtimeStats.peakReserved.inputBytes <= 32 * MiB);
  assert.ok(worker.runtimeStats.peakReserved.outputBytes <= 32 * MiB + 64);
  rows.push(main, worker);
});
test('single 256 MiB owned ArrayBuffer conversion, no truncation', { timeout: 60000 }, async () => {
  const rt = createWorkerRuntime({
    pools: { cpu: { factory, size: 1 } },
    budgets: { inputBytes: 256 * MiB, outputBytes: 256 * MiB + 32 },
    executionTimeoutMs: 60000,
  });
  try {
    const xy = coordinates((256 * MiB) / 16);
    const expected = checksum(new Uint32Array(xy.buffer));
    const start = performance.now();
    const scope = rt.createScope();
    const lease = await scope.enqueue('convert', {
      pool: 'cpu',
      budget: { inputBytes: 256 * MiB, scratchBytes: 0, outputBytes: 256 * MiB + 32 },
      prepare: () => ({ payload: { xy, operation: 'layout' }, transfer: transferBuffers(xy) }),
    }).result;
    assert.equal(xy.byteLength, 0);
    assert.equal(lease.value.count, 16777216);
    // Inspect all reconstructed coordinates against deterministic source values.
    const data = lease.value.vertices;
    for (let i = 0; i < lease.value.count; i++) {
      const x = ((i * 16807) % 3600000) / 10000 - 180,
        y = ((i * 48271) % 1600000) / 10000 - 80;
      if (
        Math.abs(data[i * 4] + data[i * 4 + 2] - x) > 1e-10 ||
        Math.abs(data[i * 4 + 1] + data[i * 4 + 3] - y) > 1e-10
      )
        assert.fail(`Mismatch at ${i}`);
    }
    rows.push({
      test: 'single-256MiB-array',
      totalWithFullValidationMs: performance.now() - start,
      originalChecksum: expected,
      count: lease.value.count,
      stats: rt.stats,
    });
    lease.release();
  } finally {
    await rt.dispose();
  }
  assert.equal(rt.stats.workers, 0);
  assert.equal(rt.stats.active, 0);
});
test('1000 request cancellation storm preserves all uncancelled identities', {
  timeout: 20000,
}, async () => {
  const rt = runtime();
  try {
    const scope = rt.createScope();
    const tasks = Array.from({ length: 1000 }, (_, i) => scope.enqueue('ping', options(i)));
    for (let i = 0; i < tasks.length; i++) if (i % 3 !== 0) tasks[i].cancel();
    const values = await Promise.all(
      tasks.map(async (task, i) => {
        if (i % 3 !== 0) {
          await assert.rejects(task.result);
          return null;
        }
        return (await take(task)).value;
      }),
    );
    assert.deepEqual(
      values.filter((v) => v !== null),
      Array.from({ length: 334 }, (_, i) => i * 3),
    );
    await Promise.all(tasks.map((task) => task.settled));
    assert.equal(rt.stats.active, 0);
    assert.equal(rt.stats.queued, 0);
    assert.equal(rt.stats.leases, 0);
    rows.push({ test: '1000-request-cancel-storm', stats: rt.stats });
  } finally {
    await rt.dispose();
  }
});
test.after(async () => {
  await mkdir('benchmark-results', { recursive: true });
  await writeFile(
    'benchmark-results/stress.json',
    JSON.stringify(
      {
        timestamp: new Date().toISOString(),
        note: 'Synthetic streamed 1 GiB input is not a single-file importer benchmark. Managed reservation limits are not process RSS limits.',
        rows,
      },
      null,
      2,
    ),
  );
});

test('single ~55 MiB UTF-8 GeoJSON: 100000 features and all 1.6 million vertices', {
  timeout: 60000,
}, async () => {
  const { geojson } = await import('../../benchmarks/workloads.mjs');
  const bytes = geojson(100000);
  const inputBytes = bytes.byteLength;
  const rt = createWorkerRuntime({
    pools: { cpu: { factory, size: 1 } },
    budgets: { inputBytes: 64 * MiB, scratchBytes: 512 * MiB, outputBytes: 64 * MiB },
    executionTimeoutMs: 60000,
  });
  try {
    const start = performance.now();
    const scope = rt.createScope();
    const lease = await scope.enqueue('flatten', {
      pool: 'cpu',
      budget: { inputBytes, scratchBytes: 512 * MiB, outputBytes: 64 * MiB },
      prepare: () => ({ payload: { bytes }, transfer: transferBuffers(bytes) }),
    }).result;
    assert.equal(bytes.byteLength, 0);
    assert.equal(lease.value.count, 1600000);
    assert.deepEqual(lease.value.xy, coordinates(1600000));
    assert.equal(lease.value.featureOffsets.length, 100001);
    rows.push({
      test: 'single-geojson',
      inputBytes,
      featureCount: 100000,
      pointCount: lease.value.count,
      totalWithValidationMs: performance.now() - start,
      stats: rt.stats,
    });
    lease.release();
  } finally {
    await rt.dispose();
  }
});
