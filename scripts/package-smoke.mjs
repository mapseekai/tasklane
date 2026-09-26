import { mkdtemp, readdir, writeFile, rm, readFile, access } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
const execute = promisify(execFile);
const directory = await mkdtemp(join(tmpdir(), 'tasklane-package-'));
const npm = process.platform === 'win32' ? 'npm.cmd' : 'npm';
const root = new URL('../', import.meta.url);
try {
  await execute(npm, ['pack', '--pack-destination', directory], { cwd: root, timeout: 60000 });
  const archive = (await readdir(directory)).find((name) => name.endsWith('.tgz'));
  if (!archive) throw new Error('Package archive missing');
  await writeFile(
    join(directory, 'package.json'),
    JSON.stringify({ name: 'tasklane-consumer', private: true, type: 'module' }),
  );
  await execute(
    npm,
    ['install', '--offline', '--ignore-scripts', '--no-audit', '--no-fund', `./${archive}`],
    { cwd: directory, timeout: 60000 },
  );
  const installed = join(directory, 'node_modules/@mapseekai/tasklane');
  const manifest = JSON.parse(await readFile(join(installed, 'package.json'), 'utf8'));
  if (manifest.publishConfig?.access !== 'public')
    throw new Error('Public access must be explicit');
  try {
    await access(join(installed, 'docs/results'));
    throw new Error('Raw benchmark results included in package');
  } catch (error) {
    if (error.code !== 'ENOENT') throw error;
  }
  await writeFile(
    join(directory, 'worker.mjs'),
    `
import { serve, output } from '@mapseekai/tasklane/host';
import { nodeHost } from '@mapseekai/tasklane/node';
serve(nodeHost(), { fail() { throw Object.assign(new Error('failed'), { name: 'DataError', code: 'DOMAIN_FAILURE' }); }, double(values) { const result = Float64Array.from(values, (n) => n * 2); return output(result, [result.buffer]); } });
`,
  );
  await writeFile(
    join(directory, 'smoke.mjs'),
    `
import assert from 'node:assert/strict';
import { createWorkerRuntime, transferBuffers, packetByteLength, dataByteLength, iterateResults, consumeResult, iterateSizedResults, transferOwnedBuffers } from '@mapseekai/tasklane';
assert.equal(packetByteLength('abcd'), 8);
assert.equal(dataByteLength('abcd'), 8);
import { nodeWorker } from '@mapseekai/tasklane/node';
const rt = createWorkerRuntime({ pools: { cpu: { factory: nodeWorker(new URL('./worker.mjs', import.meta.url)), size: 1 } } });
const values = new Float64Array([1, 2, 3]);
try {
 const lease = await rt.createScope().enqueue('double', { pool: 'cpu', budget: { inputBytes: 24, scratchBytes: 0, outputBytes: 24 }, prepare: () => ({ payload: values, transfer: transferBuffers(values) }) }).result;
 assert.deepEqual(Array.from(lease.value), [2, 4, 6]); assert.equal(values.byteLength, 0); lease.release();
 const scope = rt.createScope();
 const base = { pool: 'cpu', budget: { inputBytes: 24, scratchBytes: 0, outputBytes: 24 } };
 await consumeResult(scope.enqueuePrepared('double', { ...base, preparationScratchBytes: 24, prepareAsync: async () => ({ payload: new Float64Array([4]) }) }), (v) => assert.equal(v[0], 8));
 await assert.rejects(scope.enqueue('fail', { ...base, prepare: () => ({ payload: null }) }).result, (e) => e.code === 'REMOTE_ERROR' && e.remoteError.code === 'DOMAIN_FAILURE');
 let pulls = 0;
 const iterator = iterateResults({ next: () => scope.enqueue('double', { ...base, prepare: () => ({ payload: new Float64Array([pulls++]) }) }), isDone: (v) => v[0] >= 2, close: () => scope.dispose() });
 const chunks = [];
 for await (const chunk of iterator) chunks.push(chunk[0]);
 assert.deepEqual(chunks, [0]); await iterator.closed;
 let closes = 0;
 const cleanup = iterateResults({ next() { throw Error('unexpected pull'); }, isDone: () => false, close() { if (++closes === 1) throw Error('temporary close'); } });
 await assert.rejects(cleanup.dispose(), /temporary close/); await cleanup.retryCleanup();
 await assert.rejects(cleanup.closed, /temporary close/); assert.equal(closes, 2);
 const owner = rt.createScope('resident-smoke');
 const session = await owner.acquireSession('cpu', { mode: 'immediate', reclaimable: true, residentBytes: 8 });
 assert.equal(session.state, 'bound'); assert.equal(typeof iterateSizedResults, 'function');
 const input = new Float64Array([5]);
 const output = await session.enqueue('double', { budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 }, prepare: () => ({ payload: input, transfer: transferOwnedBuffers(input) }) }).result;
 const allocation = session.resident;
 assert.equal(allocation.bytes, 8);
 assert.equal(output.value[0], 10); output.release(); allocation.resize(16);
 assert.equal(rt.stats.reserved.residentBytes, 16);
 assert.equal(rt.diagnostics().pools[0].boundSessions, 1);
 const group = owner.sessionGroup([session]);
 await consumeResult(group.enqueue('double', { budget: { inputBytes: 8, scratchBytes: 0, outputBytes: 8 }, affinity: 'source/block', prepare: () => ({ payload: new Float64Array([6]) }) }), value => assert.equal(value[0], 12));
 assert.deepEqual((await rt.resizePool('cpu', { size: 1, cacheBytes: 0 })).failures, []);
 assert.deepEqual((await rt.trim({ workersPerPool: 1, reclaimSessions: false })).failures, []);
 const reclaimed = await rt.setMemoryPressure('critical');
 assert.equal(reclaimed.workersReclaimed, 1);
 assert.equal(rt.stats.reclaim.byReason.pressure, 1);
 await rt.setMemoryPressure('normal');
 await owner.dispose(); assert.equal(allocation.released, true);
 assert.equal(rt.stats.reserved.residentBytes, 0);

} finally { await rt.dispose(); }
assert.equal(rt.stats.workers, 0);
import { createLoopback } from '@mapseekai/tasklane/testing';
import { serve, output } from '@mapseekai/tasklane/host';
let release, begin;
const gate = new Promise((r) => { release = r; });
const started = new Promise((r) => { begin = r; });
const stops = [];
const factory = () => { const link = createLoopback(); stops.push(serve(link.host, { async hold() { begin(); await gate; return output(null); }, ping() { return output(null); } })); return link.endpoint; };
const scheduled = createWorkerRuntime({ pools: { busy: { factory, size: 1 }, idle: { factory, size: 1 } }, budgets: { scratchBytes: 100 }, budgetWaitMs: 1 });
const owner = scheduled.createScope();
const options = (pool, scratchBytes) => ({ pool, budget: { inputBytes: 0, scratchBytes, outputBytes: 0 }, prepare: () => ({ payload: null }) });
try {
 const running = owner.enqueue('hold', options('busy', 60)); await started;
 const large = owner.enqueuePrepared('ping', { ...options('busy', 80), preparationScratchBytes: 80, prepareAsync: async () => ({ payload: null }) });
 await new Promise((r) => setTimeout(r, 10));
 const small = owner.enqueue('ping', options('idle', 20));
 for (let i = 0; i < 100 && small.state !== 'succeeded'; i++) await new Promise((r) => setTimeout(r, 2));
 assert.equal(small.state, 'succeeded'); await consumeResult(small, () => {});
 assert.equal(running.state, 'running'); large.cancel(); await large.settled;
 release(); await consumeResult(running, () => {});
} finally { release(); await scheduled.dispose(); stops.forEach((stop) => stop()); }
assert.equal(scheduled.stats.reserved.scratchBytes, 0);
`,
  );
  await execute(process.execPath, ['smoke.mjs'], { cwd: directory, timeout: 15000 });
  console.log('Installed tarball exports + real Node Worker: passed');
} finally {
  // Only the uniquely created temporary consumer directory is removed.
  await rm(directory, { recursive: true, force: true });
}
