import { mkdtemp, readdir, writeFile, rm } from 'node:fs/promises';
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
  await writeFile(
    join(directory, 'worker.mjs'),
    `
import { serve, output } from '@mapseekai/tasklane/host';
import { nodeHost } from '@mapseekai/tasklane/node';
serve(nodeHost(), { double(values) { const result = Float64Array.from(values, (n) => n * 2); return output(result, [result.buffer]); } });
`,
  );
  await writeFile(
    join(directory, 'smoke.mjs'),
    `
import assert from 'node:assert/strict';
import { createWorkerRuntime, transferBuffers } from '@mapseekai/tasklane';
import { nodeWorker } from '@mapseekai/tasklane/node';
const rt = createWorkerRuntime({ pools: { cpu: { factory: nodeWorker(new URL('./worker.mjs', import.meta.url)), size: 1 } } });
const values = new Float64Array([1, 2, 3]);
try {
 const lease = await rt.createScope().enqueue('double', { pool: 'cpu', budget: { inputBytes: 24, scratchBytes: 0, outputBytes: 24 }, prepare: () => ({ payload: values, transfer: transferBuffers(values) }) }).result;
 assert.deepEqual(Array.from(lease.value), [2, 4, 6]); assert.equal(values.byteLength, 0); lease.release();
} finally { await rt.dispose(); }
assert.equal(rt.stats.workers, 0);
`,
  );
  await execute(process.execPath, ['smoke.mjs'], { cwd: directory, timeout: 15000 });
  console.log('Installed tarball exports + real Node Worker: passed');
} finally {
  // Only the uniquely created temporary consumer directory is removed.
  await rm(directory, { recursive: true, force: true });
}
