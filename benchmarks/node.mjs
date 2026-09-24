import { cpus, totalmem, platform, arch, release } from 'node:os';
import { mkdir, writeFile } from 'node:fs/promises';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { fileURLToPath } from 'node:url';
import { matrix, assertMatches } from './matrix.mjs';
const execute = promisify(execFile);
const quick = process.argv.includes('--quick');
const repeats = quick ? 1 : 3;
const rows = [];
const cases = matrix({ quick });
for (let repeat = 0; repeat < repeats; repeat++) {
  for (const config of [...cases.slice(repeat), ...cases.slice(0, repeat)]) {
    // Each case gets a clean process. Allocator high-water marks from prior cases
    // must not masquerade as this case's RSS requirement.
    const { stdout } = await execute(
      process.execPath,
      [
        '--expose-gc',
        fileURLToPath(new URL('./node-case.mjs', import.meta.url)),
        JSON.stringify(config),
      ],
      { timeout: 120000, maxBuffer: 1024 ** 2 },
    );
    const result = JSON.parse(stdout);
    rows.push({ repeat, ...result });
    console.log(
      `${repeat + 1}/${repeats} ${config.workload} ${(result.inputBytes / 1024 ** 2).toFixed(1)}MiB ${config.mode}/${config.workers}: ${result.totalMs.toFixed(1)}ms ${result.throughputMiBs.toFixed(1)}MiB/s lag=${result.maxTimerLagMs.toFixed(1)}ms`,
    );
  }
}
assertMatches(rows);
await mkdir('benchmark-results', { recursive: true });
const path = `benchmark-results/node${quick ? '-quick' : ''}.json`;
await writeFile(
  path,
  JSON.stringify(
    {
      timestamp: new Date().toISOString(),
      environment: {
        node: process.version,
        platform: platform(),
        arch: arch(),
        osRelease: release(),
        cpu: cpus()[0]?.model,
        logicalCpus: cpus().length,
        totalmem: totalmem(),
      },
      methodology:
        'Fresh process per case, three full-chunk warmups per execution context. Total includes generation and all binary checksums, excludes startup/warmup/disk I/O. RSS is process-wide sampled lower-bound peak including Worker threads. Synthetic data; all output hashes matched.',
      repeats,
      rows,
    },
    null,
    2,
  ),
);
console.log(`Verified all outputs. Saved ${path}`);
