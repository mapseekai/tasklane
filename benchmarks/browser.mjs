import { chromium } from '@playwright/test';
import { mkdir, writeFile } from 'node:fs/promises';
import { platform, arch, cpus, totalmem } from 'node:os';
import { startServer } from '../scripts/server.mjs';
import { matrix, assertMatches } from './matrix.mjs';
const quick = process.argv.includes('--quick');
const repeats = quick ? 1 : 3;
const server = await startServer();
const browser = await chromium.launch({ channel: 'chrome', headless: true });
const rows = [];
const cases = matrix({ quick });
try {
  for (let repeat = 0; repeat < repeats; repeat++) {
    for (const config of [...cases.slice(repeat), ...cases.slice(0, repeat)]) {
      // Isolate case heaps and Worker ownership in a fresh context.
      const context = await browser.newContext();
      try {
        const page = await context.newPage();
        const errors = [];
        page.on('pageerror', (error) => errors.push(error.message));
        await page.goto('http://127.0.0.1:4196/test/browser/harness.html');
        const result = await page.evaluate(async (config) => {
          const { browserWorker } = await import('/dist/index.js');
          const { runCase } = await import('/benchmarks/runner.mjs');
          return runCase(
            config,
            browserWorker('/test/fixtures/browser-worker.mjs'),
            browserWorker('/benchmarks/raw-browser-worker.mjs'),
          );
        }, config);
        if (errors.length) throw new Error(errors.join('\n'));
        rows.push({ repeat, ...result });
        console.log(
          `${repeat + 1}/${repeats} ${config.workload} ${(result.inputBytes / 1024 ** 2).toFixed(1)}MiB ${config.mode}/${config.workers}: ${result.totalMs.toFixed(1)}ms ${result.throughputMiBs.toFixed(1)}MiB/s lag=${result.maxTimerLagMs.toFixed(1)}ms`,
        );
      } finally {
        await context.close();
      }
    }
  }
  assertMatches(rows);
  await mkdir('benchmark-results', { recursive: true });
  const path = `benchmark-results/browser${quick ? '-quick' : ''}.json`;
  await writeFile(
    path,
    JSON.stringify(
      {
        timestamp: new Date().toISOString(),
        environment: {
          browser: browser.version(),
          platform: platform(),
          arch: arch(),
          cpu: cpus()[0]?.model,
          logicalCpus: cpus().length,
          totalmem: totalmem(),
        },
        methodology:
          'Chrome headless, fresh context per case; warm kernels. Total includes deterministic generation and full checksums, excludes startup/disk I/O. Timer lag is an 8ms heartbeat, not FPS/INP. No browser RSS measured; managed budgets only. Synthetic input.',
        repeats,
        rows,
      },
      null,
      2,
    ),
  );
  console.log(`Verified all outputs. Saved ${path}`);
} finally {
  await browser.close();
  await new Promise((resolve) => server.close(resolve));
}
