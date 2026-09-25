import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { assertMatches } from '../benchmarks/matrix.mjs';

const MiB = 1024 ** 2;
const median = (values) => [...values].sort((a, b) => a - b)[Math.floor(values.length / 2)];
const documents = {};

for (const name of ['node', 'browser', 'stress']) {
  documents[name] = JSON.parse(await readFile(`benchmark-results/${name}.json`, 'utf8'));
}

assertMatches(documents.node.rows);
assertMatches(documents.browser.rows);

await mkdir('docs/results', { recursive: true });
for (const name of ['node', 'browser', 'stress']) {
  await writeFile(`docs/results/${name}.json`, `${JSON.stringify(documents[name], null, 2)}\n`);
}

const summarize = (document, workload, inputMiB, mode, workers) => {
  const rows = document.rows.filter(
    (row) =>
      row.workload === workload &&
      Math.abs(row.inputBytes / MiB - inputMiB) < 0.1 &&
      row.mode === mode &&
      row.workers === workers,
  );
  return {
    samples: rows.length,
    totalMs: median(rows.map((row) => row.totalMs)),
    throughputMiBs: median(rows.map((row) => row.throughputMiBs)),
    maxTimerLagMs: median(rows.map((row) => row.maxTimerLagMs)),
  };
};

console.log('Benchmark result snapshots updated (Markdown and verification.json are unchanged):');
console.log('  docs/results/node.json');
console.log('  docs/results/browser.json');
console.log('  docs/results/stress.json');
console.log('\nSelected Chrome summary:');
console.log(
  JSON.stringify(
    {
      project256MiB2Workers: summarize(documents.browser, 'project', 256, 'runtime-transfer', 2),
      geojson2Workers: summarize(documents.browser, 'geojson', 55.5, 'runtime-transfer', 2),
    },
    null,
    2,
  ),
);
