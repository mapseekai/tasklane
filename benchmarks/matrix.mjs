export function matrix({ quick = false } = {}) {
  const variants = [
    { mode: 'main', workers: 1 },
    { mode: 'cooperative-main', workers: 1 },
    { mode: 'runtime-clone', workers: 1 },
    { mode: 'runtime-transfer', workers: 1 },
    { mode: 'runtime-transfer', workers: 2 },
    { mode: 'runtime-transfer', workers: 4 },
    { mode: 'direct-transfer', workers: 2 },
  ];
  const workloads = quick
    ? [{ workload: 'project', inputMiB: 16, chunkMiB: 4 }]
    : [
        { workload: 'layout', inputMiB: 64, chunkMiB: 8 },
        { workload: 'project', inputMiB: 64, chunkMiB: 8 },
        { workload: 'project', inputMiB: 256, chunkMiB: 8 },
        { workload: 'geojson', inputMiB: 64, chunkMiB: 8, features: 100000, featureChunk: 4096 },
      ];
  return workloads.flatMap((workload) => variants.map((variant) => ({ ...workload, ...variant })));
}
export function groupKey(config) {
  return `${config.workload}/${config.inputMiB}/${config.chunkMiB}/${config.features || 0}`;
}
export function assertMatches(rows) {
  const baselines = new Map();
  for (const row of rows) {
    const key = groupKey(row),
      fingerprint = JSON.stringify([row.count, row.inputBytes, row.outputBytes, row.hashes]);
    if (baselines.has(key) && baselines.get(key) !== fingerprint)
      throw new Error(`Output mismatch: ${key}/${row.mode}/${row.workers}`);
    baselines.set(key, fingerprint);
  }
}
