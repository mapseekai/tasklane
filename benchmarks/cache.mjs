// Isolated cache namespace-key construction cost, not end-to-end cache throughput.
const count = 1_000_000;
const keys = Array.from({ length: 1024 }, (_, i) => `tile-${i}`);
let checksum = 0;
for (const [name, make] of [
  ['json-tuple', (key) => JSON.stringify(['scope-123', 'session-456', key])],
  ['namespace-prefix', (key) => '17:' + key],
]) {
  const samples = [];
  for (let run = 0; run < 4; run++) {
    const start = performance.now();
    for (let i = 0; i < count; i++) checksum += make(keys[i % keys.length]).length;
    if (run) samples.push(performance.now() - start);
  }
  samples.sort((a, b) => a - b);
  console.log(JSON.stringify({ name, operations: count, medianMs: samples[1] }));
}
if (!checksum) throw Error('Benchmark work was not observed');
