// Local CacheStore admission cost; this does not measure COG decoding or end-to-end rendering.
import assert from 'node:assert/strict';
import { CacheStore } from '../dist/resources/cache.js';

const value = new Float32Array(4_000_000);
const store = new CacheStore(value.byteLength);
const cache = store.scope('benchmark');
for (const [name, repetitions, put] of [
  ['set', 1, () => cache.set('raster', value, value.byteLength)],
  ['setBinary', 1000, () => cache.setBinary('raster', value)],
]) {
  const samples = [];
  for (let run = 0; run < 4; run++) {
    const start = performance.now();
    for (let i = 0; i < repetitions; i++) put();
    if (run) samples.push((performance.now() - start) / repetitions);
  }
  samples.sort((a, b) => a - b);
  assert.equal(cache.get('raster').buffer, value.buffer);
  assert.equal(store.bytes, value.byteLength);
  console.log(JSON.stringify({ name, bytes: value.byteLength, medianMsPerAdmission: samples[1] }));
}
await store.release();
