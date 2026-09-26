import assert from 'node:assert/strict';
import { cpus } from 'node:os';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import * as current from '../dist/packet.js';

// Optional baseline module must retain its relative imports beside it.
const implementations = process.argv[2]
  ? [
      ['baseline', await import(pathToFileURL(resolve(process.argv[2])).href)],
      ['current', current],
    ]
  : [['current', current]];
const warmups = 3;
const samples = 11;
const medianMs = (fn) => {
  for (let i = 0; i < warmups; i++) fn();
  const times = [];
  for (let i = 0; i < samples; i++) {
    const start = performance.now();
    fn();
    times.push(performance.now() - start);
  }
  times.sort((a, b) => a - b);
  return Number(times[Math.floor(times.length / 2)].toFixed(3));
};
const numbers = Array.from({ length: 100_000 }, (_, i) => i);
const pairs = Array.from({ length: 50_000 }, (_, i) => [i, i]);
const typed = Float64Array.from(numbers);
const results = [];
for (const [input, value] of [
  ['100k numbers', numbers],
  ['50k coordinate pairs', pairs],
  ['100k Float64 elements', typed],
  ['record with 100k Float64 elements', { op: 'test', xy: typed }],
]) {
  for (const [implementation, codec] of implementations) {
    const packet = codec.encodePacket(value);
    assert.deepEqual(codec.decodePacket(structuredClone(packet)), value);
    results.push({
      input,
      implementation,
      encodeMs: medianMs(() => codec.encodePacket(value)),
      decodeMs: medianMs(() => codec.decodePacket(packet)),
      protocolBytes: codec.packetBytes(packet),
    });
  }
}
console.log(
  JSON.stringify(
    {
      node: process.version,
      cpu: cpus()[0]?.model,
      warmups,
      samples,
      statistic: 'median',
      scope: 'codec only; excludes input construction and cross-thread transport',
      results,
    },
    null,
    2,
  ),
);
