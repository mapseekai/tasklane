import assert from 'node:assert/strict';
import test from 'node:test';
import { coordinates, convert, flatten } from '../benchmarks/workloads.mjs';
import { runtime, options, take } from './helpers.mjs';
import { transferBuffers } from '../dist/index.js';

for (const operation of ['layout', 'project']) {
  test(`one million points match an independent numeric reference: ${operation}`, async (t) => {
    const rt = runtime();
    t.after(() => rt.dispose());
    const xy = coordinates(1000000);
    const source = xy.slice();
    const result = await take(
      rt.createScope().enqueue(
        'convert',
        options(null, {
          budget: { inputBytes: xy.byteLength, scratchBytes: 0, outputBytes: xy.byteLength + 32 },
          prepare: () => ({ payload: { xy, operation }, transfer: transferBuffers(xy) }),
        }),
      ),
    );
    assert.equal(result.count, source.length / 2);
    for (let p = 0; p < result.count; p++) {
      const lon = source[p * 2],
        lat = source[p * 2 + 1];
      const x = operation === 'layout' ? lon : (6378137 * lon * Math.PI) / 180;
      const sin = Math.sin((lat * Math.PI) / 180);
      const y = operation === 'layout' ? lat : (6378137 * Math.log((1 + sin) / (1 - sin))) / 2;
      const j = p * 4;
      if (
        Math.abs(result.vertices[j] + result.vertices[j + 2] - x) > 0.000001 ||
        Math.abs(result.vertices[j + 1] + result.vertices[j + 3] - y) > 0.000001
      )
        assert.fail(`Coordinate mismatch at ${p}`);
    }
  });
}
for (const xy of [
  new Float64Array([1]),
  new Float64Array([NaN, 2]),
  new Float64Array([1, Infinity]),
  new Float64Array([1, 90]),
]) {
  test(`invalid coordinate input rejected: ${Array.from(xy)}`, () =>
    assert.throws(() => convert({ xy })));
}
test('empty numeric input remains empty', () =>
  assert.equal(convert({ xy: new Float64Array() }).count, 0));
test('GeoJSON flatten preserves holes, multipolygon boundaries and feature ordering', () => {
  const ring = [
      [0, 0],
      [10, 0],
      [10, 10],
      [0, 0],
    ],
    hole = [
      [1, 1],
      [2, 1],
      [1, 2],
      [1, 1],
    ];
  const geometries = [
    { type: 'Polygon', coordinates: [ring, hole] },
    { type: 'MultiPolygon', coordinates: [[ring, hole], [ring]] },
    { type: 'Point', coordinates: [5, 6] },
  ];
  const bytes = new TextEncoder().encode(
    JSON.stringify({
      type: 'FeatureCollection',
      features: geometries.map((geometry) => ({ type: 'Feature', properties: {}, geometry })),
    }),
  );
  const result = flatten({ bytes });
  assert.deepEqual(Array.from(result.featureOffsets), [0, 2, 5, 6]);
  assert.deepEqual(Array.from(result.polygonOffsets), [0, 2, 4, 5, 6]);
  assert.deepEqual(Array.from(result.featurePolygonOffsets), [0, 1, 3, 4]);
  assert.deepEqual(Array.from(result.pathOffsets), [0, 4, 8, 12, 16, 20, 21]);
  assert.deepEqual(
    Array.from(result.xy),
    [...ring, ...hole, ...ring, ...hole, ...ring, [5, 6]].flat(),
  );
});
for (const content of [
  'bad JSON',
  '{"type":"Point","coordinates":[0,0]}',
  '{"type":"FeatureCollection","features":[{"type":"Feature","geometry":{"type":"GeometryCollection","geometries":[]}}]}',
]) {
  test(`malformed/unsupported GeoJSON rejected: ${content.slice(0, 28)}`, () =>
    assert.throws(() => flatten({ bytes: new TextEncoder().encode(content) })));
}

test('checksum includes trailing bytes in non-word-aligned arrays', async () => {
  const { checksum } = await import('../benchmarks/workloads.mjs');
  assert.notEqual(checksum(new Uint8Array([1, 2, 3])), checksum(new Uint8Array([1, 2, 4])));
});
