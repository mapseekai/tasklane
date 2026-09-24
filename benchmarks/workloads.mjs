// Deterministic benchmark kernels; no dependency on emap, luma or a GIS library.
export function coordinates(count, start = 0) {
  const xy = new Float64Array(count * 2);
  for (let j = 0; j < count; j++) {
    const i = start + j;
    xy[j * 2] = ((i * 16807) % 3600000) / 10000 - 180;
    xy[j * 2 + 1] = ((i * 48271) % 1600000) / 10000 - 80;
  }
  return xy;
}

export function convert({ xy, operation = 'project' }) {
  if (!(xy instanceof Float64Array) || xy.length % 2)
    throw new Error('Expected interleaved Float64 XY');
  if (operation !== 'project' && operation !== 'layout')
    throw new Error('Unknown coordinate operation');
  const vertices = new Float32Array(xy.length * 2);
  const bounds = new Float64Array([Infinity, Infinity, -Infinity, -Infinity]);
  const radians = Math.PI / 180;
  for (let i = 0, j = 0; i < xy.length; i += 2, j += 4) {
    let x = xy[i],
      y = xy[i + 1];
    if (!Number.isFinite(x) || !Number.isFinite(y)) throw new Error('Non-finite coordinate');
    if (operation === 'project') {
      if (Math.abs(y) > 85.0511287798066) throw new Error('Latitude exceeds Web Mercator domain');
      x = 6378137 * x * radians;
      y = 6378137 * Math.log(Math.tan(Math.PI / 4 + (y * radians) / 2));
    }
    const highX = Math.fround(x),
      highY = Math.fround(y);
    vertices[j] = highX;
    vertices[j + 1] = highY;
    vertices[j + 2] = x - highX;
    vertices[j + 3] = y - highY;
    bounds[0] = Math.min(bounds[0], x);
    bounds[1] = Math.min(bounds[1], y);
    bounds[2] = Math.max(bounds[2], x);
    bounds[3] = Math.max(bounds[3], y);
  }
  return { vertices, bounds, count: xy.length / 2 };
}

const geometryCodes = { Point: 1, LineString: 2, MultiLineString: 3, Polygon: 4, MultiPolygon: 5 };
function paths(geometry) {
  if (!geometry || !Object.hasOwn(geometryCodes, geometry.type))
    throw new Error('Unsupported GeoJSON geometry');
  switch (geometry.type) {
    case 'Point':
      return [[geometry.coordinates]];
    case 'LineString':
      return [geometry.coordinates];
    case 'MultiLineString':
    case 'Polygon':
      return geometry.coordinates;
    case 'MultiPolygon':
      return geometry.coordinates.flat();
    default:
      throw new Error('Unsupported geometry');
  }
}

// Two-pass flat binary conversion. Ring/feature/polygon membership is retained.
export function flatten({ bytes }) {
  if (!(bytes instanceof Uint8Array)) throw new Error('Expected UTF-8 bytes');
  const json = JSON.parse(new TextDecoder('utf-8', { fatal: true }).decode(bytes));
  if (json.type !== 'FeatureCollection' || !Array.isArray(json.features))
    throw new Error('Expected FeatureCollection');
  let points = 0,
    pathCount = 0,
    polygonCount = 0;
  for (const feature of json.features) {
    if (feature.type !== 'Feature') throw new Error('Expected Feature');
    const rings = paths(feature.geometry);
    for (const ring of rings) points += ring.length;
    pathCount += rings.length;
    polygonCount +=
      feature.geometry.type === 'MultiPolygon' ? feature.geometry.coordinates.length : 1;
  }
  const xy = new Float64Array(points * 2);
  const pathOffsets = new Uint32Array(pathCount + 1);
  const featureOffsets = new Uint32Array(json.features.length + 1);
  const polygonOffsets = new Uint32Array(polygonCount + 1);
  const featurePolygonOffsets = new Uint32Array(json.features.length + 1);
  const types = new Uint8Array(json.features.length);
  let at = 0,
    path = 0,
    polygon = 0;
  for (let f = 0; f < json.features.length; f++) {
    const geometry = json.features[f].geometry;
    types[f] = geometryCodes[geometry.type];
    featureOffsets[f] = path;
    featurePolygonOffsets[f] = polygon;
    const parts = geometry.type === 'MultiPolygon' ? geometry.coordinates : [paths(geometry)];
    for (const part of parts) {
      polygonOffsets[polygon++] = path;
      for (const ring of part) {
        pathOffsets[path++] = at / 2;
        for (const coordinate of ring) {
          if (
            !Array.isArray(coordinate) ||
            coordinate.length !== 2 ||
            !coordinate.every(Number.isFinite)
          )
            throw new Error('Benchmark converter requires finite 2D coordinates');
          xy[at++] = coordinate[0];
          xy[at++] = coordinate[1];
        }
      }
    }
  }
  pathOffsets[path] = points;
  featureOffsets[json.features.length] = path;
  polygonOffsets[polygon] = path;
  featurePolygonOffsets[json.features.length] = polygon;
  return {
    xy,
    pathOffsets,
    featureOffsets,
    polygonOffsets,
    featurePolygonOffsets,
    types,
    count: points,
  };
}

export function geojson(features, start = 0) {
  const xy = coordinates(features * 16, start);
  const rows = [];
  for (let f = 0; f < features; f++) {
    const points = [];
    for (let j = 0; j < 16; j++) points.push([xy[(f * 16 + j) * 2], xy[(f * 16 + j) * 2 + 1]]);
    rows.push({
      type: 'Feature',
      id: start + f,
      properties: { rank: f % 7 },
      geometry: { type: 'LineString', coordinates: points },
    });
  }
  return new TextEncoder().encode(JSON.stringify({ type: 'FeatureCollection', features: rows }));
}

// Full bitwise checksum. Benchmarks report consumer time separately; correctness
// tests also compare every component against an independent numerical reference.
export function checksum(view) {
  const words = new Uint32Array(view.buffer, view.byteOffset, Math.floor(view.byteLength / 4));
  let sum = 0,
    xor = 0;
  for (let i = 0; i < words.length; i++) {
    sum = (sum + words[i]) >>> 0;
    xor = (xor ^ Math.imul(words[i], (i & 255) + 1)) >>> 0;
  }
  const tail = new Uint8Array(view.buffer, view.byteOffset + words.length * 4, view.byteLength % 4);
  for (let i = 0; i < tail.length; i++) {
    sum = (sum + tail[i]) >>> 0;
    xor = (xor ^ Math.imul(tail[i], i + 1)) >>> 0;
  }
  return `${sum.toString(16)}:${xor.toString(16)}:${view.byteLength}`;
}
