import assert from 'node:assert/strict';
import test from 'node:test';
import { encodeError, decodeError } from '../dist/remote-error.js';
import { RuntimeError } from '../dist/index.js';
import { runtime, options, take } from './helpers.mjs';

test('real Node Worker preserves business failure information with stable Runtime codes', async (t) => {
  const rt = runtime();
  t.after(() => rt.dispose());
  await assert.rejects(rt.createScope().enqueue('businessError', options(null)).result, (e) => {
    assert.equal(e.code, 'REMOTE_ERROR');
    assert.equal(e.name, 'RuntimeError');
    assert.equal(e.remoteError.name, 'DataError');
    assert.equal(e.remoteError.code, 'READ_BUDGET');
    assert.deepEqual(e.remoteError.details, { limit: 32 });
    return true;
  });
  assert.equal((await take(rt.createScope().enqueue('ping', options(4)))).value, 4);
});

test('remote error fields and details are bounded and invalid details preserve the primary failure', () => {
  const huge = Object.assign(new Error('x'.repeat(10000)), {
    name: 'n'.repeat(500),
    code: 'c'.repeat(500),
    details: { text: 'x'.repeat(5000) },
  });
  const wire = encodeError(huge),
    result = decodeError(wire);
  assert.equal(result.remoteError.truncated, true);
  assert.equal(result.remoteError.detailsOmitted, true);
  assert.ok(JSON.stringify(wire).length * 2 < 16384);
  for (const details of [
    () => {},
    new Uint8Array(10),
    { value: 1n },
    new Map(),
    {
      get x() {
        throw Error('getter called');
      },
    },
  ]) {
    assert.equal(
      encodeError(Object.assign(new Error('original'), { details })).detailsOmitted,
      true,
    );
  }
  const cycle = {};
  cycle.self = cycle;
  assert.equal(
    encodeError(Object.assign(new Error('original'), { details: cycle })).detailsOmitted,
    true,
  );
  let reads = 0;
  const accessor = new Error('original');
  Object.defineProperty(accessor, 'details', {
    get() {
      reads++;
      throw Error('getter');
    },
  });
  assert.equal(encodeError(accessor).detailsOmitted, true);
  assert.equal(reads, 0);
  assert.equal(
    decodeError(encodeError(new RuntimeError('BUDGET_EXCEEDED', 'full'))).code,
    'BUDGET_EXCEEDED',
  );
});

test('receiver validates Runtime codes and remote control-message bounds', () => {
  const wire = encodeError(new Error('original'));
  for (const patch of [
    { code: 'READ_BUDGET' },
    { message: 'x'.repeat(1025) },
    { details: { text: 'x'.repeat(5000) } },
    { truncated: 'yes' },
  ])
    assert.throws(() => decodeError({ ...wire, ...patch }), { code: 'PROTOCOL_ERROR' });
});
