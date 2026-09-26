import { integer, RuntimeError } from '../errors.js';
import type { CacheStats, ReclaimStats, ResourceCacheReport } from '../types.js';

export const emptyCacheStats = (): CacheStats => ({ hits: 0, misses: 0, evictions: 0 });
export const emptyReclaimStats = (): ReclaimStats => ({
  attempts: 0,
  succeeded: 0,
  failed: 0,
  byReason: { capacity: 0, resident: 0, pressure: 0, resize: 0, adaptive: 0 },
});
export function addCacheStats(total: CacheStats, delta: CacheStats): void {
  for (const key of ['hits', 'misses', 'evictions'] as const)
    total[key] = Math.min(Number.MAX_SAFE_INTEGER, total[key] + delta[key]);
}
export function cacheReport(
  raw: ResourceCacheReport,
  previous = emptyCacheStats(),
): ResourceCacheReport {
  if (
    !raw ||
    typeof raw !== 'object' ||
    !Array.isArray(raw.keys) ||
    raw.keys.length > 128 ||
    raw.keys.some((key) => typeof key !== 'string' || key.length > 1024)
  )
    throw new RuntimeError(
      'INVALID_ARGUMENT',
      'Resource cache reports require at most 128 bounded keys',
    );
  const next = {
    usedBytes: integer(raw.usedBytes, 'resource usedBytes'),
    hits: integer(raw.hits, 'resource hits'),
    misses: integer(raw.misses, 'resource misses'),
    evictions: integer(raw.evictions, 'resource evictions'),
    keys: [...new Set(raw.keys)],
  };
  if (
    next.hits < previous.hits ||
    next.misses < previous.misses ||
    next.evictions < previous.evictions
  )
    throw new RuntimeError('INVALID_ARGUMENT', 'Resource cache counters must be monotonic');
  return next;
}
