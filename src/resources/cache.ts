import { binaryByteLength, dataByteLength, snapshotBinaryView } from '../binary.js';
import { integer, RuntimeError } from '../errors.js';
import type { CacheStats, ResourceCacheReport, ResourceCacheSnapshot } from '../types.js';
import { addCacheStats, cacheReport, emptyCacheStats } from './telemetry.js';

interface Namespace {
  owner: string;
  prefix: string;
  closed: boolean;
  session: string;
}
interface Entry {
  namespace: Namespace;
  value: unknown;
  bytes: number;
  pinned: boolean;
  binary?: boolean;
  dispose?: () => void | Promise<void>;
  stopping?: Promise<void>;
  resource?: string;
  report?: ResourceCacheReport;
  trim?: (targetBytes: number) => number | Promise<number>;
}
export interface CacheResourceOptions {
  /** Dispose reader-owned cache data, then return the remaining reservation. */
  trim?(targetBytes: number): number | Promise<number>;
}
export interface CacheResourceLease {
  /** Complete current footprint plus absolute reader-owned cache counters. */
  report(snapshot: ResourceCacheReport): void;
  readonly bytes: number;
  readonly released: boolean;
  /** Grow before allocation, shrink after disposal. Failure preserves the previous accounting. */
  resize(bytes: number): void;
  /** Credits remain held until the disposer succeeds; failures can be retried. */
  release(): Promise<void>;
}
export interface ScopedCache {
  get<T = unknown>(key: string): T | undefined;
  /** Declared bytes are checked against backing stores plus scalar/key metadata. */
  set(key: string, value: unknown, bytes: number): void;
  /** Fresh native view, shared backing storage, no custom view properties. Charges the full buffer. */
  setBinary(key: string, value: ArrayBufferView): void;
  setPinned(key: string, value: unknown, bytes: number): void;
  /** Opaque session-owned state. Accounting is caller-declared; disposal is explicit and may be async. */
  setResource<T>(
    key: string,
    value: T,
    bytes: number,
    dispose: (value: T) => void | Promise<void>,
    options?: CacheResourceOptions,
  ): CacheResourceLease;
  delete(key: string): void | Promise<void>;
}

export class CacheStore {
  private readonly counters: CacheStats = { hits: 0, misses: 0, evictions: 0 };
  private entries = new Map<string, Entry>();
  private namespaces = new Map<string, Map<string, Namespace>>();
  private serial = 0;
  private used = 0;
  private readonly resourceCounters = emptyCacheStats();
  private capacity: number;
  constructor(
    limit: number,
    readonly maxEntries = 4096,
  ) {
    integer(limit, 'cache limit');
    this.capacity = limit;
    integer(maxEntries, 'cache maxEntries', 1);
  }
  get bytes(): number {
    return this.used;
  }
  get limit(): number {
    return this.capacity;
  }
  get resourceStats(): CacheStats {
    return { ...this.resourceCounters };
  }
  get reports(): ResourceCacheSnapshot[] {
    return [...this.entries].flatMap(([id, entry]) =>
      entry.report
        ? [
            {
              ...entry.report,
              keys: [...entry.report.keys],
              id,
              scope: entry.namespace.owner,
              session: entry.namespace.session,
              resource: entry.resource!,
              reservedBytes: entry.bytes,
            },
          ]
        : [],
    );
  }
  /** Lower ordinary LRU occupancy, then ask opted-in reader resources to release their data. */
  async trim(targetBytes: number): Promise<void> {
    integer(targetBytes, 'cache trim target');
    for (const [key, entry] of this.entries) {
      if (this.used <= targetBytes) break;
      if (!entry.pinned && !entry.stopping) {
        this.delete(key);
        this.count('evictions');
      }
    }
    for (const [key, entry] of this.entries) {
      if (this.used <= targetBytes) break;
      if (!entry.trim || entry.stopping) continue;
      const target = Math.max(0, entry.bytes - (this.used - targetBytes));
      const report = entry.report;
      const bytes = integer(await entry.trim(target), 'trimmed resource bytes');
      if (this.entries.get(key) !== entry) continue;
      if (bytes > entry.bytes)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Resource trim cannot increase its reservation');
      this.used += bytes - entry.bytes;
      entry.bytes = bytes;
      // The reader must report its surviving keys; clear stale hints if it did not.
      if (entry.report && (entry.report === report || entry.report.usedBytes > bytes))
        entry.report = {
          ...entry.report,
          usedBytes: Math.min(entry.report.usedBytes, bytes),
          keys: [],
        };
    }
  }
  resize(limit: number): void {
    integer(limit, 'cache limit');
    const protectedBytes = [...this.entries.values()].reduce(
      (n, e) => n + (e.pinned || e.stopping ? e.bytes : 0),
      0,
    );
    if (protectedBytes > limit)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Pinned resources exceed cache target');
    for (const [key, entry] of this.entries) {
      if (this.used <= limit) break;
      if (!entry.pinned && !entry.stopping) {
        this.delete(key);
        this.count('evictions');
      }
    }
    this.capacity = limit;
  }
  get stats(): CacheStats {
    return { ...this.counters };
  }
  private count(key: keyof CacheStats): void {
    this.counters[key] = Math.min(Number.MAX_SAFE_INTEGER, this.counters[key] + 1);
  }
  scope(owner: string, session?: string): ScopedCache {
    let owners = this.namespaces.get(owner);
    if (!owners) {
      owners = new Map();
      this.namespaces.set(owner, owners);
    }
    const sessionKey = session ?? '';
    let namespace = owners.get(sessionKey);
    if (!namespace) {
      namespace = { owner, prefix: `${++this.serial}:`, closed: false, session: sessionKey };
      owners.set(sessionKey, namespace);
    }
    const ns = namespace;
    const id = (key: string) => {
      if (ns.closed) throw new RuntimeError('CLOSED', 'Cache scope has been released');
      return ns.prefix + key;
    };
    const sessionOnly = () => {
      if (!session)
        throw new RuntimeError('INVALID_ARGUMENT', 'Pinned state requires an exclusive session');
    };
    return {
      get: <T>(key: string): T | undefined => {
        if (ns.closed) return undefined;
        const name = id(key),
          entry = this.entries.get(name);
        if (!entry || entry.stopping) {
          this.count('misses');
          return undefined;
        }
        this.count('hits');
        this.entries.delete(name);
        this.entries.set(name, entry);
        return entry.value as T;
      },
      set: (key, value, bytes) => this.set(id(key), { namespace: ns, value, bytes, pinned: false }),
      setBinary: (key, value) => {
        const name = id(key);
        const view = snapshotBinaryView(value);
        this.set(name, {
          namespace: ns,
          value: view,
          bytes: binaryByteLength(view),
          pinned: false,
          binary: true,
        });
      },
      setPinned: (key, value, bytes) => {
        sessionOnly();
        this.set(id(key), { namespace: ns, value, bytes, pinned: true });
      },
      setResource: (key, value, bytes, dispose, options = {}) => {
        sessionOnly();
        if (typeof dispose !== 'function')
          throw new RuntimeError('INVALID_ARGUMENT', 'Resource disposer is required');
        if (
          typeof key !== 'string' ||
          key.length > 1024 ||
          (options.trim !== undefined && typeof options.trim !== 'function')
        )
          throw new RuntimeError('INVALID_ARGUMENT', 'Invalid resource key or trim callback');
        const name = id(key);
        const entry: Entry = {
          namespace: ns,
          value,
          bytes,
          pinned: true,
          dispose: () => dispose(value),
          resource: key,
          trim: options.trim,
        };
        this.set(name, entry);
        const current = () => this.entries.get(name) === entry;
        const store = this;
        return {
          report(snapshot) {
            if (!current() || ns.closed || entry.stopping)
              throw new RuntimeError('CLOSED', 'Cache resource is closing or released');
            const next = cacheReport(snapshot, entry.report);
            if (next.usedBytes > entry.bytes)
              throw new RuntimeError(
                'BUDGET_EXCEEDED',
                'Reported working set exceeds resource reservation',
              );
            const reports = store.reports.filter((r) => r.id !== name);
            if (
              reports.length >= 64 ||
              reports.reduce((n, r) => n + r.keys.length, next.keys.length) > 1024
            )
              throw new RuntimeError('BUDGET_EXCEEDED', 'Worker resource telemetry limit reached');
            const before = entry.report ?? emptyCacheStats();
            addCacheStats(store.resourceCounters, {
              hits: next.hits - before.hits,
              misses: next.misses - before.misses,
              evictions: next.evictions - before.evictions,
            });
            entry.report = next;
          },
          get bytes() {
            return current() ? entry.bytes : 0;
          },
          get released() {
            return !current();
          },
          resize(bytes) {
            if (!current() || ns.closed || entry.stopping)
              throw new RuntimeError('CLOSED', 'Cache resource is closing or released');
            integer(bytes, 'cache resource bytes');
            store.makeRoom(name, bytes);
            store.used += bytes - entry.bytes;
            entry.bytes = bytes;
            if (entry.report && entry.report.usedBytes > bytes)
              entry.report = { ...entry.report, usedBytes: bytes, keys: [] };
          },
          async release() {
            if (current()) await store.delete(name);
          },
        };
      },
      delete: (key) => this.delete(id(key)),
    };
  }
  release(owner?: string): Promise<void> {
    for (const [name, sessions] of this.namespaces) {
      if (owner === undefined || name === owner) {
        for (const ns of sessions.values()) ns.closed = true;
        this.namespaces.delete(name);
      }
    }
    const pending: Promise<void>[] = [];
    for (const [key, entry] of this.entries) {
      if (owner === undefined || entry.namespace.owner === owner) {
        const stop = this.delete(key);
        if (stop) pending.push(stop);
      }
    }
    return Promise.all(pending).then(() => {});
  }
  private delete(key: string): void | Promise<void> {
    const entry = this.entries.get(key);
    if (!entry) return;
    if (entry.stopping) return entry.stopping;
    const remove = () => {
      this.used -= entry.bytes;
      this.entries.delete(key);
      entry.value = undefined;
      entry.dispose = undefined;
    };
    if (!entry.dispose) {
      remove();
      return;
    }
    entry.stopping = Promise.resolve()
      .then(entry.dispose)
      .then(remove)
      .catch((error) => {
        entry.stopping = undefined;
        throw error;
      });
    // Explicit callers still receive the rejection; abandoned deletes cannot crash a host.
    void entry.stopping.catch(() => {});
    return entry.stopping;
  }
  private set(key: string, entry: Entry): void {
    integer(entry.bytes, 'cache entry bytes');
    if (
      entry.bytes > this.limit ||
      (!entry.dispose &&
        !entry.binary &&
        dataByteLength(entry.value, { resident: true, maxMetadataBytes: entry.bytes }) >
          entry.bytes)
    ) {
      throw new RuntimeError(
        'BUDGET_EXCEEDED',
        'Cache entry exceeds its declared bytes or cache limit',
      );
    }
    const previous = this.entries.get(key);
    if (previous?.dispose)
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Delete and await disposal before replacing a resource',
      );
    this.makeRoom(key, entry.bytes);
    this.delete(key);
    this.entries.set(key, entry);
    this.used += entry.bytes;
  }
  private makeRoom(key: string, requiredBytes: number): void {
    const previous = this.entries.get(key);
    let bytes = this.limit - this.used + (previous?.bytes ?? 0);
    let count = this.maxEntries - this.entries.size + (previous ? 1 : 0);
    const victims: string[] = [];
    for (const [name, candidate] of this.entries) {
      if (bytes >= requiredBytes && count >= 1) break;
      if (name === key || candidate.pinned || candidate.stopping) continue;
      victims.push(name);
      bytes += candidate.bytes;
      count++;
    }
    if (bytes < requiredBytes || count < 1)
      throw new RuntimeError(
        'BUDGET_EXCEEDED',
        'Pinned or disposing state prevents cache admission',
      );
    for (const victim of victims) {
      this.delete(victim);
      this.count('evictions');
    }
  }
}
