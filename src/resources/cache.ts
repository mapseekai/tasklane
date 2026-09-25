import { dataByteLength } from '../binary.js';
import { integer, RuntimeError } from '../errors.js';

interface Namespace {
  owner: string;
  prefix: string;
  closed: boolean;
}
interface Entry {
  namespace: Namespace;
  value: unknown;
  bytes: number;
  pinned: boolean;
  dispose?: () => void | Promise<void>;
  stopping?: Promise<void>;
}
export interface ScopedCache {
  get<T = unknown>(key: string): T | undefined;
  /** Declared bytes are checked against backing stores plus scalar/key metadata. */
  set(key: string, value: unknown, bytes: number): void;
  setPinned(key: string, value: unknown, bytes: number): void;
  /** Opaque session-owned state. Accounting is caller-declared; disposal is explicit and may be async. */
  setResource<T>(
    key: string,
    value: T,
    bytes: number,
    dispose: (value: T) => void | Promise<void>,
  ): void;
  delete(key: string): void | Promise<void>;
}

export class CacheStore {
  private entries = new Map<string, Entry>();
  private namespaces = new Map<string, Map<string, Namespace>>();
  private serial = 0;
  private used = 0;
  constructor(
    readonly limit: number,
    readonly maxEntries = 4096,
  ) {
    integer(limit, 'cache limit');
    integer(maxEntries, 'cache maxEntries', 1);
  }
  get bytes(): number {
    return this.used;
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
      namespace = { owner, prefix: `${++this.serial}:`, closed: false };
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
        if (!entry || entry.stopping) return undefined;
        this.entries.delete(name);
        this.entries.set(name, entry);
        return entry.value as T;
      },
      set: (key, value, bytes) => this.set(id(key), { namespace: ns, value, bytes, pinned: false }),
      setPinned: (key, value, bytes) => {
        sessionOnly();
        this.set(id(key), { namespace: ns, value, bytes, pinned: true });
      },
      setResource: (key, value, bytes, dispose) => {
        sessionOnly();
        if (typeof dispose !== 'function')
          throw new RuntimeError('INVALID_ARGUMENT', 'Resource disposer is required');
        this.set(id(key), {
          namespace: ns,
          value,
          bytes,
          pinned: true,
          dispose: () => dispose(value),
        });
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
      (!entry.dispose &&
        dataByteLength(entry.value, { resident: true, maxMetadataBytes: entry.bytes }) >
          entry.bytes) ||
      entry.bytes > this.limit
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
    let bytes = this.limit - this.used + (previous?.bytes ?? 0);
    let count = this.maxEntries - this.entries.size + (previous ? 1 : 0);
    const victims: string[] = [];
    for (const [name, candidate] of this.entries) {
      if (bytes >= entry.bytes && count >= 1) break;
      if (name === key || candidate.pinned || candidate.stopping) continue;
      victims.push(name);
      bytes += candidate.bytes;
      count++;
    }
    if (bytes < entry.bytes || count < 1)
      throw new RuntimeError(
        'BUDGET_EXCEEDED',
        'Pinned or disposing state prevents cache admission',
      );
    for (const victim of victims) this.delete(victim);
    this.delete(key);
    this.entries.set(key, entry);
    this.used += entry.bytes;
  }
}
