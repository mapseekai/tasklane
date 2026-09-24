import { binaryByteLength } from '../binary.js';
import { integer, RuntimeError } from '../errors.js';

interface Entry {
  owner: string;
  value: unknown;
  bytes: number;
  pinned: boolean;
}

export interface ScopedCache {
  get<T = unknown>(key: string): T | undefined;
  /** bytes must include non-binary allocations such as strings or application state. */
  set(key: string, value: unknown, bytes: number): void;
  /** Non-evictable state is allowed only inside an exclusive session. */
  setPinned(key: string, value: unknown, bytes: number): void;
  delete(key: string): void;
}

/** One bounded LRU per physical worker. Namespace and pinned-state ownership are explicit. */
export class CacheStore {
  private entries = new Map<string, Entry>();
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
    const id = (key: string) => JSON.stringify([owner, session ?? null, key]);
    return {
      get: <T>(key: string): T | undefined => {
        const name = id(key);
        const entry = this.entries.get(name);
        if (!entry) return undefined;
        this.entries.delete(name);
        this.entries.set(name, entry);
        return entry.value as T;
      },
      set: (key, value, bytes) => this.set(id(key), { owner, value, bytes, pinned: false }),
      setPinned: (key, value, bytes) => {
        if (!session) {
          throw new RuntimeError('INVALID_ARGUMENT', 'Pinned state requires an exclusive session');
        }
        this.set(id(key), { owner, value, bytes, pinned: true });
      },
      delete: (key) => this.delete(id(key)),
    };
  }
  release(owner?: string): void {
    for (const [key, entry] of this.entries) {
      if (owner === undefined || entry.owner === owner) this.delete(key);
    }
  }
  private delete(key: string): void {
    const entry = this.entries.get(key);
    if (entry) this.used -= entry.bytes;
    this.entries.delete(key);
  }
  private set(key: string, entry: Entry): void {
    integer(entry.bytes, 'cache entry bytes');
    if (binaryByteLength(entry.value) > entry.bytes || entry.bytes > this.limit) {
      throw new RuntimeError(
        'BUDGET_EXCEEDED',
        'Cache entry exceeds its declared bytes or cache limit',
      );
    }
    const previous = this.entries.get(key);
    let availableBytes = this.limit - this.used + (previous?.bytes ?? 0);
    let availableEntries = this.maxEntries - this.entries.size + (previous ? 1 : 0);
    const victims: string[] = [];
    for (const [name, candidate] of this.entries) {
      if (availableBytes >= entry.bytes && availableEntries >= 1) break;
      if (name === key || candidate.pinned) continue;
      victims.push(name);
      availableBytes += candidate.bytes;
      availableEntries++;
    }
    if (availableBytes < entry.bytes || availableEntries < 1) {
      throw new RuntimeError('BUDGET_EXCEEDED', 'Pinned state prevents cache admission');
    }
    for (const victim of victims) this.delete(victim);
    this.delete(key);
    this.entries.set(key, entry);
    this.used += entry.bytes;
  }
}
