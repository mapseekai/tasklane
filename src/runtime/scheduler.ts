/** Indexed heap: deletion and priority updates do not leave stale entries behind. */
class Heap<T extends object> {
  private items: T[] = [];
  private positions = new Map<T, number>();
  constructor(private compare: (a: T, b: T) => number) {}
  get first(): T | undefined {
    return this.items[0];
  }
  has(value: T): boolean {
    return this.positions.has(value);
  }
  add(value: T): void {
    if (this.has(value)) this.remove(value);
    this.items.push(value);
    this.positions.set(value, this.items.length - 1);
    this.up(this.items.length - 1);
  }
  remove(value: T): void {
    const index = this.positions.get(value);
    if (index === undefined) return;
    const last = this.items.pop()!;
    this.positions.delete(value);
    if (index < this.items.length) {
      this.items[index] = last;
      this.positions.set(last, index);
      this.down(this.up(index));
    }
  }
  private swap(a: number, b: number): void {
    const x = this.items[a]!,
      y = this.items[b]!;
    this.items[a] = y;
    this.items[b] = x;
    this.positions.set(x, b);
    this.positions.set(y, a);
  }
  private up(start: number): number {
    let i = start;
    while (i > 0) {
      const p = (i - 1) >> 1;
      if (this.compare(this.items[p]!, this.items[i]!) <= 0) break;
      this.swap(p, i);
      i = p;
    }
    return i;
  }
  private down(start: number): void {
    let i = start;
    for (;;) {
      let child = i * 2 + 1;
      if (child >= this.items.length) return;
      if (
        child + 1 < this.items.length &&
        this.compare(this.items[child + 1]!, this.items[child]!) < 0
      )
        child++;
      if (this.compare(this.items[i]!, this.items[child]!) <= 0) return;
      this.swap(i, child);
      i = child;
    }
  }
}

export interface ScheduledJob {
  groupKey: string;
  laneKey?: string;
  order: number;
  enqueuedAt: number;
  options: { priority?: 'interactive' | 'foreground' | 'background' };
}
interface Group<T extends ScheduledJob> {
  key: string;
  served: number;
  buckets: Map<string, Bucket<T>>;
}
interface Bucket<T extends ScheduledJob> {
  group: Group<T>;
  rank: number;
  key: string;
  lane: string;
  jobs: Heap<T>;
}
interface Promotion<T> {
  job: T;
  at: number;
}

/** FIFO within each priority/group; ageing and service history are independent of task completion. */
export class Scheduler<T extends ScheduledJob> {
  private groups = new Map<string, Group<T>>();
  private idle = new Map<string, Group<T>>();
  private membership = new Map<T, Bucket<T>>();
  private events = new Map<T, Promotion<T>>();
  private promotions = new Heap<Promotion<T>>((a, b) => a.at - b.at);
  private ready = new Heap<Bucket<T>>(
    (a, b) =>
      a.rank - b.rank ||
      a.group.served - b.group.served ||
      a.jobs.first!.order - b.jobs.first!.order,
  );
  private clock = 0;
  constructor(
    private ageingMs: number,
    private historyLimit = 4096,
    private policy: 'strict' | 'ageing' = 'strict',
  ) {}
  get historySize(): number {
    return this.idle.size;
  }
  add(job: T): void {
    let group = this.groups.get(job.groupKey);
    if (!group) {
      group = { key: job.groupKey, served: this.clock, buckets: new Map() };
      this.groups.set(group.key, group);
    }
    this.idle.delete(group.key);
    const rank = { interactive: 0, foreground: 1, background: 2 }[
      job.options.priority ?? 'foreground'
    ];
    const bucket = this.bucket(group, rank, job.laneKey ?? '');
    this.ready.remove(bucket);
    bucket.jobs.add(job);
    this.membership.set(job, bucket);
    this.ready.add(bucket);
    if (rank && this.policy === 'ageing') this.promoteAt(job, job.enqueuedAt + this.ageingMs);
  }
  remove(job: T): void {
    const bucket = this.membership.get(job);
    if (!bucket) return;
    this.ready.remove(bucket);
    bucket.jobs.remove(job);
    this.membership.delete(job);
    if (bucket.jobs.first) this.ready.add(bucket);
    else bucket.group.buckets.delete(bucket.key);
    const event = this.events.get(job);
    if (event) this.promotions.remove(event);
    this.events.delete(job);
    if (bucket.group.buckets.size === 0) {
      this.idle.delete(bucket.group.key);
      this.idle.set(bucket.group.key, bucket.group);
      while (this.idle.size > this.historyLimit) {
        const key = this.idle.keys().next().value!;
        this.idle.delete(key);
        this.groups.delete(key);
      }
    }
  }
  private bucket(group: Group<T>, rank: number, lane: string): Bucket<T> {
    const key = `${rank}\0${lane}`;
    let bucket = group.buckets.get(key);
    if (!bucket) {
      bucket = { group, rank, key, lane, jobs: new Heap<T>((a, b) => a.order - b.order) };
      group.buckets.set(key, bucket);
    }
    return bucket;
  }
  private promoteAt(job: T, at: number): void {
    const event = { job, at };
    this.events.set(job, event);
    this.promotions.add(event);
  }
  select(now: number, eligible: (job: T) => boolean): T | undefined {
    while (this.promotions.first && this.promotions.first.at <= now) {
      const event = this.promotions.first;
      this.promotions.remove(event);
      this.events.delete(event.job);
      const bucket = this.membership.get(event.job)!;
      this.ready.remove(bucket);
      bucket.jobs.remove(event.job);
      if (bucket.jobs.first) this.ready.add(bucket);
      else bucket.group.buckets.delete(bucket.key);
      const next = this.bucket(bucket.group, bucket.rank - 1, bucket.lane);
      this.ready.remove(next);
      next.jobs.add(event.job);
      this.membership.set(event.job, next);
      this.ready.add(next);
      if (next.rank) this.promoteAt(event.job, event.at + this.ageingMs);
    }
    const skipped: Bucket<T>[] = [];
    try {
      while (this.ready.first) {
        const bucket = this.ready.first;
        this.ready.remove(bucket);
        skipped.push(bucket);
        const job = bucket.jobs.first!;
        if (!eligible(job)) continue;
        // Update every bucket of this group before the next selection.
        for (const b of bucket.group.buckets.values()) this.ready.remove(b);
        bucket.group.served = ++this.clock;
        for (const b of bucket.group.buckets.values())
          if (b.jobs.first && !skipped.includes(b)) this.ready.add(b);
        return job;
      }
      return undefined;
    } finally {
      for (const bucket of skipped) if (bucket.jobs.first) this.ready.add(bucket);
    }
  }
  releaseScope(prefix: string): void {
    for (const [key] of this.idle)
      if (key.startsWith(prefix)) {
        this.idle.delete(key);
        this.groups.delete(key);
      }
  }
}
