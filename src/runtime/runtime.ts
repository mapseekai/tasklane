import { binaryByteLength } from '../binary.js';
import { aborted, asError, integer, required, RuntimeError, timeout } from '../errors.js';
import { type FromWorker, header, isHeader } from '../protocol.js';
import { BudgetLedger, validateTaskBudget } from '../resources/budget.js';
import { OwnedResult } from '../resources/lease.js';
import type {
  Catalog,
  PoolOptions,
  PreparedInput,
  ResultLease,
  RuntimeBudgets,
  RuntimeOptions,
  RuntimeStats,
  SessionTaskOptions,
  TaskHandle,
  TaskMap,
  TaskName,
  TaskOptions,
  TaskState,
  TaskTiming,
  WorkerEndpoint,
} from '../types.js';
import { type Deferred, deferred } from './deferred.js';

type Timer = ReturnType<typeof setTimeout>;
interface Pool {
  name: string;
  options: Required<PoolOptions>;
  slots: Set<Slot>;
}
interface Slot {
  id: number;
  epoch: number;
  pool: Pool;
  endpoint: WorkerEndpoint;
  ready: Deferred<void>;
  state: 'starting' | 'ready' | 'closing' | 'closed';
  tasks: Set<string>;
  job?: Job;
  session?: SessionRecord;
  subscriptions: (() => void)[];
  startupTimer?: Timer;
  idleTimer?: Timer;
  stopped?: Promise<void>;
  releaseCache(): void;
  used: number;
  cacheUsed: number;
}
interface ScopeRecord {
  id: string;
  label: string;
  parent?: ScopeRecord;
  closed: boolean;
  children: Set<ScopeRecord>;
  jobs: Set<Job>;
  sessions: Set<SessionRecord>;
  leases: Set<OwnedResult<unknown>>;
  touched: Set<Slot>;
  disposal?: Promise<void>;
}
interface SessionRecord {
  id: string;
  scope: ScopeRecord;
  pool: string;
  slot?: Slot;
  closed: boolean;
  lost?: RuntimeError;
  leases: Set<OwnedResult<unknown>>;
  disposal?: Promise<void>;
}
interface Job {
  id: string;
  order: number;
  name: string;
  scope: ScopeRecord;
  session?: SessionRecord;
  options: TaskOptions<unknown>;
  state: TaskState;
  result: Deferred<ResultLease<unknown>>;
  settled: Deferred<void>;
  controller: AbortController;
  timing: TaskTiming;
  enqueuedAt: number;
  admittedAt?: number;
  postedAt?: number;
  slot?: Slot;
  cancelled: boolean;
  done: boolean;
  releaseExecution?: () => void;
  releaseOutput?: () => void;
  queueTimer?: Timer;
  executionTimer?: Timer;
  removeSignal?: () => void;
}

const MiB = 1024 ** 2;
const priorities = { interactive: 0, foreground: 1, background: 2 } as const;
let runtimeSerial = 0;

/** Application-owned runtime; no hidden module-global pool, CPU budget or dataset. */
export class WorkerRuntime<T extends Catalog<T> = TaskMap> {
  private readonly pools = new Map<string, Pool>();
  private readonly scopes = new Set<ScopeRecord>();
  private readonly jobs = new Map<string, Job>();
  private readonly queue = new Set<Job>();
  private readonly ledger: BudgetLedger;
  private readonly affinity = new Map<string, Slot>();
  private readonly served = new Map<string, number>();
  private readonly options: Required<Omit<RuntimeOptions, 'pools' | 'budgets' | 'onDiagnostic'>>;
  private readonly diagnostic?: RuntimeOptions['onDiagnostic'];
  private readonly prefix = `runtime-${++runtimeSerial}`;
  private serial = 0;
  private clock = 0;
  private active = 0;
  private leaseCount = 0;
  private closed = false;
  private scheduled = false;
  private draining = false;
  private disposal?: Promise<void>;
  private readonly counters = {
    completed: 0,
    cancelled: 0,
    failed: 0,
    workerStarts: 0,
    workerTerminations: 0,
    inputBytes: 0,
    outputBytes: 0,
    observerErrors: 0,
  };

  constructor(options: RuntimeOptions) {
    const entries = Object.entries(options.pools);
    if (!entries.length)
      throw new RuntimeError('INVALID_ARGUMENT', 'At least one pool is required');
    let capacity = 0;
    for (const [name, pool] of entries) {
      if (!name || typeof pool.factory !== 'function') {
        throw new RuntimeError(
          'INVALID_ARGUMENT',
          'Pools need non-empty names and endpoint factories',
        );
      }
      const size = integer(pool.size, `${name}.size`, 1);
      capacity += size;
      this.pools.set(name, {
        name,
        options: {
          ...pool,
          size,
          cacheBytes: integer(pool.cacheBytes ?? 0, `${name}.cacheBytes`),
          cacheEntries: integer(pool.cacheEntries ?? 4096, `${name}.cacheEntries`, 1),
          allowHardCancel: pool.allowHardCancel ?? false,
          idleTimeoutMs:
            pool.idleTimeoutMs === 0 ? 0 : timeout(pool.idleTimeoutMs ?? 30_000, 'idleTimeoutMs'),
        },
        slots: new Set(),
      });
    }
    this.options = {
      maxWorkers: integer(options.maxWorkers ?? capacity, 'maxWorkers', 1),
      maxActiveTasks: integer(options.maxActiveTasks ?? Math.min(capacity, 2), 'maxActiveTasks', 1),
      maxQueuedTasks: integer(options.maxQueuedTasks ?? 1024, 'maxQueuedTasks', 1),
      startupTimeoutMs: timeout(options.startupTimeoutMs ?? 10_000, 'startupTimeoutMs'),
      queueTimeoutMs: timeout(options.queueTimeoutMs ?? 120_000, 'queueTimeoutMs'),
      executionTimeoutMs: timeout(options.executionTimeoutMs ?? 120_000, 'executionTimeoutMs'),
      ageingMs: timeout(options.ageingMs ?? 2000, 'ageingMs'),
      maxAffinityEntries: integer(options.maxAffinityEntries ?? 4096, 'maxAffinityEntries', 1),
    };
    const budgets: RuntimeBudgets = {
      inputBytes: options.budgets?.inputBytes ?? 64 * MiB,
      scratchBytes: options.budgets?.scratchBytes ?? 128 * MiB,
      outputBytes: options.budgets?.outputBytes ?? 64 * MiB,
      cacheBytes: options.budgets?.cacheBytes ?? 128 * MiB,
    };
    this.ledger = new BudgetLedger(budgets);
    for (const pool of this.pools.values()) {
      this.ledger.validate({ cacheBytes: pool.options.cacheBytes });
    }
    this.diagnostic = options.onDiagnostic;
  }

  get stats(): RuntimeStats {
    const slots = this.slots();
    return {
      queued: this.queue.size,
      active: this.active,
      workers: slots.length,
      closingWorkers: slots.filter((slot) => slot.state === 'closing').length,
      leases: this.leaseCount,
      reserved: { ...this.ledger.used },
      peakReserved: { ...this.ledger.peak },
      cacheUsedBytes: slots.reduce((sum, slot) => sum + slot.cacheUsed, 0),
      ...this.counters,
    };
  }

  createScope(label = 'scope'): RuntimeScope<T> {
    return this._createScope(label);
  }

  /** Idempotent. Cooperatively written prepare callbacks must eventually settle. */
  dispose(): Promise<void> {
    if (this.disposal) return this.disposal;
    this.closed = true;
    const work = [...this.jobs.values()].map((job) => job.settled.promise);
    const scopes = [...this.scopes].filter((scope) => !scope.parent);
    const scopeStops = scopes.map((scope) => this._disposeScope(scope));
    const stops = this.slots().map((slot) =>
      this.retire(slot, new RuntimeError('CLOSED', 'Runtime disposed')),
    );
    this.disposal = Promise.all([...scopeStops, ...stops, ...work]).then(() => {
      this.affinity.clear();
      this.served.clear();
    });
    return this.disposal;
  }

  /** @internal */
  _createScope(label: string, parent?: ScopeRecord): RuntimeScope<T> {
    if (this.closed || parent?.closed)
      throw new RuntimeError('CLOSED', 'Runtime or parent scope is closed');
    const record: ScopeRecord = {
      id: `${this.prefix}/scope-${++this.serial}`,
      label,
      parent,
      closed: false,
      children: new Set(),
      jobs: new Set(),
      sessions: new Set(),
      leases: new Set(),
      touched: new Set(),
    };
    parent?.children.add(record);
    this.scopes.add(record);
    return new RuntimeScope(this, record);
  }

  /** @internal */
  _session(scope: ScopeRecord, pool: string): WorkerSession<T> {
    if (this.closed || scope.closed) throw new RuntimeError('CLOSED', 'Scope is closed');
    if (!this.pools.has(pool)) throw new RuntimeError('INVALID_ARGUMENT', `Unknown pool: ${pool}`);
    const record: SessionRecord = {
      id: `${scope.id}/session-${++this.serial}`,
      scope,
      pool,
      closed: false,
      leases: new Set(),
    };
    scope.sessions.add(record);
    return new WorkerSession(this, record);
  }

  /** @internal */
  _enqueue<K extends TaskName<T>>(
    scope: ScopeRecord,
    name: K,
    raw: TaskOptions<T[K]['input']>,
    session?: SessionRecord,
  ): TaskHandle<T[K]['output']> {
    if (this.closed || scope.closed || session?.closed)
      throw new RuntimeError('CLOSED', 'Scope or session is closed');
    if (session?.lost) throw session.lost;
    const pool = this.pools.get(raw.pool);
    if (!pool) throw new RuntimeError('INVALID_ARGUMENT', `Unknown pool: ${raw.pool}`);
    if (typeof name !== 'string' || !name || typeof raw.prepare !== 'function') {
      throw new RuntimeError('INVALID_ARGUMENT', 'A task name and prepare callback are required');
    }
    if (raw.priority !== undefined && !Object.hasOwn(priorities, raw.priority)) {
      throw new RuntimeError('INVALID_ARGUMENT', 'Unknown task priority');
    }
    const cancellation = raw.cancellation ?? 'cooperative';
    if (!['cooperative', 'discard', 'terminate'].includes(cancellation)) {
      throw new RuntimeError('INVALID_ARGUMENT', 'Unknown cancellation mode');
    }
    if (cancellation === 'terminate' && !pool.options.allowHardCancel) {
      throw new RuntimeError('HARD_CANCEL_DENIED', 'This pool does not allow hard cancellation');
    }
    const budget = validateTaskBudget(raw.budget);
    this.ledger.validate(budget);
    const queueTimeoutMs = timeout(
      raw.queueTimeoutMs ?? this.options.queueTimeoutMs,
      'queueTimeoutMs',
    );
    const executionTimeoutMs = timeout(
      raw.executionTimeoutMs ?? this.options.executionTimeoutMs,
      'executionTimeoutMs',
    );
    if (this.queue.size >= this.options.maxQueuedTasks)
      throw new RuntimeError('QUEUE_FULL', 'Waiting queue is full');
    const order = ++this.serial;
    const job: Job = {
      id: `${this.prefix}/job-${order}`,
      order,
      name,
      scope,
      session,
      options: {
        ...raw,
        budget,
        cancellation,
        queueTimeoutMs,
        executionTimeoutMs,
      } as TaskOptions<unknown>,
      state: 'queued',
      result: deferred<ResultLease<unknown>>(),
      settled: deferred<void>(),
      controller: new AbortController(),
      timing: { queueMs: 0, startupMs: 0, prepareMs: 0, roundTripMs: 0, workerMs: 0, totalMs: 0 },
      enqueuedAt: performance.now(),
      cancelled: false,
      done: false,
    };
    this.jobs.set(job.id, job);
    scope.jobs.add(job);
    this.queue.add(job);
    const signal = raw.signal;
    if (signal) {
      const cancel = () => this.cancel(job, signal.reason);
      signal.addEventListener('abort', cancel, { once: true });
      job.removeSignal = () => signal.removeEventListener('abort', cancel);
      if (signal.aborted) cancel();
    }
    if (!job.done) {
      job.queueTimer = setTimeout(() => {
        this.failQueued(
          job,
          new RuntimeError('QUEUE_TIMEOUT', 'Task expired while waiting for admission'),
        );
      }, queueTimeoutMs);
      this.schedule();
    }
    return {
      id: job.id,
      get state() {
        return job.state;
      },
      get timing() {
        return { ...job.timing };
      },
      result: job.result.promise as Promise<ResultLease<T[K]['output']>>,
      settled: job.settled.promise,
      cancel: (reason) => this.cancel(job, reason),
    };
  }

  /** @internal */
  _disposeScope(scope: ScopeRecord): Promise<void> {
    if (scope.disposal) return scope.disposal;
    scope.closed = true;
    const tasks = [...scope.jobs];
    for (const task of tasks) this.cancel(task, new RuntimeError('CLOSED', 'Scope disposed'));
    for (const lease of [...scope.leases]) lease.release();
    const children = [...scope.children].map((child) => this._disposeScope(child));
    const sessions = [...scope.sessions].map((session) => this._disposeSession(session));
    for (const slot of scope.touched) {
      if (slot.state === 'ready') {
        try {
          slot.endpoint.postMessage({
            ...header(slot.epoch),
            type: 'release-scope',
            scope: scope.id,
          });
        } catch (error) {
          void this.retire(slot, new RuntimeError('WORKER_FAILED', asError(error).message));
        }
      }
    }
    for (const key of this.affinity.keys())
      if (key.startsWith(`${scope.id}\0`)) this.affinity.delete(key);
    for (const key of this.served.keys())
      if (key.startsWith(`${scope.id}\0`)) this.served.delete(key);
    scope.disposal = Promise.all([
      ...children,
      ...sessions,
      ...tasks.map((job) => job.settled.promise),
    ]).then(() => {
      scope.touched.clear();
      scope.parent?.children.delete(scope);
      this.scopes.delete(scope);
    });
    return scope.disposal;
  }

  /** @internal */
  _disposeSession(session: SessionRecord): Promise<void> {
    if (session.disposal) return session.disposal;
    session.closed = true;
    for (const lease of [...session.leases]) lease.release();
    const jobs = [...session.scope.jobs].filter((job) => job.session === session);
    for (const job of jobs) this.cancel(job, new RuntimeError('CLOSED', 'Session disposed'));
    // A session owns its physical worker exclusively, so termination cannot kill
    // another session or another scope's active task.
    const stop = session.slot
      ? this.retire(session.slot, new RuntimeError('CLOSED', 'Session disposed'))
      : Promise.resolve();
    session.disposal = Promise.all([stop, ...jobs.map((job) => job.settled.promise)]).then(() => {
      session.scope.sessions.delete(session);
    });
    return session.disposal;
  }

  private slots(): Slot[] {
    return [...this.pools.values()].flatMap((pool) => [...pool.slots]);
  }
  private schedule(): void {
    if (this.closed || this.scheduled) return;
    this.scheduled = true;
    queueMicrotask(() => {
      this.scheduled = false;
      this.drain();
    });
  }
  private group(job: Job): string {
    return `${job.scope.id}\0${job.options.group ?? 'default'}`;
  }
  private rank(job: Job, now: number): number {
    return Math.max(
      0,
      priorities[job.options.priority ?? 'foreground'] -
        Math.floor((now - job.enqueuedAt) / this.options.ageingMs),
    );
  }
  private drain(): void {
    if (this.closed || this.draining) return;
    this.draining = true;
    try {
      while (this.active < this.options.maxActiveTasks && this.queue.size) {
        const now = performance.now();
        const candidates = [...this.queue].sort(
          (a, b) =>
            this.rank(a, now) - this.rank(b, now) ||
            (this.served.get(this.group(a)) ?? 0) - (this.served.get(this.group(b)) ?? 0) ||
            a.order - b.order,
        );
        let admitted = false;
        for (const job of candidates) {
          if (!this.ledger.fits(job.options.budget)) continue;
          let slot: Slot | undefined;
          try {
            slot = this.findSlot(job);
          } catch (error) {
            this.failQueued(job, asError(error));
            continue;
          }
          if (!slot) continue;
          this.admit(job, slot);
          admitted = true;
          break;
        }
        if (!admitted) break;
      }
    } finally {
      this.draining = false;
    }
  }
  private affinityId(job: Job): string | undefined {
    return job.options.affinity === undefined
      ? undefined
      : `${job.scope.id}\0${job.options.pool}\0${job.options.affinity}`;
  }
  private findSlot(job: Job): Slot | undefined {
    if (job.session?.lost) throw job.session.lost;
    if (job.session?.slot) {
      const slot = job.session.slot;
      return !slot.job && (slot.state === 'ready' || slot.state === 'starting') ? slot : undefined;
    }
    const pool = required(this.pools.get(job.options.pool), 'Task pool');
    const available = [...pool.slots].filter(
      (slot) => !slot.job && !slot.session && (slot.state === 'ready' || slot.state === 'starting'),
    );
    const affinity = this.affinityId(job);
    const preferred = affinity ? this.affinity.get(affinity) : undefined;
    if (preferred && available.includes(preferred)) return preferred;
    if (available.length) return available.sort((a, b) => a.used - b.used)[0];
    if (pool.slots.size >= pool.options.size) return undefined;
    const all = this.slots();
    if (
      all.length >= this.options.maxWorkers ||
      !this.ledger.fits({ cacheBytes: pool.options.cacheBytes })
    ) {
      const victim = all
        .filter(
          (slot) => slot.pool !== pool && !slot.job && !slot.session && slot.state === 'ready',
        )
        .sort((a, b) => a.used - b.used)[0];
      if (victim)
        void this.retire(
          victim,
          new RuntimeError('CLOSED', 'Idle worker reclaimed for another pool'),
        );
      return undefined;
    }
    return this.spawn(pool);
  }

  private spawn(pool: Pool): Slot {
    const releaseCache = this.ledger.reserve({ cacheBytes: pool.options.cacheBytes });
    let endpoint: WorkerEndpoint;
    try {
      endpoint = pool.options.factory();
    } catch (error) {
      releaseCache();
      throw new RuntimeError('WORKER_FAILED', 'Worker factory failed', { cause: error });
    }
    const id = ++this.serial;
    const slot: Slot = {
      id,
      epoch: id,
      pool,
      endpoint,
      state: 'starting',
      ready: deferred<void>(),
      tasks: new Set(),
      subscriptions: [],
      releaseCache,
      used: ++this.clock,
      cacheUsed: 0,
    };
    pool.slots.add(slot);
    this.counters.workerStarts++;
    try {
      slot.subscriptions.push(endpoint.onMessage((message) => this.receive(slot, message)));
      slot.subscriptions.push(
        endpoint.onFailure((error) => {
          void this.retire(
            slot,
            new RuntimeError('WORKER_FAILED', error.message, { cause: error }),
          );
        }),
      );
      slot.startupTimer = setTimeout(() => {
        void this.retire(
          slot,
          new RuntimeError('STARTUP_TIMEOUT', 'Worker did not complete its protocol handshake'),
        );
      }, this.options.startupTimeoutMs);
      endpoint.postMessage({
        ...header(slot.epoch),
        type: 'hello',
        cacheBytes: pool.options.cacheBytes,
        cacheEntries: pool.options.cacheEntries,
      });
    } catch (error) {
      void this.retire(slot, new RuntimeError('WORKER_FAILED', asError(error).message));
    }
    return slot;
  }
  private admit(job: Job, slot: Slot): void {
    this.queue.delete(job);
    clearTimeout(job.queueTimer);
    clearTimeout(slot.idleTimer);
    const budget = job.options.budget;
    job.releaseExecution = this.ledger.reserve({
      inputBytes: budget.inputBytes,
      scratchBytes: budget.scratchBytes,
    });
    job.releaseOutput = this.ledger.reserve({ outputBytes: budget.outputBytes });
    job.slot = slot;
    job.scope.touched.add(slot);
    slot.job = job;
    slot.used = ++this.clock;
    if (job.session && !job.session.slot) {
      job.session.slot = slot;
      slot.session = job.session;
    }
    const key = this.affinityId(job);
    if (key) {
      this.affinity.delete(key);
      this.affinity.set(key, slot);
      while (this.affinity.size > this.options.maxAffinityEntries)
        this.affinity.delete(required(this.affinity.keys().next().value, 'Affinity entry'));
    }
    this.served.set(this.group(job), ++this.clock);
    this.active++;
    job.admittedAt = performance.now();
    job.timing.queueMs = job.admittedAt - job.enqueuedAt;
    job.state = 'starting';
    job.executionTimer = setTimeout(() => this.deadline(job), job.options.executionTimeoutMs);
    void this.run(job, slot);
  }
  private async run(job: Job, slot: Slot): Promise<void> {
    try {
      await slot.ready.promise;
      job.timing.startupMs = performance.now() - required(job.admittedAt, 'Admission timestamp');
      job.controller.signal.throwIfAborted();
      if (!slot.tasks.has(job.name))
        throw new RuntimeError('UNKNOWN_TASK', `Worker does not implement ${job.name}`);
      job.state = 'preparing';
      const started = performance.now();
      let prepared: PreparedInput<unknown>;
      try {
        prepared = await job.options.prepare({ signal: job.controller.signal });
      } finally {
        job.timing.prepareMs = performance.now() - started;
      }
      job.controller.signal.throwIfAborted();
      if (slot.state !== 'ready')
        throw new RuntimeError('WORKER_FAILED', 'Worker was lost during input preparation');
      const bytes = binaryByteLength(prepared.payload);
      if (bytes > job.options.budget.inputBytes)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Prepared input exceeds reserved inputBytes');
      job.state = 'running';
      job.postedAt = performance.now();
      slot.endpoint.postMessage(
        {
          ...header(slot.epoch),
          type: 'request',
          id: job.id,
          scope: job.scope.id,
          session: job.session?.id,
          task: job.name,
          payload: prepared.payload,
          maxOutputBytes: job.options.budget.outputBytes,
        },
        prepared.transfer,
      );
      this.counters.inputBytes += bytes;
    } catch (error) {
      this.finish(job, undefined, asError(error));
    }
  }
  private receive(slot: Slot, raw: unknown): void {
    if (slot.state === 'closed' || slot.state === 'closing') return;
    if (!isHeader(raw)) {
      void this.retire(
        slot,
        new RuntimeError('PROTOCOL_ERROR', 'Invalid worker protocol envelope'),
      );
      return;
    }
    if (raw.epoch !== slot.epoch) return; // Old physical instance; never match by request ID alone.
    const message = raw as FromWorker;
    if (message.type === 'ready') {
      if (slot.state !== 'starting') return;
      if (!Array.isArray(message.tasks) || message.tasks.some((name) => typeof name !== 'string')) {
        void this.retire(
          slot,
          new RuntimeError('PROTOCOL_ERROR', 'Invalid worker capability handshake'),
        );
        return;
      }
      clearTimeout(slot.startupTimer);
      slot.tasks = new Set(message.tasks);
      slot.state = 'ready';
      slot.ready.resolve();
      this.schedule();
      return;
    }
    if (message.type === 'released') {
      if (
        Number.isSafeInteger(message.cacheBytes) &&
        message.cacheBytes >= 0 &&
        message.cacheBytes <= slot.pool.options.cacheBytes
      )
        slot.cacheUsed = message.cacheBytes;
      return;
    }
    const job = slot.job;
    if (!job || job.done || message.id !== job.id || message.scope !== job.scope.id) return;
    if (message.type === 'progress') {
      if (!job.cancelled && !job.scope.closed && job.options.onProgress) {
        try {
          job.options.onProgress(message.value);
        } catch (error) {
          this.observe(
            new RuntimeError('REMOTE_ERROR', 'Progress observer threw', { cause: error }),
          );
        }
      }
      return;
    }
    if (message.type !== 'result' && message.type !== 'error' && message.type !== 'cancelled') {
      void this.retire(slot, new RuntimeError('PROTOCOL_ERROR', 'Unexpected worker response type'));
      return;
    }
    if (
      !Number.isFinite(message.workerMs) ||
      message.workerMs < 0 ||
      !Number.isSafeInteger(message.cacheBytes) ||
      message.cacheBytes < 0 ||
      message.cacheBytes > slot.pool.options.cacheBytes
    ) {
      void this.retire(slot, new RuntimeError('PROTOCOL_ERROR', 'Invalid worker metrics'));
      return;
    }
    slot.cacheUsed = message.cacheBytes;
    job.timing.workerMs = message.workerMs;
    job.timing.roundTripMs = performance.now() - required(job.postedAt, 'Dispatch timestamp');
    if (message.type === 'result') {
      try {
        const bytes = binaryByteLength(message.value);
        if (bytes !== message.byteLength || bytes > job.options.budget.outputBytes)
          throw new RuntimeError(
            'BUDGET_EXCEEDED',
            'Worker result violates its reserved output budget',
          );
        this.finish(job, { value: message.value, bytes });
      } catch (error) {
        void this.retire(slot, asError(error));
      }
    } else {
      const error = message.error;
      if (!error || typeof error.message !== 'string' || typeof error.code !== 'string') {
        void this.retire(slot, new RuntimeError('PROTOCOL_ERROR', 'Malformed remote error'));
        return;
      }
      this.finish(job, undefined, new RuntimeError(error.code, error.message));
    }
  }
  private cancel(job: Job, reason?: unknown): void {
    if (job.done || job.cancelled) return;
    job.cancelled = true;
    job.controller.abort(aborted(reason));
    job.result.reject(aborted(reason));
    job.removeSignal?.();
    job.removeSignal = undefined;
    if (job.state === 'queued') {
      this.finish(job, undefined, aborted(reason));
      return;
    }
    job.state = 'cancelling';
    const slot = job.slot;
    if (!slot || job.postedAt === undefined) return; // Keep prepare reservation until callback settles.
    if (job.options.cancellation === 'terminate') {
      void this.retire(slot, aborted(reason));
    } else if (job.options.cancellation === 'cooperative' && slot.state === 'ready') {
      try {
        slot.endpoint.postMessage({
          ...header(slot.epoch),
          type: 'cancel',
          id: job.id,
          scope: job.scope.id,
        });
      } catch (error) {
        void this.retire(slot, new RuntimeError('WORKER_FAILED', asError(error).message));
      }
    }
    // discard mode deliberately waits for the physical result, without sending cancel.
  }
  private deadline(job: Job): void {
    if (job.done) return;
    const error = new RuntimeError(
      'EXECUTION_TIMEOUT',
      'Task exceeded its physical execution deadline',
    );
    job.result.reject(error);
    job.controller.abort(error);
    if (job.postedAt !== undefined && job.slot) void this.retire(job.slot, error);
    // A main-thread prepare callback is not preemptible. It retains its reservation
    // until it settles; abort-aware callbacks must observe their signal.
    else if (job.state === 'starting' && job.slot) void this.retire(job.slot, error);
  }
  private failQueued(job: Job, error: Error): void {
    if (job.done || job.state !== 'queued') return;
    this.finish(job, undefined, error);
  }
  private finish(job: Job, result?: { value: unknown; bytes: number }, error?: Error): void {
    if (job.done) return;
    job.done = true;
    clearTimeout(job.queueTimer);
    clearTimeout(job.executionTimer);
    job.removeSignal?.();
    job.removeSignal = undefined;
    this.queue.delete(job);
    this.jobs.delete(job.id);
    job.scope.jobs.delete(job);
    const group = this.group(job);
    if (![...job.scope.jobs].some((pending) => this.group(pending) === group))
      this.served.delete(group);
    if (job.admittedAt !== undefined) this.active--;
    job.releaseExecution?.();
    job.releaseExecution = undefined;
    const slot = job.slot;
    if (slot?.job === job) {
      slot.job = undefined;
      slot.used = ++this.clock;
      this.armIdle(slot);
    }
    job.timing.totalMs = performance.now() - job.enqueuedAt;
    if (job.cancelled || job.scope.closed || job.session?.closed) {
      job.state = 'cancelled';
      this.counters.cancelled++;
      job.result.reject(aborted(error));
    } else if (error || !result) {
      job.state = 'failed';
      this.counters.failed++;
      job.result.reject(error ?? new RuntimeError('REMOTE_ERROR', 'Missing worker result'));
    } else {
      job.state = 'succeeded';
      this.counters.completed++;
      this.counters.outputBytes += result.bytes;
      const releaseOutput = required(job.releaseOutput, 'Output reservation');
      job.releaseOutput = undefined;
      this.leaseCount++;
      const lease = new OwnedResult(result.value, result.bytes, () => {
        releaseOutput();
        this.leaseCount--;
        job.scope.leases.delete(lease);
        job.session?.leases.delete(lease);
        this.schedule();
      });
      job.scope.leases.add(lease);
      job.session?.leases.add(lease);
      job.result.resolve(lease);
    }
    job.releaseOutput?.();
    job.releaseOutput = undefined;
    // Drop large user closures as soon as physical work ends, even if a TaskHandle is retained.
    job.options.prepare = () => {
      throw new RuntimeError('CLOSED', 'Task has finished');
    };
    job.options.onProgress = undefined;
    job.options.signal = undefined;
    job.slot = undefined;
    job.settled.resolve();
    this.schedule();
  }
  private armIdle(slot: Slot): void {
    if (
      this.closed ||
      slot.session ||
      slot.job ||
      slot.state !== 'ready' ||
      !slot.pool.options.idleTimeoutMs
    )
      return;
    clearTimeout(slot.idleTimer);
    slot.idleTimer = setTimeout(() => {
      if (!slot.job && !slot.session)
        void this.retire(slot, new RuntimeError('CLOSED', 'Idle worker expired'));
    }, slot.pool.options.idleTimeoutMs);
  }
  private retire(slot: Slot, reason: Error): Promise<void> {
    if (slot.stopped) return slot.stopped;
    const stopped = deferred<void>();
    slot.stopped = stopped.promise;
    slot.state = 'closing';
    clearTimeout(slot.startupTimer);
    clearTimeout(slot.idleTimer);
    for (const unsubscribe of slot.subscriptions) {
      try {
        unsubscribe();
      } catch (error) {
        this.observe(
          new RuntimeError('WORKER_FAILED', 'Endpoint listener cleanup failed', { cause: error }),
        );
      }
    }
    slot.subscriptions = [];
    slot.ready.reject(reason);
    if (slot.session && !slot.session.closed) {
      slot.session.lost = new RuntimeError(
        'SESSION_LOST',
        'Session worker was lost; create a new session',
        { cause: reason },
      );
      for (const pending of slot.session.scope.jobs) {
        if (pending.session === slot.session && pending.state === 'queued')
          this.failQueued(pending, slot.session.lost);
      }
    }
    const job = slot.job;
    // Capture preparation state: terminating a Worker cannot stop user code in the main realm.
    const wasPosted = job?.postedAt !== undefined;
    job?.controller.abort(reason);
    if (job && !job.cancelled) job.result.reject(reason);
    void (async () => {
      try {
        await slot.endpoint.terminate();
      } catch (error) {
        // Failed termination is not proof of physical completion. Keep the slot
        // quarantined and retain reservations rather than oversubscribe resources.
        const failure = new RuntimeError(
          'WORKER_FAILED',
          'Endpoint termination failed; slot quarantined',
          { cause: error },
        );
        this.observe(failure);
        stopped.reject(failure);
        return;
      }
      slot.state = 'closed';
      slot.cacheUsed = 0;
      slot.releaseCache();
      slot.pool.slots.delete(slot);
      for (const scope of this.scopes) scope.touched.delete(slot);
      for (const [key, value] of this.affinity) if (value === slot) this.affinity.delete(key);
      this.counters.workerTerminations++;
      if (job && wasPosted) this.finish(job, undefined, reason);
      stopped.resolve();
      this.schedule();
    })();
    return slot.stopped;
  }
  private observe(error: RuntimeError): void {
    this.counters.observerErrors++;
    try {
      this.diagnostic?.(error);
    } catch {
      /* Observers cannot break lifecycle cleanup. */
    }
  }
}

export class RuntimeScope<T extends Catalog<T> = TaskMap> {
  /** @internal */
  constructor(
    private readonly runtime: WorkerRuntime<T>,
    private readonly record: ScopeRecord,
  ) {}
  get id(): string {
    return this.record.id;
  }
  get label(): string {
    return this.record.label;
  }
  get closed(): boolean {
    return this.record.closed;
  }
  createScope(label = 'scope'): RuntimeScope<T> {
    return this.runtime._createScope(label, this.record);
  }
  session(pool: string): WorkerSession<T> {
    return this.runtime._session(this.record, pool);
  }
  enqueue<K extends TaskName<T>>(
    name: K,
    options: TaskOptions<T[K]['input']>,
  ): TaskHandle<T[K]['output']> {
    return this.runtime._enqueue(this.record, name, options);
  }
  dispose(): Promise<void> {
    return this.runtime._disposeScope(this.record);
  }
}

/** Required affinity with an exclusive physical worker and fail-closed generation semantics. */
export class WorkerSession<T extends Catalog<T> = TaskMap> {
  /** @internal */
  constructor(
    private readonly runtime: WorkerRuntime<T>,
    private readonly record: SessionRecord,
  ) {}
  get id(): string {
    return this.record.id;
  }
  get state(): 'unbound' | 'bound' | 'lost' | 'closed' {
    return this.record.closed
      ? 'closed'
      : this.record.lost
        ? 'lost'
        : this.record.slot
          ? 'bound'
          : 'unbound';
  }
  enqueue<K extends TaskName<T>>(
    name: K,
    options: SessionTaskOptions<T[K]['input']>,
  ): TaskHandle<T[K]['output']> {
    return this.runtime._enqueue(
      this.record.scope,
      name,
      { ...options, pool: this.record.pool },
      this.record,
    );
  }
  dispose(): Promise<void> {
    return this.runtime._disposeSession(this.record);
  }
}

export function createWorkerRuntime<T extends Catalog<T> = TaskMap>(
  options: RuntimeOptions,
): WorkerRuntime<T> {
  return new WorkerRuntime<T>(options);
}
