import { decodeError } from '../remote-error.js';
import { validateProgress } from '../progress.js';
import {
  encodePacket,
  packetBytes,
  packetBlobBytes,
  validateBlobTransfers,
  type Packet,
} from '../packet.js';
import { aborted, asError, integer, required, RuntimeError, timeout } from '../errors.js';
import { type FromWorker, header, isHeader } from '../protocol.js';
import { BudgetLedger, validateTaskBudget } from '../resources/budget.js';
import { OwnedResult } from '../resources/lease.js';
import type {
  Catalog,
  PoolOptions,
  PreparedInput,
  PreparedTaskOptions,
  SessionPreparedTaskOptions,
  TaskBudget,
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
import { Scheduler } from './scheduler.js';

type Timer = ReturnType<typeof setTimeout>;
interface Pool {
  options: Required<PoolOptions>;
  slots: Set<Slot>;
}
interface Slot {
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
  terminationFailed?: boolean;
  releases: Map<string, { deferred: Deferred<void>; timer: Timer }>;
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
  groupKey: string;
  laneKey: string;
  phase: 'queue' | 'produce' | 'prepared' | 'startup' | 'prepare' | 'execute' | 'done';
  name: string;
  scope: ScopeRecord;
  session?: SessionRecord;
  options: TaskOptions<unknown>;
  cost: TaskBudget;
  preparation?: { produce: PreparedTaskOptions<unknown>['prepareAsync']; timeoutMs: number };
  preparedInput?: PreparedInput<unknown>;
  reserved: boolean;
  workerStartedAt?: number;
  preparationTimer?: Timer;
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
  private readonly scheduler: Scheduler<Job>;
  private reservation?: Job;
  private reclaiming = false;
  private readonly options: Required<Omit<RuntimeOptions, 'pools' | 'budgets' | 'onDiagnostic'>>;
  private readonly diagnostic?: RuntimeOptions['onDiagnostic'];
  private readonly prefix = `runtime-${++runtimeSerial}`;
  private serial = 0;
  private clock = 0;
  private active = 0;
  private resultReservations = 0;
  private readonly preparationWindow = new Set<Job>();
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
    if (
      options.priorityPolicy !== undefined &&
      !['strict', 'ageing'].includes(options.priorityPolicy)
    )
      throw new RuntimeError('INVALID_ARGUMENT', 'Unknown priority policy');
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
      priorityPolicy: options.priorityPolicy ?? 'strict',
      maxWorkers: integer(options.maxWorkers ?? capacity, 'maxWorkers', 1),
      maxActiveTasks: integer(
        options.maxActiveTasks ?? Math.min(capacity, options.maxWorkers ?? capacity),
        'maxActiveTasks',
        1,
      ),
      maxPreparingTasks: integer(options.maxPreparingTasks ?? 2, 'maxPreparingTasks', 1),
      maxQueuedTasks: integer(options.maxQueuedTasks ?? 1024, 'maxQueuedTasks', 1),
      maxResultLeases: integer(options.maxResultLeases ?? 1024, 'maxResultLeases', 1),
      maxScopes: integer(options.maxScopes ?? 4096, 'maxScopes', 1),
      startupTimeoutMs: timeout(options.startupTimeoutMs ?? 10_000, 'startupTimeoutMs'),
      queueTimeoutMs: timeout(options.queueTimeoutMs ?? 120_000, 'queueTimeoutMs'),
      executionTimeoutMs: timeout(options.executionTimeoutMs ?? 120_000, 'executionTimeoutMs'),
      ageingMs: timeout(options.ageingMs ?? 2000, 'ageingMs'),
      maxAffinityEntries: integer(options.maxAffinityEntries ?? 4096, 'maxAffinityEntries', 1),
      releaseTimeoutMs: timeout(options.releaseTimeoutMs ?? 10_000, 'releaseTimeoutMs'),
      budgetWaitMs: timeout(options.budgetWaitMs ?? 1000, 'budgetWaitMs'),
    };
    const budgets: RuntimeBudgets = {
      inputBytes: options.budgets?.inputBytes ?? 64 * MiB,
      scratchBytes: options.budgets?.scratchBytes ?? 128 * MiB,
      outputBytes: options.budgets?.outputBytes ?? 64 * MiB,
      cacheBytes: options.budgets?.cacheBytes ?? 128 * MiB,
    };
    this.ledger = new BudgetLedger(budgets);
    this.scheduler = new Scheduler(this.options.ageingMs, 4096, options.priorityPolicy ?? 'strict');
    for (const pool of this.pools.values()) {
      this.ledger.validate({ cacheBytes: pool.options.cacheBytes });
    }
    this.diagnostic = options.onDiagnostic;
  }

  get stats(): RuntimeStats {
    const slots = this.slots();
    const preparationReserved: TaskBudget = { inputBytes: 0, scratchBytes: 0, outputBytes: 0 };
    for (const job of this.preparationWindow)
      for (const key of ['inputBytes', 'scratchBytes', 'outputBytes'] as const)
        preparationReserved[key] += job.cost[key];
    return {
      queued: this.queue.size,
      active: this.active,
      preparing: [...this.preparationWindow].filter((job) => job.phase === 'produce').length,
      prepared: [...this.preparationWindow].filter((job) => job.phase === 'prepared').length,
      preparationReserved,
      workers: slots.length,
      closingWorkers: slots.filter((slot) => slot.state === 'closing').length,
      leases: this.leaseCount,
      scopes: this.scopes.size,
      quarantinedWorkers: slots.filter((slot) => slot.terminationFailed).length,
      reserved: { ...this.ledger.used },
      peakReserved: { ...this.ledger.peak },
      cacheUsedBytes: slots.reduce((sum, slot) => sum + slot.cacheUsed, 0),
      ...this.counters,
    };
  }

  createScope(label = 'scope'): RuntimeScope<T> {
    return this._createScope(label);
  }

  /** Idempotent shutdown with confirmed physical completion. */
  dispose(): Promise<void> {
    if (this.disposal) return this.disposal;
    this.closed = true;
    const work = [...this.jobs.values()].map((job) => job.settled.promise);
    const scopes = [...this.scopes].filter((scope) => !scope.parent);
    const scopeStops = scopes.map((scope) => this._disposeScope(scope));
    // Active worker code may be non-cooperative. Idle workers get a graceful release barrier.
    const stops = this.slots()
      .filter((slot) => slot.job?.phase === 'execute' || slot.state === 'starting')
      .map((slot) => this.retire(slot, new RuntimeError('CLOSED', 'Runtime disposed')));
    this.disposal = (async () => {
      try {
        await Promise.all([...scopeStops, ...stops, ...work]);
      } finally {
        await Promise.all(
          this.slots().map((slot) =>
            this.retire(slot, new RuntimeError('CLOSED', 'Runtime disposed')),
          ),
        );
      }
      this.affinity.clear();
      this.scheduler.releaseScope(this.prefix);
    })();
    return this.disposal;
  }

  /** @internal */
  _createScope(label: string, parent?: ScopeRecord): RuntimeScope<T> {
    if (this.closed || parent?.closed)
      throw new RuntimeError('CLOSED', 'Runtime or parent scope is closed');
    if (this.scopes.size >= this.options.maxScopes)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Scope limit reached; dispose unused scopes');
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
    preparation?: {
      produce: PreparedTaskOptions<unknown>['prepareAsync'];
      scratchBytes: number;
      timeoutMs?: number;
    },
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
    if (raw.discardResult !== undefined && typeof raw.discardResult !== 'boolean')
      throw new RuntimeError('INVALID_ARGUMENT', 'discardResult must be boolean');
    const cancellation = raw.cancellation ?? 'cooperative';
    if (!['cooperative', 'discard', 'terminate'].includes(cancellation)) {
      throw new RuntimeError('INVALID_ARGUMENT', 'Unknown cancellation mode');
    }
    if (cancellation === 'terminate' && !pool.options.allowHardCancel) {
      throw new RuntimeError('HARD_CANCEL_DENIED', 'This pool does not allow hard cancellation');
    }
    const budget = validateTaskBudget(raw.budget);
    const cost = {
      ...budget,
      scratchBytes: Math.max(budget.scratchBytes, preparation?.scratchBytes ?? 0),
    };
    this.ledger.validate(cost);
    if (
      raw.blobLimits !== undefined &&
      (!raw.blobLimits || typeof raw.blobLimits !== 'object' || Array.isArray(raw.blobLimits))
    )
      throw new RuntimeError('INVALID_ARGUMENT', 'blobLimits must be an options object');
    const blobLimits = {
      inputBytes: integer(
        raw.blobLimits === undefined ? 0 : raw.blobLimits.inputBytes,
        'blobLimits.inputBytes',
      ),
      outputBytes: integer(
        raw.blobLimits === undefined ? 0 : raw.blobLimits.outputBytes,
        'blobLimits.outputBytes',
      ),
    };
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
      groupKey: `${scope.id}\0${raw.group ?? 'default'}`,
      laneKey: `${raw.pool}\0${session?.id ?? ''}`,
      phase: 'queue',
      name,
      cost,
      reserved: false,
      preparation: preparation
        ? {
            produce: preparation.produce,
            timeoutMs: timeout(preparation.timeoutMs ?? executionTimeoutMs, 'preparationTimeoutMs'),
          }
        : undefined,
      scope,
      session,
      options: {
        ...raw,
        budget,
        blobLimits,
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
    };
    this.jobs.set(job.id, job);
    scope.jobs.add(job);
    this.queue.add(job);
    this.scheduler.add(job);
    const signal = raw.signal;
    if (signal) {
      const cancel = () => this.cancel(job, signal.reason);
      signal.addEventListener('abort', cancel, { once: true });
      job.removeSignal = () => signal.removeEventListener('abort', cancel);
      if (signal.aborted) cancel();
    }
    if (job.phase !== 'done') {
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
  _enqueuePrepared<K extends TaskName<T>>(
    scope: ScopeRecord,
    name: K,
    options: PreparedTaskOptions<T[K]['input']>,
    session?: SessionRecord,
  ): TaskHandle<T[K]['output']> {
    const { prepareAsync, preparationScratchBytes, preparationTimeoutMs, ...rest } = options;
    if (typeof prepareAsync !== 'function')
      throw new RuntimeError('INVALID_ARGUMENT', 'prepareAsync callback is required');
    integer(preparationScratchBytes, 'preparationScratchBytes');
    return this._enqueue(
      scope,
      name,
      {
        ...rest,
        prepare: () => {
          throw new RuntimeError('PROTOCOL_ERROR', 'Missing prepared input');
        },
      },
      session,
      {
        produce: prepareAsync,
        scratchBytes: preparationScratchBytes,
        timeoutMs: preparationTimeoutMs,
      },
    );
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
    const releases: Promise<void>[] = [];
    for (const slot of scope.touched) {
      if (slot.state === 'ready') {
        releases.push(this.releaseScope(slot, scope.id));
      } else if (slot.stopped) releases.push(slot.stopped);
    }
    for (const key of this.affinity.keys())
      if (key.startsWith(`${scope.id}\0`)) this.affinity.delete(key);
    this.scheduler.releaseScope(`${scope.id}\0`);
    scope.disposal = Promise.all([
      ...releases,
      ...children,
      ...sessions,
      ...tasks.map((job) => job.settled.promise),
    ])
      .then(() => {
        this.scheduler.releaseScope(`${scope.id}\0`);
        scope.touched.clear();
        scope.parent?.children.delete(scope);
        this.scopes.delete(scope);
      })
      .catch((error) => {
        scope.disposal = undefined;
        throw error;
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
    const slot = session.slot;
    const stop = slot
      ? (async () => {
          if (slot.state === 'ready' && !slot.job) await this.releaseScope(slot, session.scope.id);
          await this.retire(slot, new RuntimeError('CLOSED', 'Session disposed'));
        })()
      : Promise.resolve();
    session.disposal = Promise.all([stop, ...jobs.map((job) => job.settled.promise)])
      .then(() => {
        session.scope.sessions.delete(session);
      })
      .catch((error) => {
        session.disposal = undefined;
        throw error;
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
  private drain(): void {
    if (this.closed || this.draining) return;
    this.draining = true;
    try {
      while (this.queue.size) {
        const now = performance.now();
        let chosen: Slot | undefined;
        const job = this.scheduler.select(
          now,
          (candidate) => {
            if (candidate.phase !== 'queue' && candidate.phase !== 'prepared') return false;
            const producing = !!candidate.preparation && !candidate.reserved;
            if (
              producing
                ? this.preparationWindow.size >= this.options.maxPreparingTasks
                : this.active >= this.options.maxActiveTasks
            )
              return false;
            if (candidate.reserved) {
              try {
                chosen = this.findSlot(candidate);
              } catch (error) {
                this.finish(candidate, undefined, asError(error));
              }
              return chosen !== undefined;
            }
            if (
              !candidate.options.discardResult &&
              this.resultReservations + this.leaseCount >= this.options.maxResultLeases
            )
              return false;
            if (this.reservation?.phase === 'done') this.reservation = undefined;
            const reserved = this.reservation;
            if (reserved && reserved !== candidate) {
              const used = this.ledger.used,
                limits = this.ledger.limits;
              const keys = ['inputBytes', 'scratchBytes', 'outputBytes'] as const;
              if (
                keys.some(
                  (key) => reserved.cost[key] > limits[key] - used[key] && candidate.cost[key] > 0,
                )
              )
                return false;
            }
            if (!this.ledger.fits(candidate.cost)) {
              if (
                !reserved &&
                now - candidate.enqueuedAt >= this.options.budgetWaitMs &&
                (producing || this.canRun(candidate))
              )
                this.reservation = candidate;
              return false;
            }
            if (producing) return true;
            try {
              chosen = this.findSlot(candidate);
            } catch (error) {
              this.failQueued(candidate, asError(error));
              return false;
            }
            return chosen !== undefined;
          },
          (candidate) => !candidate.reserved,
        );
        if (!job) break;
        if (this.reservation === job) this.reservation = undefined;
        if (job.preparation && !job.reserved) this.startPreparation(job);
        else if (chosen) this.admit(job, chosen);
      }
    } finally {
      this.draining = false;
    }
  }
  private canRun(job: Job): boolean {
    if (job.session?.lost) return false;
    if (job.session?.slot) return !job.session.slot.job && job.session.slot.state === 'ready';
    const pool = this.pools.get(job.options.pool)!;
    if ([...pool.slots].some((slot) => !slot.job && !slot.session && slot.state === 'ready'))
      return true;
    if (pool.slots.size >= pool.options.size) return false;
    const all = this.slots();
    return (
      (all.length < this.options.maxWorkers &&
        this.ledger.fits({ cacheBytes: pool.options.cacheBytes })) ||
      all.some((slot) => slot.pool !== pool && !slot.job && !slot.session && slot.state === 'ready')
    );
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
      if (this.reclaiming || all.some((slot) => slot.state === 'closing')) return undefined;
      const victim = all
        .filter(
          (slot) => slot.pool !== pool && !slot.job && !slot.session && slot.state === 'ready',
        )
        .sort((a, b) => a.used - b.used)[0];
      if (victim) {
        this.reclaiming = true;
        void this.retire(
          victim,
          new RuntimeError('CLOSED', 'Idle worker reclaimed for another pool'),
        )
          .finally(() => {
            this.reclaiming = false;
            this.schedule();
          })
          .catch(() => {});
      }
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
      epoch: id,
      pool,
      endpoint,
      state: 'starting',
      ready: deferred<void>(),
      tasks: new Set(),
      subscriptions: [],
      releases: new Map(),
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
      const failure = new RuntimeError('WORKER_FAILED', asError(error).message);
      void this.retire(slot, failure);
      throw failure;
    }
    if (slot.state === 'closing' || slot.state === 'closed')
      throw new RuntimeError('WORKER_FAILED', 'Worker failed synchronously during startup');
    return slot;
  }
  private reserveJob(job: Job): void {
    if (job.reserved) return;
    job.releaseExecution = this.ledger.reserve({
      inputBytes: job.cost.inputBytes,
      scratchBytes: job.cost.scratchBytes,
    });
    job.releaseOutput = this.ledger.reserve({ outputBytes: job.cost.outputBytes });
    job.reserved = true;
    if (!job.options.discardResult) this.resultReservations++;
    clearTimeout(job.queueTimer);
    job.admittedAt = performance.now();
    job.timing.queueMs = job.admittedAt - job.enqueuedAt;
    job.executionTimer = setTimeout(() => this.deadline(job), job.options.executionTimeoutMs);
  }
  private startPreparation(job: Job): void {
    this.reserveJob(job);
    this.preparationWindow.add(job);
    job.phase = 'produce';
    job.state = 'preparing';
    job.preparationTimer = setTimeout(() => this.deadline(job), job.preparation!.timeoutMs);
    const started = performance.now();
    void (async () => {
      try {
        const input = await job.preparation!.produce({ signal: job.controller.signal });
        job.controller.signal.throwIfAborted();
        if (
          performance.now() - started >= job.preparation!.timeoutMs ||
          performance.now() - job.admittedAt! >= job.options.executionTimeoutMs!
        )
          throw new RuntimeError('EXECUTION_TIMEOUT', 'Preparation exceeded deadline');
        if (!input || !Object.hasOwn(input, 'payload'))
          throw new RuntimeError('INVALID_ARGUMENT', 'prepareAsync must return a payload');
        job.preparedInput = input;
        job.phase = 'prepared';
        job.state = 'prepared';
      } catch (error) {
        this.finish(job, undefined, asError(error));
      } finally {
        clearTimeout(job.preparationTimer);
        job.timing.prepareMs = performance.now() - started;
        job.preparation = undefined;
        this.schedule();
      }
    })();
  }

  private admit(job: Job, slot: Slot): void {
    if (slot.state !== 'ready' && slot.state !== 'starting') {
      this.failQueued(job, new RuntimeError('WORKER_FAILED', 'Worker is unavailable'));
      return;
    }
    this.queue.delete(job);
    this.scheduler.remove(job);
    clearTimeout(job.queueTimer);
    clearTimeout(slot.idleTimer);
    this.reserveJob(job);
    this.preparationWindow.delete(job);
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

    this.active++;
    job.workerStartedAt = performance.now();
    job.state = 'starting';
    job.phase = 'startup';
    void this.run(job, slot);
  }
  private async run(job: Job, slot: Slot): Promise<void> {
    try {
      await slot.ready.promise;
      job.timing.startupMs =
        performance.now() - required(job.workerStartedAt, 'Worker admission timestamp');
      job.controller.signal.throwIfAborted();
      if (!slot.tasks.has(job.name))
        throw new RuntimeError('UNKNOWN_TASK', `Worker does not implement ${job.name}`);
      job.state = 'preparing';
      job.phase = 'prepare';
      const started = performance.now();
      let prepared: PreparedInput<unknown>;
      const asynchronouslyPrepared = job.preparedInput !== undefined;
      try {
        const value = job.preparedInput ?? job.options.prepare({ signal: job.controller.signal });
        if (value && typeof (value as unknown as { then?: unknown }).then === 'function') {
          void Promise.resolve(value).catch(() => {});
          throw new RuntimeError(
            'INVALID_ARGUMENT',
            'prepare must return synchronously; perform asynchronous work in the Worker',
          );
        }
        if (!value || !Object.hasOwn(value, 'payload'))
          throw new RuntimeError('INVALID_ARGUMENT', 'prepare must return a payload');
        prepared = value;
        job.preparedInput = undefined;
        if (
          performance.now() - required(job.admittedAt, 'Admission timestamp') >=
          required(job.options.executionTimeoutMs, 'Execution timeout')
        )
          throw new RuntimeError('EXECUTION_TIMEOUT', 'Preparation exceeded execution deadline');
      } finally {
        if (!asynchronouslyPrepared) job.timing.prepareMs = performance.now() - started;
      }
      job.controller.signal.throwIfAborted();
      if (slot.state !== 'ready')
        throw new RuntimeError('WORKER_FAILED', 'Worker was lost during input preparation');
      validateBlobTransfers(prepared.transfer);
      const payload = encodePacket(
        prepared.payload,
        job.options.budget.inputBytes,
        job.options.blobLimits!.inputBytes,
      );
      const bytes = packetBytes(payload);
      if (bytes > job.options.budget.inputBytes)
        throw new RuntimeError('BUDGET_EXCEEDED', 'Prepared input exceeds reserved inputBytes');
      job.state = 'running';
      job.phase = 'execute';
      job.postedAt = performance.now();
      slot.endpoint.postMessage(
        {
          ...header(slot.epoch),
          type: 'request',
          id: job.id,
          scope: job.scope.id,
          session: job.session?.id,
          task: job.name,
          payload,
          maxScratchBytes: job.options.budget.scratchBytes,
          maxOutputBytes: job.options.budget.outputBytes,
          maxOutputBlobBytes: job.options.blobLimits!.outputBytes,
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
      const pending = slot.releases.get(message.scope);
      if (!pending) return;
      if (
        !Number.isSafeInteger(message.cacheBytes) ||
        message.cacheBytes < 0 ||
        message.cacheBytes > slot.pool.options.cacheBytes ||
        (message.error && typeof message.error.message !== 'string')
      ) {
        void this.retire(
          slot,
          new RuntimeError('PROTOCOL_ERROR', 'Invalid release acknowledgement'),
        );
        return;
      }
      clearTimeout(pending.timer);
      slot.releases.delete(message.scope);
      if (message.error) {
        try {
          pending.deferred.reject(decodeError(message.error));
        } catch (error) {
          pending.deferred.reject(error);
          void this.retire(slot, asError(error));
        }
        this.armIdle(slot);
        this.schedule();
        return;
      }
      pending.deferred.resolve();
      if (
        Number.isSafeInteger(message.cacheBytes) &&
        message.cacheBytes >= 0 &&
        message.cacheBytes <= slot.pool.options.cacheBytes
      )
        slot.cacheUsed = message.cacheBytes;
      this.armIdle(slot);
      this.schedule();
      return;
    }
    const job = slot.job;
    if (!job || job.phase === 'done' || message.id !== job.id || message.scope !== job.scope.id)
      return;
    if (job.phase !== 'execute' || job.postedAt === undefined) {
      void this.retire(slot, new RuntimeError('PROTOCOL_ERROR', 'Response before dispatch'));
      return;
    }
    if (message.type === 'progress') {
      try {
        validateProgress(message.value);
      } catch (error) {
        void this.retire(slot, asError(error));
        return;
      }
      if (!job.cancelled && !job.scope.closed && job.options.onProgress) {
        try {
          job.options.onProgress(message.value);
        } catch (error) {
          this.observe(
            new RuntimeError('REMOTE_ERROR', 'Progress observer threw', { cause: error }),
          );
        }
      }
      try {
        slot.endpoint.postMessage({
          ...header(slot.epoch),
          type: 'progress-ack',
          id: job.id,
          scope: job.scope.id,
        });
      } catch (error) {
        void this.retire(slot, asError(error));
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
        const bytes = packetBytes(message.value);
        if (packetBlobBytes(message.value) > job.options.blobLimits!.outputBytes)
          throw new RuntimeError(
            'BUDGET_EXCEEDED',
            'Worker result exceeds logical blob byte limit',
          );
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
      try {
        this.finish(job, undefined, decodeError(message.error));
      } catch (error) {
        void this.retire(slot, asError(error));
      }
    }
  }
  private cancel(job: Job, reason?: unknown): void {
    if (job.phase === 'done' || job.cancelled) return;
    job.cancelled = true;
    job.controller.abort(aborted(reason));
    job.result.reject(aborted(reason));
    job.removeSignal?.();
    job.removeSignal = undefined;
    if (job.state === 'queued' || job.phase === 'prepared') {
      this.finish(job, undefined, aborted(reason));
      return;
    }
    job.state = 'cancelling';
    const slot = job.slot;
    if (!slot || job.postedAt === undefined) return; // Startup is bounded by its physical deadline.
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
    if (job.phase === 'done') return;
    const error = new RuntimeError(
      'EXECUTION_TIMEOUT',
      'Task exceeded its physical execution deadline',
    );
    job.result.reject(error);
    job.controller.abort(error);
    if (job.postedAt !== undefined && job.slot) void this.retire(job.slot, error);
    // Synchronous prepare cannot be preempted; run checks elapsed time on return.
    else if (job.phase === 'startup' && job.slot) void this.retire(job.slot, error);
    else if (job.phase === 'prepared') this.finish(job, undefined, error);
    else if (job.phase === 'produce') job.state = 'cancelling';
  }
  private failQueued(job: Job, error: Error): void {
    if (job.phase === 'done' || job.state !== 'queued') return;
    this.finish(job, undefined, error);
  }
  private finish(job: Job, result?: { value: Packet; bytes: number }, error?: Error): void {
    if (job.phase === 'done') return;
    job.phase = 'done';
    if (this.reservation === job) this.reservation = undefined;
    clearTimeout(job.queueTimer);
    clearTimeout(job.executionTimer);
    clearTimeout(job.preparationTimer);
    job.removeSignal?.();
    job.removeSignal = undefined;
    this.queue.delete(job);
    this.scheduler.remove(job);
    this.jobs.delete(job.id);
    job.scope.jobs.delete(job);

    if (job.slot !== undefined) this.active--;
    this.preparationWindow.delete(job);
    if (job.reserved && !job.options.discardResult) this.resultReservations--;
    job.preparedInput = undefined;
    job.preparation = undefined;
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
      if (job.options.discardResult) lease.release();
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
  private releaseScope(slot: Slot, scope: string): Promise<void> {
    const existing = slot.releases.get(scope);
    if (existing) return existing.deferred.promise;
    const result = deferred<void>();
    clearTimeout(slot.idleTimer);
    const timer = setTimeout(() => {
      slot.releases.delete(scope);
      this.armIdle(slot);
      this.schedule();
      result.reject(
        new RuntimeError('EXECUTION_TIMEOUT', 'Worker scope release acknowledgement timed out'),
      );
    }, this.options.releaseTimeoutMs);
    slot.releases.set(scope, { deferred: result, timer });
    try {
      slot.endpoint.postMessage({ ...header(slot.epoch), type: 'release-scope', scope });
    } catch (error) {
      void this.retire(slot, asError(error));
    }
    return result.promise;
  }
  /** Stops waiting after the deadline; it does not pretend that preparation or termination stopped. */
  disposeWithin(timeoutMs: number): Promise<void> {
    return waitWithin(this.dispose(), timeoutMs);
  }
  /** Caller-triggered diagnostics avoid one timer per retained result/scope. */
  resourceDiagnostics(): {
    scopes: number;
    leases: number;
    preparing: string[];
    prepared: string[];
    quarantinedWorkers: number;
    owners: { id: string; label: string; tasks: number; leases: number; sessions: number }[];
  } {
    return {
      scopes: this.scopes.size,
      leases: this.leaseCount,
      preparing: [...this.jobs.values()]
        .filter((job) => job.phase === 'prepare' || job.phase === 'produce')
        .map((job) => job.id),
      prepared: [...this.preparationWindow]
        .filter((job) => job.phase === 'prepared')
        .map((job) => job.id),
      quarantinedWorkers: this.stats.quarantinedWorkers,
      owners: [...this.scopes].map((scope) => ({
        id: scope.id,
        label: scope.label,
        tasks: scope.jobs.size,
        leases: scope.leases.size,
        sessions: scope.sessions.size,
      })),
    };
  }
  /** Retry only workers whose physical termination failed; credits remain held until success. */
  async retryTermination(): Promise<void> {
    await Promise.all(
      this.slots()
        .filter((slot) => slot.terminationFailed)
        .map((slot) => {
          slot.stopped = undefined;
          return this.retire(slot, new RuntimeError('CLOSED', 'Retrying physical termination'));
        }),
    );
    if (this.closed) this.disposal = undefined;
  }
  async withScope<R>(label: string, work: (scope: RuntimeScope<T>) => Promise<R>): Promise<R> {
    const scope = this.createScope(label);
    try {
      return await work(scope);
    } finally {
      await scope.dispose();
    }
  }
  private armIdle(slot: Slot): void {
    if (
      this.closed ||
      slot.session ||
      slot.job ||
      slot.state !== 'ready' ||
      slot.releases.size > 0 ||
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
    slot.terminationFailed = false;
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
        if (pending.session !== slot.session) continue;
        if (pending.phase === 'produce') {
          pending.controller.abort(slot.session.lost);
          pending.result.reject(slot.session.lost);
          pending.state = 'cancelling';
        } else if (pending.phase === 'prepared' || pending.state === 'queued')
          this.finish(pending, undefined, slot.session.lost);
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
        slot.terminationFailed = true;
        for (const pending of slot.releases.values()) {
          clearTimeout(pending.timer);
          pending.deferred.reject(failure);
        }
        slot.releases.clear();
        stopped.reject(failure);
        return;
      }
      slot.state = 'closed';
      for (const pending of slot.releases.values()) {
        clearTimeout(pending.timer);
        pending.deferred.resolve();
      }
      slot.releases.clear();
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
  enqueuePrepared<K extends TaskName<T>>(
    name: K,
    options: PreparedTaskOptions<T[K]['input']>,
  ): TaskHandle<T[K]['output']> {
    return this.runtime._enqueuePrepared(this.record, name, options);
  }
  dispose(): Promise<void> {
    return this.runtime._disposeScope(this.record);
  }
  disposeWithin(timeoutMs: number): Promise<void> {
    return waitWithin(this.dispose(), timeoutMs);
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
  enqueuePrepared<K extends TaskName<T>>(
    name: K,
    options: SessionPreparedTaskOptions<T[K]['input']>,
  ): TaskHandle<T[K]['output']> {
    return this.runtime._enqueuePrepared(
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

function waitWithin(work: Promise<void>, timeoutMs: number): Promise<void> {
  timeout(timeoutMs, 'dispose timeout');
  let timer: Timer;
  const deadline = new Promise<never>((_, reject) => {
    timer = setTimeout(
      () =>
        reject(
          new RuntimeError(
            'EXECUTION_TIMEOUT',
            'Cleanup is still pending; inspect resourceDiagnostics()',
          ),
        ),
      timeoutMs,
    );
  });
  return Promise.race([work, deadline]).finally(() => clearTimeout(timer));
}
