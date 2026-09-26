import { decodeError } from '../remote-error.js';
import { validateProgress } from '../progress.js';
import {
  checkBlobLimit,
  encodePacket,
  packetBytes,
  packetBlobBytes,
  validateBlobTransfers,
  type Packet,
} from '../packet.js';
import {
  aborted,
  asError,
  integer,
  required,
  RuntimeError,
  SessionAdmissionError,
  timeout,
} from '../errors.js';
import { type FromWorker, header, isHeader } from '../protocol.js';
import { BudgetLedger, validateTaskBudget } from '../resources/budget.js';
import { OwnedResult } from '../resources/lease.js';
import { ResidentLease } from '../resources/resident.js';
import {
  addCacheStats,
  cacheReport,
  emptyCacheStats,
  emptyReclaimStats,
} from '../resources/telemetry.js';
import type {
  AdaptivePoolOptions,
  InteractiveReserve,
  MaintenanceReport,
  MemoryPressure,
  PoolSizing,
  TrimOptions,
  ReclaimReason,
  ReclaimStats,
  ResourceCacheSnapshot,
  Priority,
  CacheStats,
  AdmissionBlocker,
  RuntimeDiagnostics,
  SessionAdmissionOptions,
  SessionOptions,
  ResourceOptions,
  ResourceLease,
  ResourceReservations,
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
  options: Required<Omit<PoolOptions, 'adaptive'>> & { adaptive?: Required<AdaptivePoolOptions> };
  capacity: number;
  cacheTarget: number;
  resourceCacheStats: CacheStats;
  reclaim: ReclaimStats;
  lastDemand: number;
  lastSample: CacheStats;
  lastSampleAt: number;
  slots: Set<Slot>;
  cacheStats: CacheStats;
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
  cacheStats: CacheStats;
  resourceCacheStats: CacheStats;
  reports: ResourceCacheSnapshot[];
  cacheLimit: number;
  priority: Priority;
  control?: {
    id: string;
    target: number;
    extra?: () => void;
    result: Deferred<void>;
    timer?: Timer;
  };
  reclaim?: Promise<void>;
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
  resources: Set<ResourceLease>;
  touched: Set<Slot>;
  disposal?: Promise<void>;
}
interface SessionRecord {
  priority: Priority;
  id: string;
  scope: ScopeRecord;
  pool: string;
  slot?: Slot;
  closed: boolean;
  lost?: RuntimeError;
  leases: Set<OwnedResult<unknown>>;
  resources: Set<ResourceLease>;
  resident?: ResourceLease;
  /** Disposal must await a factory that may synchronously cancel its own admission. */
  binding?: Promise<void>;
  disposal?: Promise<void>;
  reclaimable: boolean;
  reclaimPriority: number;
  reclaimed: boolean;
  delivering: boolean;
}
interface SessionRequest<T extends Catalog<T>> {
  session: SessionRecord;
  result: Deferred<WorkerSession<T>>;
  mode: 'immediate' | 'wait';
  residentBytes?: number;
  timer?: Timer;
  removeSignal?: () => void;
}
interface Job {
  candidates?: readonly SessionRecord[];
  admissionTimer?: Timer;
  id: string;
  order: number;
  groupKey: string;
  laneKey: string;
  affinityKeys: readonly string[];
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
  private readonly pendingShrinks = new Map<Pool, { reason: ReclaimReason; sessions: boolean }>();
  private readonly scopes = new Set<ScopeRecord>();
  private readonly jobs = new Map<string, Job>();
  private readonly queue = new Set<Job>();
  private readonly acquisitions = new Map<SessionRecord, SessionRequest<T>>();
  private readonly ledger: BudgetLedger;
  private readonly affinity = new Map<string, Set<Slot>>();
  private readonly resourceLeases = new Set<ResourceLease>();
  readonly resources: ResourceReservations = {
    acquire: (options) => this._acquireResource(options),
  };
  private readonly scheduler: Scheduler<Job>;
  private reservation?: Job;
  private reclaiming = false;
  private readonly options: Required<
    Omit<RuntimeOptions, 'pools' | 'budgets' | 'onDiagnostic' | 'interactiveReserve'>
  >;
  private readonly interactive: Required<Omit<InteractiveReserve, 'budgets'>>;
  private nonInteractiveActive = 0;
  private nonInteractiveResults = 0;
  private memoryPressure: MemoryPressure = 'normal';
  private maintenance?: Promise<MaintenanceReport>;
  private adaptiveTimer?: Timer;
  private schedulerTimer?: Timer;
  private pressureTargets = new Map<Pool, Required<PoolSizing>>();
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
    sessionsReclaimed: 0,
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
      const cacheBytes = integer(pool.cacheBytes ?? 0, `${name}.cacheBytes`);
      const interactiveWorkers = integer(pool.interactiveWorkers ?? 0, 'interactiveWorkers');
      if (interactiveWorkers > size)
        throw new RuntimeError('INVALID_ARGUMENT', 'Interactive Worker reserve exceeds pool size');
      const adaptive = pool.adaptive
        ? {
            minWorkers: integer(
              pool.adaptive.minWorkers ?? Math.max(1, interactiveWorkers),
              'minWorkers',
              1,
            ),
            minCacheBytes: integer(
              pool.adaptive.minCacheBytes ?? Math.floor(cacheBytes / 4),
              'minCacheBytes',
            ),
            sampleMs: timeout(pool.adaptive.sampleMs ?? 1000, 'adaptive sampleMs'),
            idleMs: timeout(pool.adaptive.idleMs ?? 30_000, 'adaptive idleMs'),
            missRatio: pool.adaptive.missRatio ?? 0.2,
          }
        : undefined;
      if (
        adaptive &&
        (adaptive.minWorkers > size ||
          adaptive.minWorkers < interactiveWorkers ||
          adaptive.minCacheBytes > cacheBytes ||
          !Number.isFinite(adaptive.missRatio) ||
          adaptive.missRatio < 0 ||
          adaptive.missRatio > 1)
      )
        throw new RuntimeError('INVALID_ARGUMENT', 'Invalid adaptive pool bounds');
      capacity += size;
      this.pools.set(name, {
        options: {
          ...pool,
          adaptive,
          size,
          interactiveWorkers,
          cacheBytes,
          cacheEntries: integer(pool.cacheEntries ?? 4096, `${name}.cacheEntries`, 1),
          allowHardCancel: pool.allowHardCancel ?? false,
          idleTimeoutMs:
            pool.idleTimeoutMs === 0 ? 0 : timeout(pool.idleTimeoutMs ?? 30_000, 'idleTimeoutMs'),
        },
        slots: new Set(),
        capacity: adaptive?.minWorkers ?? size,
        cacheTarget: adaptive?.minCacheBytes ?? cacheBytes,
        lastDemand: performance.now(),
        lastSample: emptyCacheStats(),
        lastSampleAt: performance.now(),
        resourceCacheStats: emptyCacheStats(),
        reclaim: emptyReclaimStats(),
        cacheStats: { hits: 0, misses: 0, evictions: 0 },
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
      maxResourceLeases: integer(options.maxResourceLeases ?? 4096, 'maxResourceLeases', 1),
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
      residentBytes: options.budgets?.residentBytes ?? 128 * MiB,
    };
    const reserve = options.interactiveReserve ?? {};
    this.interactive = {
      workers: integer(reserve.workers ?? 0, 'interactive workers'),
      activeTasks: integer(reserve.activeTasks ?? 0, 'interactive activeTasks'),
      preparingTasks: integer(reserve.preparingTasks ?? 0, 'interactive preparingTasks'),
      resultLeases: integer(reserve.resultLeases ?? 0, 'interactive resultLeases'),
    };
    if (
      this.interactive.workers > this.options.maxWorkers ||
      this.interactive.activeTasks > this.options.maxActiveTasks ||
      this.interactive.preparingTasks > this.options.maxPreparingTasks ||
      this.interactive.resultLeases > this.options.maxResultLeases
    )
      throw new RuntimeError('INVALID_ARGUMENT', 'Interactive reserve exceeds runtime capacity');
    this.ledger = new BudgetLedger(budgets, reserve.budgets);
    this.scheduler = new Scheduler(this.options.ageingMs, 4096, options.priorityPolicy ?? 'strict');
    for (const pool of this.pools.values()) {
      this.ledger.validate({ cacheBytes: pool.options.cacheBytes });
    }
    this.diagnostic = options.onDiagnostic;
    this.armAdaptive();
  }

  get stats(): RuntimeStats {
    const slots = this.slots();
    const preparationReserved: TaskBudget = { inputBytes: 0, scratchBytes: 0, outputBytes: 0 };
    for (const job of this.preparationWindow)
      for (const key of ['inputBytes', 'scratchBytes', 'outputBytes'] as const)
        preparationReserved[key] += job.cost[key];
    return {
      resourceCacheStats: [...this.pools.values()].reduce((sum, p) => {
        addCacheStats(sum, p.resourceCacheStats);
        return sum;
      }, emptyCacheStats()),
      reclaim: this.reclaimStats(),
      scheduler: this.scheduler.stats,
      cacheStats: [...this.pools.values()].reduce(
        (sum, pool) => ({
          hits: Math.min(Number.MAX_SAFE_INTEGER, sum.hits + pool.cacheStats.hits),
          misses: Math.min(Number.MAX_SAFE_INTEGER, sum.misses + pool.cacheStats.misses),
          evictions: Math.min(Number.MAX_SAFE_INTEGER, sum.evictions + pool.cacheStats.evictions),
        }),
        { hits: 0, misses: 0, evictions: 0 },
      ),
      queued: this.queue.size,
      active: this.active,
      preparing: [...this.preparationWindow].filter((job) => job.phase === 'produce').length,
      prepared: [...this.preparationWindow].filter((job) => job.phase === 'prepared').length,
      preparationReserved,
      workers: slots.length,
      closingWorkers: slots.filter((slot) => slot.state === 'closing').length,
      leases: this.leaseCount,
      resourceLeases: this.resourceLeases.size,
      scopes: this.scopes.size,
      quarantinedWorkers: slots.filter((slot) => slot.terminationFailed).length,
      reserved: { ...this.ledger.used },
      peakReserved: { ...this.ledger.peak },
      cacheUsedBytes: slots.reduce((sum, slot) => sum + slot.cacheUsed, 0),
      ...this.counters,
    };
  }
  private priority(value?: Priority): Priority {
    if (value !== undefined && !Object.hasOwn(priorities, value))
      throw new RuntimeError('INVALID_ARGUMENT', 'Unknown admission priority');
    return value ?? 'foreground';
  }
  private slotClass(slot: Slot, priority: Priority): boolean {
    return (
      priority === 'interactive' ||
      slot.priority !== 'interactive' ||
      !(
        this.interactive.workers ||
        slot.pool.options.interactiveWorkers ||
        this.ledger.protected.cacheBytes
      )
    );
  }
  private canSpawn(pool: Pool, priority: Priority): boolean {
    const slots = this.slots();
    return (
      pool.slots.size < pool.capacity &&
      slots.length < this.options.maxWorkers &&
      this.ledger.fits({ cacheBytes: pool.cacheTarget }, priority) &&
      (priority === 'interactive' ||
        (slots.filter((s) => s.priority !== 'interactive').length <
          this.options.maxWorkers - this.interactive.workers &&
          [...pool.slots].filter((s) => s.priority !== 'interactive').length <
            pool.capacity - pool.options.interactiveWorkers))
    );
  }
  private classBlocked(pool: Pool, priority: Priority): boolean {
    if (priority === 'interactive') return false;
    return (
      (!!this.interactive.workers &&
        this.slots().filter((s) => s.priority !== 'interactive').length >=
          this.options.maxWorkers - this.interactive.workers) ||
      (!!pool.options.interactiveWorkers &&
        [...pool.slots].filter((s) => s.priority !== 'interactive').length >=
          pool.capacity - pool.options.interactiveWorkers) ||
      (!this.ledger.fits({ cacheBytes: pool.cacheTarget }, priority) &&
        this.ledger.fits({ cacheBytes: pool.cacheTarget }))
    );
  }
  private reclaimStats(): ReclaimStats {
    const sum = emptyReclaimStats();
    for (const { reclaim: r } of this.pools.values()) {
      sum.attempts += r.attempts;
      sum.succeeded += r.succeeded;
      sum.failed += r.failed;
      for (const reason of Object.keys(sum.byReason) as ReclaimReason[])
        sum.byReason[reason] += r.byReason[reason];
    }
    return sum;
  }
  /** Change admission targets within configured hard bounds; busy and required Sessions are retained. */
  resizePool(name: string, sizing: PoolSizing): Promise<MaintenanceReport> {
    return this.runMaintenance(async () => {
      const pool = this.pools.get(name);
      if (!pool) throw new RuntimeError('INVALID_ARGUMENT', `Unknown pool: ${name}`);
      return this.adjustPool(pool, sizing, 'resize', true);
    });
  }
  /** Application-driven pressure signal; normal restores the targets saved before pressure. */
  setMemoryPressure(level: MemoryPressure): Promise<MaintenanceReport> {
    return this.runMaintenance(async () => {
      if (!['normal', 'moderate', 'critical'].includes(level))
        throw new RuntimeError('INVALID_ARGUMENT', 'Unknown memory pressure level');
      this.memoryPressure = level;
      const reports: MaintenanceReport[] = [];
      for (const pool of this.pools.values()) {
        if (level === 'normal') {
          const target = this.pressureTargets.get(pool);
          if (target) reports.push(await this.adjustPool(pool, target, 'pressure', true));
        } else {
          if (!this.pressureTargets.has(pool))
            this.pressureTargets.set(pool, { size: pool.capacity, cacheBytes: pool.cacheTarget });
          const base = this.pressureTargets.get(pool)!;
          reports.push(
            await this.adjustPool(
              pool,
              {
                size: Math.max(
                  1,
                  pool.options.interactiveWorkers,
                  level === 'critical' ? 1 : Math.ceil(base.size / 2),
                ),
                cacheBytes: level === 'critical' ? 0 : Math.floor(base.cacheBytes / 2),
              },
              'pressure',
              true,
              level === 'critical' ? 0 : undefined,
            ),
          );
        }
      }
      if (level === 'normal') this.pressureTargets.clear();
      return this.combineReports(reports);
    });
  }
  trim(options: TrimOptions = {}): Promise<MaintenanceReport> {
    return this.runMaintenance(async () => {
      if (
        !options ||
        (options.reclaimSessions !== undefined && typeof options.reclaimSessions !== 'boolean')
      )
        throw new RuntimeError('INVALID_ARGUMENT', 'Invalid trim options');
      const pools = options.pool ? [this.pools.get(options.pool)] : [...this.pools.values()];
      if (pools.some((p) => !p)) throw new RuntimeError('INVALID_ARGUMENT', 'Unknown trim pool');
      const workers = integer(options.workersPerPool ?? 0, 'trim workers');
      const reports: MaintenanceReport[] = [];
      for (const pool of pools as Pool[])
        reports.push(
          await this.adjustPool(
            pool,
            {
              size: Math.max(1, pool.options.interactiveWorkers, Math.min(pool.capacity, workers)),
              cacheBytes: options.cacheBytesPerWorker ?? 0,
            },
            'pressure',
            options.reclaimSessions ?? true,
            workers,
          ),
        );
      return this.combineReports(reports);
    });
  }
  private combineReports(reports: MaintenanceReport[]): MaintenanceReport {
    return {
      workersReclaimed: reports.reduce((n, r) => n + r.workersReclaimed, 0),
      cacheBytesReleased: reports.reduce((n, r) => n + r.cacheBytesReleased, 0),
      failures: reports.flatMap((r) => [...r.failures]),
    };
  }
  private runMaintenance(work: () => Promise<MaintenanceReport>): Promise<MaintenanceReport> {
    if (this.closed) return Promise.reject(new RuntimeError('CLOSED', 'Runtime is closed'));
    if (this.maintenance)
      return Promise.reject(
        new RuntimeError('INVALID_ARGUMENT', 'Await the preceding maintenance operation'),
      );
    const result = Promise.resolve()
      .then(work)
      .finally(() => {
        this.maintenance = undefined;
        this.wakeReclamation();
      });
    this.maintenance = result;
    return result;
  }
  private async adjustPool(
    pool: Pool,
    sizing: PoolSizing,
    reason: ReclaimReason,
    sessions: boolean,
    liveTarget?: number,
  ): Promise<MaintenanceReport> {
    const size = integer(sizing.size ?? pool.capacity, 'pool size', 1);
    const cacheBytes = integer(sizing.cacheBytes ?? pool.cacheTarget, 'pool cacheBytes');
    if (
      size > pool.options.size ||
      size < pool.options.interactiveWorkers ||
      cacheBytes > pool.options.cacheBytes
    )
      throw new RuntimeError('INVALID_ARGUMENT', 'Pool targets exceed configured bounds');
    pool.capacity = size;
    pool.cacheTarget = cacheBytes;
    if (this.shrinkNeeded(pool)) this.pendingShrinks.set(pool, { reason, sessions });
    else this.pendingShrinks.delete(pool);
    this.schedule(this.poolKey(pool), 'workers', 'cache-budget');
    const failures: { pool: string; worker: number; message: string }[] = [];
    let workersReclaimed = 0;
    const before = [...pool.slots].reduce((n, s) => n + s.cacheLimit, 0);
    const name = [...this.pools].find(([, value]) => value === pool)![0];
    const eligible = (slot: Slot) =>
      (this.available(slot) && slot.state === 'ready') ||
      (sessions && this.reclaimableSession(slot));
    const victims = this.shrinkCandidates(pool, sessions);
    for (const slot of victims) {
      if (this.closed || !this.shrinkNeeded(pool, undefined, liveTarget)) break;
      // Earlier cleanup may have yielded while another candidate received work or a lease.
      if (!eligible(slot) || !this.shrinkNeeded(pool, slot.priority, liveTarget)) continue;
      try {
        await this.reclaimSlot(slot, reason);
        workersReclaimed++;
      } catch (error) {
        failures.push({ pool: name, worker: slot.epoch, message: asError(error).message });
      }
    }
    for (const slot of [...pool.slots]) {
      if (this.closed) break;
      if (slot.state === 'closed' || slot.state === 'closing') continue;
      if (reason === 'adaptive' && (slot.job || slot.state === 'starting')) continue;
      try {
        await this.resizeCache(slot, cacheBytes, cacheBytes < slot.cacheLimit);
      } catch (error) {
        failures.push({ pool: name, worker: slot.epoch, message: asError(error).message });
      }
    }
    this.schedule(`pool:${name}`, 'workers', 'cache-budget');
    const after = [...pool.slots].reduce((n, s) => n + s.cacheLimit, 0);
    return { workersReclaimed, cacheBytesReleased: Math.max(0, before - after), failures };
  }
  private liveSlots(pool: Pool): Slot[] {
    // In-flight termination still holds physical credits, but is already a selected victim.
    return [...pool.slots].filter(
      (slot) => slot.state !== 'closing' && slot.state !== 'closed' && !slot.session?.closed,
    );
  }
  private shrinkNeeded(pool: Pool, priority?: Priority, target = pool.capacity): boolean {
    const slots = this.liveSlots(pool);
    // Explicit trim/critical pressure may discard idle Workers below the future admission floor.
    if (target < pool.capacity) return slots.length > target;
    if (
      slots.filter((slot) => slot.priority !== 'interactive').length >
      Math.max(0, target - pool.options.interactiveWorkers)
    )
      return priority !== 'interactive';
    return slots.length > target;
  }
  private shrinkCandidates(pool: Pool, sessions: boolean): Slot[] {
    const nonInteractiveFirst = this.shrinkNeeded(pool) && !this.shrinkNeeded(pool, 'interactive');
    return [...pool.slots]
      .filter(
        (slot) =>
          (this.available(slot) && slot.state === 'ready') ||
          (sessions && this.reclaimableSession(slot)),
      )
      .sort(
        (a, b) =>
          (nonInteractiveFirst
            ? Number(a.priority === 'interactive') - Number(b.priority === 'interactive')
            : 0) ||
          Number(!!a.session) - Number(!!b.session) ||
          (a.session?.reclaimPriority ?? 0) - (b.session?.reclaimPriority ?? 0) ||
          a.used - b.used,
      );
  }
  private reconcileShrinks(): void {
    if (this.maintenance) return;
    for (const [pool, { reason, sessions }] of this.pendingShrinks) {
      for (const slot of this.shrinkCandidates(pool, sessions)) {
        if (!this.shrinkNeeded(pool)) break;
        if (
          !this.shrinkNeeded(pool, slot.priority) ||
          !(
            (this.available(slot) && slot.state === 'ready') ||
            (sessions && this.reclaimableSession(slot))
          )
        )
          continue;
        void this.reclaimSlot(slot, reason).catch((cause) =>
          this.observe(
            new RuntimeError(
              'WORKER_FAILED',
              'Deferred pool shrink failed; capacity remains held',
              {
                cause,
              },
            ),
          ),
        );
      }
      if (!this.shrinkNeeded(pool)) this.pendingShrinks.delete(pool);
    }
  }
  private async resizeCache(slot: Slot, limit: number, trim: boolean): Promise<void> {
    await slot.ready.promise;
    if (slot.state !== 'ready' || this.closed)
      throw new RuntimeError('CLOSED', 'Worker is closing');
    if (slot.control) return slot.control.result.promise;
    if (limit === slot.cacheLimit && !trim) return;
    const extra =
      limit > slot.cacheLimit
        ? this.ledger.reserve({ cacheBytes: limit - slot.cacheLimit }, slot.priority)
        : undefined;
    const id = `cache-${++this.serial}`,
      result = deferred<void>();
    const control: NonNullable<Slot['control']> = { id, target: limit, extra, result };
    slot.control = control;
    clearTimeout(slot.idleTimer);
    // The acknowledgement deadline covers cleanup, not a preceding business task.
    // Mark the Slot first so subsequent jobs cannot race the quiescent boundary.
    const running = slot.job;
    void (async () => {
      if (running) await running.settled.promise;
      if (slot.control !== control || slot.state !== 'ready') return;
      control.timer = setTimeout(() => {
        void this.retire(
          slot,
          new RuntimeError('EXECUTION_TIMEOUT', 'Cache control acknowledgement timed out'),
        );
      }, this.options.releaseTimeoutMs);
      slot.endpoint.postMessage({ ...header(slot.epoch), type: 'cache-control', id, limit, trim });
    })().catch((error) => {
      void this.retire(slot, asError(error));
    });
    return result.promise;
  }
  private armAdaptive(): void {
    const pools = [...this.pools.values()].filter((p) => p.options.adaptive);
    if (this.closed || !pools.length) return;
    this.adaptiveTimer = setTimeout(
      () => {
        void (async () => {
          if (this.memoryPressure !== 'normal' || this.maintenance) return;
          await this.runMaintenance(async () => {
            const reports: Promise<MaintenanceReport>[] = [];
            for (const pool of pools) {
              const config = pool.options.adaptive!,
                now = performance.now();
              if (now - pool.lastSampleAt < config.sampleMs) continue;
              pool.lastSampleAt = now;
              const name = [...this.pools].find(([, p]) => p === pool)![0];
              const queued =
                [...this.queue].filter((j) => j.options.pool === name).length +
                [...this.acquisitions.keys()].filter((s) => s.pool === name && !s.slot).length;
              if (queued || [...pool.slots].some((s) => s.job)) pool.lastDemand = now;
              const totals = { ...pool.cacheStats };
              addCacheStats(totals, pool.resourceCacheStats);
              const misses = totals.misses - pool.lastSample.misses,
                hits = totals.hits - pool.lastSample.hits;
              const evictions = totals.evictions - pool.lastSample.evictions;
              pool.lastSample = totals;
              const idle = now - pool.lastDemand >= config.idleMs;
              const size = idle
                ? config.minWorkers
                : Math.min(pool.options.size, pool.capacity + Number(queued > 0));
              const cacheBytes = idle
                ? config.minCacheBytes
                : misses > 0 &&
                    (evictions > 0 || [...pool.slots].some((s) => s.cacheUsed >= s.cacheLimit)) &&
                    misses / Math.max(1, hits + misses) >= config.missRatio
                  ? Math.min(
                      pool.options.cacheBytes,
                      Math.max(pool.cacheTarget + 1, Math.ceil(pool.cacheTarget * 1.5)),
                    )
                  : pool.cacheTarget;
              const reconcile = [...pool.slots].some(
                (slot) =>
                  (slot.cacheLimit !== pool.cacheTarget &&
                    slot.state === 'ready' &&
                    !slot.job &&
                    !slot.control &&
                    !slot.releases.size &&
                    (slot.cacheLimit > pool.cacheTarget ||
                      this.ledger.fits(
                        { cacheBytes: pool.cacheTarget - slot.cacheLimit },
                        slot.priority,
                      ))) ||
                  (this.shrinkNeeded(pool, slot.priority, size) &&
                    ((this.available(slot) && slot.state === 'ready') ||
                      this.reclaimableSession(slot))),
              );
              if (size !== pool.capacity || cacheBytes !== pool.cacheTarget || reconcile)
                reports.push(this.adjustPool(pool, { size, cacheBytes }, 'adaptive', true));
            }
            const report = this.combineReports(await Promise.all(reports));
            for (const failure of report.failures)
              this.observe(
                new RuntimeError(
                  'WORKER_FAILED',
                  `Adaptive maintenance failed in ${failure.pool}: ${failure.message}`,
                ),
              );
            return report;
          });
        })()
          .catch((error) =>
            this.observe(
              new RuntimeError('WORKER_FAILED', 'Adaptive maintenance failed', { cause: error }),
            ),
          )
          .finally(() => this.armAdaptive());
      },
      Math.min(...pools.map((p) => p.options.adaptive!.sampleMs)),
    );
  }

  /** Independent read-only snapshots; reading diagnostics never admits work. */
  diagnostics(): RuntimeDiagnostics {
    const waiting: RuntimeDiagnostics['waiting'][number][] = [];
    for (const job of this.queue)
      waiting.push({
        id: job.id,
        pool: job.options.pool,
        session: job.session?.id,
        kind: 'task',
        reasons: this.taskBlockers(job),
      });
    for (const { session } of this.acquisitions.values())
      waiting.push({
        id: session.id,
        pool: session.pool,
        session: session.id,
        kind: 'session',
        reasons: this.sessionBlockers(session),
      });
    return {
      memoryPressure: this.memoryPressure,
      limits: {
        ...this.ledger.limits,
        maxWorkers: this.options.maxWorkers,
        maxActiveTasks: this.options.maxActiveTasks,
        maxPreparingTasks: this.options.maxPreparingTasks,
        maxResultLeases: this.options.maxResultLeases,
        maxQueuedTasks: this.options.maxQueuedTasks,
        maxResourceLeases: this.options.maxResourceLeases,
      },
      pools: [...this.pools].map(([name, pool]) => {
        const slots = [...pool.slots];
        return {
          name,
          cacheStats: { ...pool.cacheStats },
          resourceCacheStats: { ...pool.resourceCacheStats },
          resources: slots.flatMap((s) => s.reports.map((r) => ({ ...r, keys: [...r.keys] }))),
          reclaim: { ...pool.reclaim, byReason: { ...pool.reclaim.byReason } },
          maxCapacity: pool.options.size,
          maxCacheBytesPerWorker: pool.options.cacheBytes,
          adaptive: !!pool.options.adaptive,
          capacity: pool.capacity,
          workers: slots.length,
          starting: slots.filter((s) => s.state === 'starting').length,
          running: slots.filter((s) => s.state === 'ready' && s.job).length,
          idle: slots.filter((s) => this.available(s) && s.state === 'ready').length,
          sessionIdle: slots.filter((s) => s.state === 'ready' && s.session && !s.job).length,
          closing: slots.filter((s) => s.state === 'closing').length,
          quarantined: slots.filter((s) => s.terminationFailed).length,
          boundSessions: slots.filter((s) => s.session).length,
          reclaimableSessions: slots.filter((s) => this.reclaimableSession(s)).length,
          waitingSessions: [...this.acquisitions.keys()].filter((session) => session.pool === name)
            .length,
          queuedTasks: [...this.queue].filter((job) => job.options.pool === name).length,
          cacheBytesPerWorker: pool.cacheTarget,
          cacheReservedBytes: slots.reduce(
            (n, s) => n + s.cacheLimit + Math.max(0, (s.control?.target ?? 0) - s.cacheLimit),
            0,
          ),
          cacheUsedBytes: slots.reduce((sum, slot) => sum + slot.cacheUsed, 0),
          blockers: this.poolBlockers(pool),
        };
      }),
      waiting,
    };
  }
  private poolBlockers(pool: Pool, priority: Priority = 'interactive'): AdmissionBlocker[] {
    if ([...pool.slots].some((s) => this.availableFor(s, priority))) return [];
    const reasons: AdmissionBlocker[] = [];
    if (this.classBlocked(pool, priority)) reasons.push('interactive-reserve');
    if ([...pool.slots].some((s) => s.control)) reasons.push('maintenance');
    if (pool.slots.size >= pool.capacity) reasons.push('pool-capacity');
    if (this.slots().length >= this.options.maxWorkers) reasons.push('worker-capacity');
    if (!this.ledger.fits({ cacheBytes: pool.cacheTarget }, priority)) reasons.push('cache-budget');
    if (reasons.length) {
      if ([...pool.slots].some((s) => s.state === 'closing')) reasons.push('worker-closing');
      if ([...pool.slots].some((s) => s.session)) reasons.push('session-busy');
      if ([...pool.slots].some((s) => s.releases.size)) reasons.push('scope-release');
    }
    return reasons;
  }
  private waitingPrimary(pool: string): boolean {
    return [...this.acquisitions.keys()].some(
      (session) => session.pool === pool && !session.reclaimable && !session.slot,
    );
  }
  private sessionBlockers(session: SessionRecord): AdmissionBlocker[] {
    if (session.slot) return ['session-starting'];
    if (session.reclaimable && this.waitingPrimary(session.pool)) return ['session-priority'];
    const reasons = this.poolBlockers(this.pools.get(session.pool)!, session.priority);
    const bytes = this.acquisitions.get(session)?.residentBytes;
    if (bytes !== undefined) {
      if (!this.ledger.fits({ residentBytes: bytes }, session.priority)) {
        reasons.push('resident-budget');
        if (this.ledger.fits({ residentBytes: bytes }) && !reasons.includes('interactive-reserve'))
          reasons.push('interactive-reserve');
      }
      if (this.resourceLeases.size >= this.options.maxResourceLeases)
        reasons.push('resource-leases');
    }
    return reasons;
  }
  private protectedBudget(candidate: Job): boolean {
    const reserved = this.reservation;
    return (
      !!reserved &&
      reserved !== candidate &&
      reserved.phase !== 'done' &&
      this.canRun(reserved) &&
      this.scheduler.priority(candidate) >= this.scheduler.priority(reserved) &&
      (['inputBytes', 'scratchBytes', 'outputBytes'] as const).some(
        (key) =>
          candidate.cost[key] > 0 &&
          (reserved.cost[key] > this.ledger.available(key) ||
            (candidate.options.priority !== 'interactive' &&
              reserved.cost[key] > this.ledger.available(key, reserved.options.priority))),
      )
    );
  }
  private taskBlockers(job: Job): AdmissionBlocker[] {
    if (job.phase === 'produce') return ['preparing'];
    const reasons: AdmissionBlocker[] = [];
    if (!this.scheduler.isHead(job)) reasons.push('lane-order');
    const producing = !!job.preparation && !job.reserved;
    if (
      job.options.priority !== 'interactive' &&
      ((producing
        ? [...this.preparationWindow].filter((j) => j.options.priority !== 'interactive').length >=
          this.options.maxPreparingTasks - this.interactive.preparingTasks
        : this.nonInteractiveActive >=
          this.options.maxActiveTasks - this.interactive.activeTasks) ||
        (!job.reserved &&
          ((!job.options.discardResult &&
            this.nonInteractiveResults >=
              this.options.maxResultLeases - this.interactive.resultLeases) ||
            (!this.ledger.fits(job.cost, job.options.priority) && this.ledger.fits(job.cost)))))
    )
      reasons.push('interactive-reserve');
    if (producing && this.preparationWindow.size >= this.options.maxPreparingTasks)
      reasons.push('preparation-window');
    if (!producing && this.active >= this.options.maxActiveTasks) reasons.push('active-tasks');
    if (!job.reserved) {
      if (
        !job.options.discardResult &&
        this.resultReservations + this.leaseCount >= this.options.maxResultLeases
      )
        reasons.push('result-leases');
      for (const [key, reason] of [
        ['inputBytes', 'input-budget'],
        ['scratchBytes', 'scratch-budget'],
        ['outputBytes', 'output-budget'],
      ] as const)
        if (job.cost[key] > this.ledger.limits[key] - this.ledger.used[key]) reasons.push(reason);
      if (this.protectedBudget(job)) reasons.push('budget-reservation');
    }
    if (!producing) {
      const slot = job.session?.slot;
      if (job.session?.lost) reasons.push('session-lost');
      else if (slot) {
        if (slot.job) reasons.push('session-busy');
        if (slot.state === 'starting') reasons.push('session-starting');
        if (slot.state === 'closing') reasons.push('worker-closing');
        if (slot.releases.size) reasons.push('scope-release');
        if (slot.control) reasons.push('maintenance');
      } else if (job.candidates) {
        if (job.candidates.every((s) => s.closed || s.lost)) reasons.push('session-lost');
        else if (!this.groupSlot(job)) {
          reasons.push('session-busy');
          if (job.candidates.some((s) => s.slot?.control)) reasons.push('maintenance');
        }
      } else
        reasons.push(...this.poolBlockers(this.pools.get(job.options.pool)!, job.options.priority));
    }
    return reasons.length ? reasons : ['scheduler-turn'];
  }

  createScope(label = 'scope'): RuntimeScope<T> {
    return this._createScope(label);
  }

  /** @internal */
  _acquireResource(
    options: ResourceOptions,
    scope?: ScopeRecord,
    session?: SessionRecord,
  ): ResourceLease {
    if (this.closed || scope?.closed || session?.closed || session?.lost)
      throw new RuntimeError('CLOSED', 'Resource owner is closed');
    if (!options || options.kind !== 'resident')
      throw new RuntimeError('INVALID_ARGUMENT', 'Resource kind must be resident');
    const priority = this.priority(options.priority ?? session?.priority);
    if (this.resourceLeases.size >= this.options.maxResourceLeases)
      throw new RuntimeError('BUDGET_EXCEEDED', 'Resident resource lease limit reached');
    const lease = new ResidentLease(
      this.ledger,
      options.bytes,
      () => {
        this.resourceLeases.delete(lease);
        scope?.resources.delete(lease);
        session?.resources.delete(lease);
        this.schedule();
      },
      () => this.schedule(),
      priority,
    );
    this.resourceLeases.add(lease);
    scope?.resources.add(lease);
    session?.resources.add(lease);
    return lease;
  }

  /** Idempotent shutdown with confirmed physical completion. */
  dispose(): Promise<void> {
    if (this.disposal) return this.disposal;
    this.closed = true;
    this.pendingShrinks.clear();
    clearTimeout(this.adaptiveTimer);
    clearTimeout(this.schedulerTimer);
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
      for (const lease of this.resourceLeases) lease.release();
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
      resources: new Set(),
      touched: new Set(),
    };
    parent?.children.add(record);
    this.scopes.add(record);
    return new RuntimeScope(this, record);
  }

  /** @internal */
  _session(scope: ScopeRecord, pool: string, options: SessionOptions = {}): WorkerSession<T> {
    return new WorkerSession(this, this.newSession(scope, pool, options));
  }
  /** @internal */
  _sessionGroup(scope: ScopeRecord, sessions: readonly WorkerSession<T>[]): SessionGroup<T> {
    if (!Array.isArray(sessions) || sessions.length < 1 || sessions.length > 128)
      throw new RuntimeError('INVALID_ARGUMENT', 'Session groups require 1 to 128 bound Sessions');
    const records = [...new Set(sessions.map((session) => session._groupRecord(this, scope)))];
    if (records.some((r) => r.pool !== records[0]!.pool || r.priority !== records[0]!.priority))
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Session group members must share a pool and admission class',
      );
    return new SessionGroup(this, scope, records);
  }
  private newSession(scope: ScopeRecord, pool: string, options: SessionOptions): SessionRecord {
    if (this.closed || scope.closed) throw new RuntimeError('CLOSED', 'Scope is closed');
    if (!this.pools.has(pool)) throw new RuntimeError('INVALID_ARGUMENT', `Unknown pool: ${pool}`);
    if (!options || (options.reclaimable !== undefined && typeof options.reclaimable !== 'boolean'))
      throw new RuntimeError('INVALID_ARGUMENT', 'reclaimable must be boolean');
    const record: SessionRecord = {
      priority: this.priority(options.priority),
      id: `${scope.id}/session-${++this.serial}`,
      scope,
      pool,
      closed: false,
      leases: new Set(),
      reclaimable: options.reclaimable ?? false,
      resources: new Set(),
      reclaimPriority: integer(options.reclaimPriority ?? 0, 'reclaimPriority'),
      reclaimed: false,
      delivering: false,
    };
    scope.sessions.add(record);
    return record;
  }

  /** @internal */
  _acquireSession(
    scope: ScopeRecord,
    pool: string,
    options: SessionAdmissionOptions = {},
  ): Promise<WorkerSession<T>> {
    try {
      return this.requestSession(scope, pool, options);
    } catch (error) {
      return Promise.reject(error);
    }
  }
  private requestSession(
    scope: ScopeRecord,
    pool: string,
    options: SessionAdmissionOptions,
  ): Promise<WorkerSession<T>> {
    if (!options || typeof options !== 'object')
      throw new RuntimeError('INVALID_ARGUMENT', 'Session admission options must be an object');
    if (options.mode !== undefined && options.mode !== 'wait' && options.mode !== 'immediate')
      return Promise.reject(new RuntimeError('INVALID_ARGUMENT', 'Unknown Session admission mode'));
    const duration = timeout(
      options.timeoutMs ?? this.options.queueTimeoutMs,
      'Session admission timeout',
    );
    if (options.residentBytes !== undefined)
      this.ledger.validate(
        { residentBytes: options.residentBytes },
        this.priority(options.priority),
      );
    if (options.signal?.aborted) return Promise.reject(aborted(options.signal.reason));
    if (this.queue.size + this.acquisitions.size >= this.options.maxQueuedTasks)
      return Promise.reject(new RuntimeError('QUEUE_FULL', 'Admission queue is full'));
    const session = this.newSession(scope, pool, options);
    const request: SessionRequest<T> = {
      session,
      result: deferred<WorkerSession<T>>(),
      mode: options.mode ?? 'wait',
      residentBytes: options.residentBytes,
    };
    this.acquisitions.set(session, request);
    const cancel = () => this.finishAcquisition(request, aborted(options.signal?.reason));
    options.signal?.addEventListener('abort', cancel, { once: true });
    request.removeSignal = () => options.signal?.removeEventListener('abort', cancel);
    request.timer = setTimeout(
      () =>
        this.finishAcquisition(
          request,
          new RuntimeError('QUEUE_TIMEOUT', 'Session admission timed out'),
        ),
      duration,
    );
    this.drain();
    if (request.mode === 'immediate' && this.acquisitions.has(session) && !session.slot)
      this.finishAcquisition(
        request,
        new SessionAdmissionError(pool, this.sessionBlockers(session)),
      );
    return request.result.promise;
  }
  private finishAcquisition(request: SessionRequest<T>, error?: Error): void {
    if (!this.acquisitions.delete(request.session)) return;
    clearTimeout(request.timer);
    request.removeSignal?.();
    if (error) {
      request.result.reject(error);
      void this._disposeSession(request.session).catch((cause) =>
        this.observe(
          new RuntimeError('WORKER_FAILED', 'Session admission cleanup failed', { cause }),
        ),
      );
      this.schedule(`pool:${request.session.pool}`, 'reservation');
    } else {
      request.session.delivering = true;
      request.result.resolve(new WorkerSession(this, request.session));
      // A ready message may already have scheduled a drain. Keep the replica protected
      // until the awaiting caller gets its first opportunity to enqueue initialization.
      queueMicrotask(() => {
        request.session.delivering = false;
        this.schedule(`pool:${request.session.pool}`, 'workers', 'reservation');
      });
    }
  }
  private admitSessions(): void {
    const requests = [...this.acquisitions.values()].sort(
      (a, b) =>
        Number(a.session.reclaimable) - Number(b.session.reclaimable) ||
        priorities[a.session.priority] - priorities[b.session.priority],
    );
    for (const request of requests) {
      const session = request.session;
      if (!this.acquisitions.has(session)) continue;
      if (session.slot) continue;
      try {
        // Optional replicas cannot take capacity while a required Session is waiting.
        if (session.reclaimable && this.waitingPrimary(session.pool)) continue;
        const bytes = request.residentBytes;
        if (bytes !== undefined) {
          const bytePressure = !this.ledger.fits({ residentBytes: bytes }, session.priority);
          const leasePressure = this.resourceLeases.size >= this.options.maxResourceLeases;
          if (bytePressure || leasePressure) {
            if (request.mode === 'wait' && !session.reclaimable) {
              const victim = this.slots()
                .filter(
                  (slot) =>
                    this.reclaimableSession(slot) &&
                    [...slot.session!.resources].some((r) =>
                      bytePressure ? r.bytes > 0 : !r.released,
                    ),
                )
                .sort(
                  (a, b) =>
                    a.session!.reclaimPriority - b.session!.reclaimPriority || a.used - b.used,
                )[0];
              this.reclaimIdle(victim, 'resident');
            }
            continue;
          }
        }
        const pool = this.pools.get(session.pool)!;
        const capacity =
          [...pool.slots].some((slot) => this.availableFor(slot, session.priority)) ||
          this.canSpawn(pool, session.priority);
        if (!capacity) {
          if (request.mode === 'wait')
            this.reclaimIdle(this.reclaimVictim(pool, !session.reclaimable, session.priority));
          continue;
        }
        const binding = deferred<void>();
        session.binding = binding.promise;
        let slot: Slot | undefined;
        try {
          if (bytes !== undefined) {
            // Reserve before invoking the factory, which may synchronously reenter the runtime.
            session.resident = this._acquireResource(
              { kind: 'resident', bytes, priority: session.priority },
              session.scope,
              session,
            );
          }
          slot = this.findPoolSlot(
            pool,
            [],
            request.mode === 'wait',
            !session.reclaimable,
            (created) => {
              // Establish ownership before subscriptions/hello can synchronously fail or cancel.
              session.slot = created;
              created.session = session;
              session.scope.touched.add(created);
              clearTimeout(created.idleTimer);
            },
            session.priority,
          );
        } finally {
          // Once an endpoint exists, even a failed handshake must cross physical cleanup.
          if (!session.slot) {
            session.resident?.release();
            session.resident = undefined;
          }
          binding.resolve();
          session.binding = undefined;
        }
        if (!slot) continue;
        // Disposal already awaits binding and will terminate any endpoint returned after cancel.
        if (!this.acquisitions.has(session) || session.closed || session.scope.closed) continue;
        void slot.ready.promise.then(
          () => {
            if (session.lost || session.closed || slot.state !== 'ready')
              this.finishAcquisition(
                request,
                session.lost ?? new RuntimeError('CLOSED', 'Session closed during startup'),
              );
            else this.finishAcquisition(request);
          },
          (error) => this.finishAcquisition(request, asError(error)),
        );
      } catch (error) {
        this.finishAcquisition(request, asError(error));
      }
    }
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
    candidates?: readonly SessionRecord[],
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
    const priority = this.priority(raw.priority ?? session?.priority);
    if (
      priority !== 'interactive' &&
      [session, ...(candidates ?? [])].some(
        (s) =>
          s &&
          (s.slot
            ? !this.slotClass(s.slot, priority)
            : s.priority === 'interactive' &&
              !!(
                this.interactive.workers ||
                pool.options.interactiveWorkers ||
                this.ledger.protected.cacheBytes
              )),
      )
    )
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Tasks on a protected interactive Session must remain interactive',
      );
    this.ledger.validate(cost, priority);
    pool.lastDemand = performance.now();
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
    if (this.queue.size + this.acquisitions.size >= this.options.maxQueuedTasks)
      throw new RuntimeError('QUEUE_FULL', 'Waiting queue is full');
    const order = ++this.serial;
    const affinityKeys = this.normalizeAffinity(raw.affinity);
    const job: Job = {
      id: `${this.prefix}/job-${order}`,
      order,
      groupKey: `${scope.id}\0${raw.group ?? 'default'}`,
      // Ageing can equalize ranks, but a non-interactive head must not hide interactive reserves.
      laneKey: `${raw.pool}\0${session?.id ?? candidates?.map((s) => s.id).join(',') ?? ''}\0${priority === 'interactive'}`,
      affinityKeys,
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
      candidates,
      options: {
        ...raw,
        priority,
        affinity: undefined, // The bounded, immutable submission snapshot lives in affinityKeys.
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
    candidates?: readonly SessionRecord[],
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
      candidates,
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
        for (const lease of scope.resources) lease.release();
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
    const acquisition = this.acquisitions.get(session);
    if (acquisition) {
      this.acquisitions.delete(session);
      clearTimeout(acquisition.timer);
      acquisition.removeSignal?.();
      acquisition.result.reject(new RuntimeError('CLOSED', 'Session disposed during admission'));
      this.schedule(`pool:${session.pool}`, 'reservation');
    }
    for (const lease of [...session.leases]) lease.release();
    const jobs = [...session.scope.jobs].filter((job) => job.session === session);
    for (const job of jobs) this.cancel(job, new RuntimeError('CLOSED', 'Session disposed'));
    // A session owns its physical worker exclusively, so termination cannot kill
    // another session or another scope's active task.
    const stop = (async () => {
      if (session.binding) await session.binding;
      const slot = session.slot;
      if (slot) {
        if (slot.state === 'ready' && !slot.job) await this.releaseScope(slot, session.scope.id);
        await this.retire(slot, new RuntimeError('CLOSED', 'Session disposed'));
      }
    })();
    session.disposal = Promise.all([stop, ...jobs.map((job) => job.settled.promise)])
      .then(() => {
        session.scope.sessions.delete(session);
        for (const resource of session.resources) resource.release();
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
  private poolKey(pool: Pool): string {
    return `pool:${[...this.pools].find(([, p]) => p === pool)![0]}`;
  }
  private schedule(...resources: string[]): void {
    for (const key of resources) this.scheduler.wake(key);
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
      this.reconcileShrinks();
      this.admitSessions();
      while (this.queue.size) {
        const now = performance.now();
        let chosen: Slot | undefined;
        const job = this.scheduler.select(
          now,
          (candidate) => {
            const block = (...keys: string[]) => {
              this.scheduler.suspend(candidate, keys);
              return false;
            };
            if (candidate.phase !== 'queue' && candidate.phase !== 'prepared')
              return block(`prepare:${candidate.id}`);
            const producing = !!candidate.preparation && !candidate.reserved;
            const interactive = candidate.options.priority === 'interactive';
            if (
              producing
                ? this.preparationWindow.size >= this.options.maxPreparingTasks ||
                  (!interactive &&
                    [...this.preparationWindow].filter((j) => j.options.priority !== 'interactive')
                      .length >=
                      this.options.maxPreparingTasks - this.interactive.preparingTasks)
                : this.active >= this.options.maxActiveTasks ||
                  (!interactive &&
                    this.nonInteractiveActive >=
                      this.options.maxActiveTasks - this.interactive.activeTasks)
            )
              return block(producing ? 'preparing' : 'active');
            if (candidate.reserved) {
              try {
                chosen = this.findSlot(candidate);
              } catch (error) {
                this.finish(candidate, undefined, asError(error));
              }
              return chosen !== undefined || block(...this.slotWaitKeys(candidate));
            }
            if (
              !candidate.options.discardResult &&
              (this.resultReservations + this.leaseCount >= this.options.maxResultLeases ||
                (!interactive &&
                  this.nonInteractiveResults >=
                    this.options.maxResultLeases - this.interactive.resultLeases))
            )
              return block('results');
            if (
              this.reservation &&
              (this.reservation.phase === 'done' || !this.canRun(this.reservation))
            )
              this.reservation = undefined;
            const reserved = this.reservation;
            if (this.protectedBudget(candidate)) return block('reservation');
            if (!this.ledger.fits(candidate.cost, candidate.options.priority)) {
              if (
                !reserved &&
                now - candidate.enqueuedAt >= this.options.budgetWaitMs &&
                this.canRun(candidate)
              )
                this.reservation = candidate;
              if (
                !candidate.admissionTimer &&
                now - candidate.enqueuedAt < this.options.budgetWaitMs
              )
                candidate.admissionTimer = setTimeout(
                  () => {
                    // Timer clocks can fire early; the next drain must be able to rearm it.
                    candidate.admissionTimer = undefined;
                    this.schedule('budget', 'reservation');
                  },
                  Math.ceil(this.options.budgetWaitMs - (now - candidate.enqueuedAt)),
                );
              return block('budget', ...this.slotWaitKeys(candidate));
            }
            if (producing) return true;
            try {
              chosen = this.findSlot(candidate);
            } catch (error) {
              this.failQueued(candidate, asError(error));
              return false;
            }
            return chosen !== undefined || block(...this.slotWaitKeys(candidate));
          },
          (candidate) => !candidate.reserved,
        );
        if (!job) break;
        if (this.reservation === job) {
          this.reservation = undefined;
          this.scheduler.wake('reservation');
        }
        if (job.preparation && !job.reserved) this.startPreparation(job);
        else if (chosen) this.admit(job, chosen);
      }
    } finally {
      this.draining = false;
      clearTimeout(this.schedulerTimer);
      const promotion = this.scheduler.nextPromotionAt;
      if (promotion !== undefined)
        this.schedulerTimer = setTimeout(
          () => this.schedule(),
          Math.max(0, promotion - performance.now()),
        );
    }
  }
  private slotWaitKeys(job: Job): string[] {
    const pool = this.pools.get(job.options.pool)!;
    return job.session?.slot || job.candidates || pool.slots.size >= pool.capacity
      ? [this.poolKey(pool)]
      : [this.poolKey(pool), 'workers', 'cache-budget'];
  }
  private canRun(job: Job): boolean {
    if (job.candidates)
      return job.candidates.some((s) => !s.closed && !s.lost) && !!this.groupSlot(job);
    if (job.session?.lost) return false;
    if (job.session?.slot)
      return (
        !job.session.slot.job &&
        !job.session.slot.releases.size &&
        !job.session.slot.control &&
        this.slotClass(job.session.slot, job.options.priority!) &&
        job.session.slot.state === 'ready'
      );
    const pool = this.pools.get(job.options.pool)!;
    if (
      [...pool.slots].some(
        (slot) => this.availableFor(slot, job.options.priority!) && slot.state === 'ready',
      )
    )
      return true;
    const reclaimSession = !!job.session && !job.session.reclaimable;
    if (pool.slots.size >= pool.capacity)
      return !!this.reclaimVictim(pool, reclaimSession, job.options.priority);
    return (
      this.canSpawn(pool, job.options.priority!) ||
      !!this.reclaimVictim(pool, reclaimSession, job.options.priority)
    );
  }
  private normalizeAffinity(affinity: TaskOptions<unknown>['affinity']): readonly string[] {
    if (affinity === undefined) return [];
    const keys = typeof affinity === 'string' ? [affinity] : affinity?.keys;
    if (
      !Array.isArray(keys) ||
      keys.length > 128 ||
      keys.some((key) => typeof key !== 'string' || key.length > 1024)
    )
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Affinity requires at most 128 string keys of at most 1024 characters',
      );
    return [...new Set<string>(keys)];
  }
  private affinityIds(job: Job): string[] {
    return job.affinityKeys.map(
      (key) => `${job.scope.id}\0${JSON.stringify([job.options.pool, key])}`,
    );
  }
  private rememberAffinity(job: Job, slot: Slot): void {
    for (const key of this.affinityIds(job)) {
      const placements = this.affinity.get(key) ?? new Set<Slot>();
      placements.add(slot);
      this.affinity.delete(key);
      this.affinity.set(key, placements);
      while (this.affinity.size > this.options.maxAffinityEntries)
        this.affinity.delete(required(this.affinity.keys().next().value, 'Affinity entry'));
    }
  }
  private findSlot(job: Job): Slot | undefined {
    if (job.candidates) return this.groupSlot(job);
    if (job.session?.lost) throw job.session.lost;
    if (job.session?.slot) {
      const slot = job.session.slot;
      return !slot.job &&
        !slot.releases.size &&
        !slot.control &&
        this.slotClass(slot, job.options.priority!) &&
        (slot.state === 'ready' || slot.state === 'starting')
        ? slot
        : undefined;
    }
    const pool = required(this.pools.get(job.options.pool), 'Task pool');
    if (job.session?.reclaimable && this.waitingPrimary(job.options.pool)) return undefined;
    return this.findPoolSlot(
      pool,
      this.affinityIds(job),
      true,
      !!job.session && !job.session.reclaimable,
      undefined,
      job.options.priority,
    );
  }
  private groupSlot(job: Job): Slot | undefined {
    const sessions = job.candidates!.filter((s) => !s.closed && !s.lost);
    if (!sessions.length)
      throw new RuntimeError('SESSION_LOST', 'All Session group members are closed or lost');
    const score = (session: SessionRecord) => {
      const keys = new Set(
        session
          .slot!.reports.filter((r) => r.scope === job.scope.id && r.session === session.id)
          .flatMap((r) => [...r.keys]),
      );
      return job.affinityKeys.reduce((n, key) => n + Number(keys.has(key)), 0);
    };
    return sessions
      .filter(
        (s) =>
          s.slot?.state === 'ready' &&
          !s.slot.job &&
          !s.slot.releases.size &&
          !s.slot.control &&
          this.slotClass(s.slot, job.options.priority!),
      )
      .sort((a, b) => score(b) - score(a) || a.slot!.used - b.slot!.used)[0]?.slot;
  }
  private available(slot: Slot): boolean {
    return (
      !slot.job &&
      !slot.control &&
      !slot.session &&
      slot.releases.size === 0 &&
      (slot.state === 'ready' || slot.state === 'starting')
    );
  }
  private availableFor(slot: Slot, priority: Priority): boolean {
    // Shrink may leave busy/required non-interactive Workers above the new class target.
    return (
      this.available(slot) &&
      this.slotClass(slot, priority) &&
      (priority === 'interactive' ||
        slot.pool.slots.size <= slot.pool.capacity - slot.pool.options.interactiveWorkers ||
        this.liveSlots(slot.pool).filter((s) => s.priority !== 'interactive').length <=
          slot.pool.capacity - slot.pool.options.interactiveWorkers)
    );
  }
  private reclaimableSession(slot: Slot): boolean {
    const session = slot.session;
    return (
      !!session &&
      session.reclaimable &&
      !session.closed &&
      !session.lost &&
      !session.delivering &&
      !this.acquisitions.has(session) &&
      slot.state === 'ready' &&
      !slot.control &&
      !slot.job &&
      !slot.releases.size &&
      session.leases.size === 0 &&
      ![...session.scope.jobs].some(
        (job) => job.session === session || job.candidates?.includes(session),
      )
    );
  }
  private reclaimVictim(
    pool: Pool,
    sessions: boolean,
    priority: Priority = 'foreground',
  ): Slot | undefined {
    const full = pool.slots.size >= pool.capacity;
    return this.slots()
      .filter(
        (slot) =>
          (priority === 'interactive' ||
            ((this.slots().filter((s) => s.priority !== 'interactive').length <
              this.options.maxWorkers - this.interactive.workers ||
              slot.priority !== 'interactive') &&
              ([...pool.slots].filter((s) => s.priority !== 'interactive').length <
                pool.capacity - pool.options.interactiveWorkers ||
                (slot.pool === pool && slot.priority !== 'interactive')))) &&
          (!full || slot.pool === pool) &&
          ((!slot.session &&
            (slot.pool !== pool || !this.slotClass(slot, priority)) &&
            this.available(slot) &&
            slot.state === 'ready') ||
            (sessions && this.reclaimableSession(slot))),
      )
      .sort(
        (a, b) =>
          Number(!!a.session) - Number(!!b.session) ||
          (a.session?.reclaimPriority ?? 0) - (b.session?.reclaimPriority ?? 0) ||
          a.used - b.used,
      )[0];
  }
  private findPoolSlot(
    pool: Pool,
    affinity: readonly string[] = [],
    reclaim = true,
    sessions = false,
    bind?: (slot: Slot) => void,
    priority: Priority = 'foreground',
  ): Slot | undefined {
    const available = [...pool.slots].filter((slot) => this.availableFor(slot, priority));
    const score = (slot: Slot) =>
      affinity.reduce((n, key) => n + Number(this.affinity.get(key)?.has(slot) ?? false), 0);
    if (available.length) {
      const slot = available.sort((a, b) => score(b) - score(a) || a.used - b.used)[0]!;
      bind?.(slot);
      return slot;
    }
    if (!this.canSpawn(pool, priority)) {
      if (reclaim) this.reclaimIdle(this.reclaimVictim(pool, sessions, priority));
      return undefined;
    }
    return this.spawn(pool, bind, priority);
  }
  private reclaimIdle(victim: Slot | undefined, reason: ReclaimReason = 'capacity'): void {
    // A failed termination remains charged, but must not block healthy victims forever.
    if (
      victim &&
      !this.reclaiming &&
      !this.maintenance &&
      !this.slots().some((slot) => slot.state === 'closing' && !slot.terminationFailed)
    ) {
      this.reclaiming = true;
      const stop = this.reclaimSlot(victim, reason);
      void stop
        .finally(() => {
          this.reclaiming = false;
          this.wakeReclamation();
        })
        .catch((cause) =>
          this.observe(
            new RuntimeError('WORKER_FAILED', 'Worker reclamation failed; capacity remains held', {
              cause,
            }),
          ),
        );
    }
  }

  private wakeReclamation(): void {
    // A global reclamation barrier also blocks full pools, whose tasks wait only on pool keys.
    this.schedule(
      ...[...this.pools.keys()].map((name) => `pool:${name}`),
      'workers',
      'cache-budget',
    );
  }

  private reclaimSlot(slot: Slot, reason: ReclaimReason): Promise<void> {
    if (slot.reclaim) return slot.reclaim;
    const stats = slot.pool.reclaim;
    stats.attempts++;
    stats.byReason[reason]++;
    if (slot.session) slot.session.reclaimed = true;
    slot.reclaim = (
      slot.session
        ? this._disposeSession(slot.session)
        : this.retire(slot, new RuntimeError('CLOSED', `Idle worker reclaimed: ${reason}`))
    )
      .then(() => {
        stats.succeeded++;
        if (slot.session) this.counters.sessionsReclaimed++;
      })
      .catch((error) => {
        stats.failed++;
        throw error;
      });
    return slot.reclaim;
  }
  private spawn(pool: Pool, bind?: (slot: Slot) => void, priority: Priority = 'foreground'): Slot {
    const releaseCache = this.ledger.reserve({ cacheBytes: pool.cacheTarget }, priority);
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
      cacheStats: { hits: 0, misses: 0, evictions: 0 },
      resourceCacheStats: emptyCacheStats(),
      reports: [],
      cacheLimit: pool.cacheTarget,
      priority,
    };
    pool.slots.add(slot);
    this.counters.workerStarts++;
    bind?.(slot);
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
        cacheBytes: slot.cacheLimit,
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
    job.releaseExecution = this.ledger.reserve(
      {
        inputBytes: job.cost.inputBytes,
        scratchBytes: job.cost.scratchBytes,
      },
      job.options.priority,
    );
    job.releaseOutput = this.ledger.reserve(
      { outputBytes: job.cost.outputBytes },
      job.options.priority,
    );
    job.reserved = true;
    if (!job.options.discardResult) {
      this.resultReservations++;
      if (job.options.priority !== 'interactive') this.nonInteractiveResults++;
    }
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
        this.schedule(`prepare:${job.id}`, 'preparing');
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
    if (job.candidates) job.session = slot.session;
    if (job.session && !job.session.slot) {
      job.session.slot = slot;
      slot.session = job.session;
    }
    this.active++;
    if (job.options.priority !== 'interactive') this.nonInteractiveActive++;
    this.scheduler.wake('preparing');
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
            'prepare must return synchronously; use enqueuePrepared for async I/O and a Worker handler for CPU-heavy work',
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
        'blobLimits.inputBytes',
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
  private updateCacheStats(slot: Slot, raw: unknown): boolean {
    // Optional for custom endpoints. Only accept monotonic, bounded counters.
    if (raw === undefined) return true;
    if (!raw || typeof raw !== 'object') return false;
    const next = raw as CacheStats;
    const keys = ['hits', 'misses', 'evictions'] as const;
    if (keys.some((key) => !Number.isSafeInteger(next[key]) || next[key] < slot.cacheStats[key]))
      return false;
    for (const key of keys) {
      slot.pool.cacheStats[key] = Math.min(
        Number.MAX_SAFE_INTEGER,
        slot.pool.cacheStats[key] + next[key] - slot.cacheStats[key],
      );
      slot.cacheStats[key] = next[key];
    }
    return true;
  }
  private updateResourceTelemetry(slot: Slot, stats: unknown, reports: unknown): boolean {
    try {
      if (stats === undefined && reports === undefined) return true;
      const totals = cacheReport(
        { ...(stats as CacheStats), usedBytes: 0, keys: [] },
        slot.resourceCacheStats,
      );
      if (!Array.isArray(reports) || reports.length > 64) return false;
      const snapshots: ResourceCacheSnapshot[] = [];
      const ids = new Set<string>();
      let keys = 0,
        reserved = 0;
      for (const raw of reports) {
        for (const field of ['id', 'scope', 'session', 'resource'])
          if (!raw || typeof raw[field] !== 'string' || raw[field].length > 2048) return false;
        if (ids.has(raw.id)) return false;
        ids.add(raw.id);
        const report = cacheReport(raw);
        keys += report.keys.length;
        const bytes = integer(raw.reservedBytes, 'resource reservedBytes');
        reserved += bytes;
        if (keys > 1024 || report.usedBytes > bytes || reserved > slot.cacheLimit) return false;
        snapshots.push({
          ...report,
          id: raw.id,
          scope: raw.scope,
          session: raw.session,
          resource: raw.resource,
          reservedBytes: bytes,
        });
      }
      addCacheStats(slot.pool.resourceCacheStats, {
        hits: totals.hits - slot.resourceCacheStats.hits,
        misses: totals.misses - slot.resourceCacheStats.misses,
        evictions: totals.evictions - slot.resourceCacheStats.evictions,
      });
      slot.resourceCacheStats = {
        hits: totals.hits,
        misses: totals.misses,
        evictions: totals.evictions,
      };
      slot.reports = snapshots;
      return true;
    } catch {
      return false;
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
    if (message.type === 'cache-controlled') {
      const control = slot.control;
      if (!control || message.id !== control.id) return;
      if (
        !Number.isSafeInteger(message.limit) ||
        message.limit !== (message.error ? slot.cacheLimit : control.target) ||
        !Number.isSafeInteger(message.cacheBytes) ||
        message.cacheBytes < 0 ||
        message.cacheBytes > message.limit
      ) {
        void this.retire(
          slot,
          new RuntimeError('PROTOCOL_ERROR', 'Invalid cache control acknowledgement'),
        );
        return;
      }
      const previous = slot.cacheLimit;
      slot.cacheLimit = message.limit;
      if (
        !this.updateCacheStats(slot, message.cacheStats) ||
        !this.updateResourceTelemetry(slot, message.resourceCacheStats, message.resourceReports)
      ) {
        slot.cacheLimit = previous;
        void this.retire(
          slot,
          new RuntimeError('PROTOCOL_ERROR', 'Invalid cache control telemetry'),
        );
        return;
      }
      clearTimeout(control.timer);
      slot.control = undefined;
      control.extra?.();
      slot.releaseCache();
      slot.releaseCache = this.ledger.reserve({ cacheBytes: slot.cacheLimit }, slot.priority);
      slot.cacheUsed = message.cacheBytes;
      if (message.error) {
        try {
          control.result.reject(decodeError(message.error));
        } catch (error) {
          control.result.reject(error);
        }
      } else control.result.resolve();
      this.armIdle(slot);
      this.schedule(this.poolKey(slot.pool), 'workers', 'cache-budget');
      return;
    }
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
      this.armIdle(slot);
      this.schedule(this.poolKey(slot.pool), 'workers', 'reservation');
      return;
    }
    if (message.type === 'released') {
      const pending = slot.releases.get(message.scope);
      if (!pending) return;
      if (
        !Number.isSafeInteger(message.cacheBytes) ||
        message.cacheBytes < 0 ||
        message.cacheBytes > slot.cacheLimit ||
        (message.error && typeof message.error.message !== 'string') ||
        !this.updateCacheStats(slot, message.cacheStats) ||
        !this.updateResourceTelemetry(slot, message.resourceCacheStats, message.resourceReports)
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
        this.schedule(this.poolKey(slot.pool), 'workers', 'reservation');
        return;
      }
      pending.deferred.resolve();
      if (
        Number.isSafeInteger(message.cacheBytes) &&
        message.cacheBytes >= 0 &&
        message.cacheBytes <= slot.cacheLimit
      )
        slot.cacheUsed = message.cacheBytes;
      this.armIdle(slot);
      this.schedule(this.poolKey(slot.pool), 'workers', 'reservation');
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
      message.cacheBytes > slot.cacheLimit ||
      !this.updateCacheStats(slot, message.cacheStats) ||
      !this.updateResourceTelemetry(slot, message.resourceCacheStats, message.resourceReports)
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
        checkBlobLimit(
          packetBlobBytes(message.value),
          job.options.blobLimits!.outputBytes,
          'blobLimits.outputBytes',
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
    clearTimeout(job.admissionTimer);
    job.removeSignal?.();
    job.removeSignal = undefined;
    this.queue.delete(job);
    this.scheduler.remove(job);
    this.jobs.delete(job.id);
    job.scope.jobs.delete(job);

    if (job.slot !== undefined) {
      this.active--;
      if (job.options.priority !== 'interactive') this.nonInteractiveActive--;
    }
    this.preparationWindow.delete(job);
    if (job.reserved && !job.options.discardResult) {
      this.resultReservations--;
      if (job.options.priority !== 'interactive') this.nonInteractiveResults--;
    }
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
      if (slot?.state === 'ready') this.rememberAffinity(job, slot);
      this.counters.completed++;
      this.counters.outputBytes += result.bytes;
      const releaseOutput = required(job.releaseOutput, 'Output reservation');
      job.releaseOutput = undefined;
      this.leaseCount++;
      if (!job.options.discardResult && job.options.priority !== 'interactive')
        this.nonInteractiveResults++;
      const lease = new OwnedResult(result.value, result.bytes, () => {
        releaseOutput();
        this.leaseCount--;
        if (!job.options.discardResult && job.options.priority !== 'interactive')
          this.nonInteractiveResults--;
        job.scope.leases.delete(lease);
        job.session?.leases.delete(lease);
        this.schedule('budget', 'results', 'reservation', `pool:${job.options.pool}`, 'workers');
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
    job.candidates = undefined;
    job.settled.resolve();
    this.schedule(
      'budget',
      'active',
      'preparing',
      'results',
      'reservation',
      `pool:${job.options.pool}`,
      'workers',
    );
  }
  private releaseScope(slot: Slot, scope: string): Promise<void> {
    const existing = slot.releases.get(scope);
    if (existing) return existing.deferred.promise;
    const result = deferred<void>();
    clearTimeout(slot.idleTimer);
    const timer = setTimeout(() => {
      slot.releases.delete(scope);
      this.armIdle(slot);
      this.schedule(this.poolKey(slot.pool), 'workers', 'reservation');
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
    owners: {
      id: string;
      label: string;
      tasks: number;
      leases: number;
      sessions: number;
      resources: number;
      residentBytes: number;
    }[];
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
        resources: scope.resources.size,
        residentBytes: [...scope.resources].reduce((sum, lease) => sum + lease.bytes, 0),
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
      slot.control ||
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
    if (slot.control) {
      clearTimeout(slot.control.timer);
      slot.control.result.reject(reason);
    }
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
      slot.reports = [];
      slot.control?.extra?.();
      slot.control = undefined;
      slot.releaseCache();
      slot.pool.slots.delete(slot);
      for (const scope of this.scopes) scope.touched.delete(slot);
      for (const [key, placements] of this.affinity) {
        placements.delete(slot);
        if (!placements.size) this.affinity.delete(key);
      }
      this.counters.workerTerminations++;
      if (job && wasPosted) this.finish(job, undefined, reason);
      stopped.resolve();
      this.schedule(this.poolKey(slot.pool), 'workers', 'cache-budget', 'reservation');
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
  readonly resources: ResourceReservations;
  /** @internal */
  constructor(
    private readonly runtime: WorkerRuntime<T>,
    private readonly record: ScopeRecord,
  ) {
    this.resources = { acquire: (options) => this.runtime._acquireResource(options, this.record) };
  }
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
  session(pool: string, options?: SessionOptions): WorkerSession<T> {
    return this.runtime._session(this.record, pool, options);
  }
  acquireSession(pool: string, options?: SessionAdmissionOptions): Promise<WorkerSession<T>> {
    return this.runtime._acquireSession(this.record, pool, options);
  }
  sessionGroup(sessions: readonly WorkerSession<T>[]): SessionGroup<T> {
    return this.runtime._sessionGroup(this.record, sessions);
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
  readonly resources: ResourceReservations;
  /** @internal */
  constructor(
    private readonly runtime: WorkerRuntime<T>,
    private readonly record: SessionRecord,
  ) {
    this.resources = {
      acquire: (options) => this.runtime._acquireResource(options, this.record.scope, this.record),
    };
  }
  get id(): string {
    return this.record.id;
  }
  /** @internal */
  _groupRecord(runtime: WorkerRuntime<T>, scope: ScopeRecord): SessionRecord {
    if (runtime !== this.runtime || scope !== this.record.scope || this.state !== 'bound')
      throw new RuntimeError(
        'INVALID_ARGUMENT',
        'Session group members must be bound to the same Runtime and Scope',
      );
    return this.record;
  }
  get reclaimed(): boolean {
    return this.record.reclaimed;
  }
  /** Optional lifetime reservation created atomically by acquireSession. */
  get resident(): ResourceLease | undefined {
    return this.record.resident;
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

/** Borrowed replicas of one business source; the group does not own member lifetimes. */
export class SessionGroup<T extends Catalog<T> = TaskMap> {
  /** @internal */
  constructor(
    private runtime: WorkerRuntime<T>,
    private scope: ScopeRecord,
    private sessions: readonly SessionRecord[],
  ) {}
  enqueue<K extends TaskName<T>>(
    name: K,
    options: Omit<TaskOptions<T[K]['input']>, 'pool'>,
  ): TaskHandle<T[K]['output']> {
    return this.runtime._enqueue(
      this.scope,
      name,
      {
        ...options,
        pool: this.sessions[0]!.pool,
        priority: options.priority ?? this.sessions[0]!.priority,
      },
      undefined,
      undefined,
      this.sessions,
    );
  }
  enqueuePrepared<K extends TaskName<T>>(
    name: K,
    options: Omit<PreparedTaskOptions<T[K]['input']>, 'pool'>,
  ): TaskHandle<T[K]['output']> {
    return this.runtime._enqueuePrepared(
      this.scope,
      name,
      {
        ...options,
        pool: this.sessions[0]!.pool,
        priority: options.priority ?? this.sessions[0]!.priority,
      },
      undefined,
      this.sessions,
    );
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
