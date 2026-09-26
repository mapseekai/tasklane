import type { RuntimeError } from './errors.js';

/** The portable boundary. Factories must return a fresh, dedicated endpoint. */
export interface MessagePortLike {
  postMessage(message: unknown, transfer?: readonly Transferable[]): void;
  onMessage(listener: (message: unknown) => void): () => void;
}

export interface WorkerEndpoint extends MessagePortLike {
  onFailure(listener: (error: Error) => void): () => void;
  /** Resolves only when physical termination has completed. */
  terminate(): void | Promise<void>;
}

export interface TaskType<Input = unknown, Output = unknown> {
  input: Input;
  output: Output;
}
export type TaskMap = Record<string, TaskType>;
export type TaskName<T> = Extract<keyof T, string>;
export type Catalog<T> = { [K in keyof T]: TaskType };
export type Priority = 'interactive' | 'foreground' | 'background';
export type Cancellation = 'cooperative' | 'discard' | 'terminate';
export type TaskState =
  | 'queued'
  | 'starting'
  | 'preparing'
  | 'prepared'
  | 'running'
  | 'cancelling'
  | 'succeeded'
  | 'failed'
  | 'cancelled';

/** Declared upper bounds, reserved atomically BEFORE prepare executes. */
export interface TaskBudget {
  inputBytes: number;
  scratchBytes: number;
  outputBytes: number;
}
export interface RuntimeBudgets extends TaskBudget {
  /** Reservations for all resident worker caches, not just currently used entries. */
  cacheBytes: number;
  /** Caller-declared lifetime budget, independent of task/cache ceilings. Default 128 MiB. */
  residentBytes?: number;
}

export interface ResourceLease {
  readonly bytes: number;
  readonly released: boolean;
  /** Atomic, exact resize. Failure leaves the previous reservation intact. */
  resize(bytes: number): void;
  release(): void;
}
export interface ResourceOptions {
  kind: 'resident';
  bytes: number;
  priority?: Priority;
}
export interface ResourceReservations {
  acquire(options: ResourceOptions): ResourceLease;
}
export interface SessionOptions {
  /** Resource admission class; task priority defaults to this class. */
  priority?: Priority;
  /** Only idle Sessions without pending tasks or held results may be reclaimed. */
  reclaimable?: boolean;
  /** Lower values are reclaimed first, then least recently used. Default 0. */
  reclaimPriority?: number;
}
export type TaskAffinity = string | { keys: readonly string[] };

export interface PreparedInput<T> {
  payload: T;
  /** Explicit ownership transfer. The runtime never automatically detaches input buffers. */
  transfer?: readonly Transferable[];
}

export interface TaskOptions<Input> {
  pool: string;
  budget: TaskBudget;
  /** Per-packet logical Blob/File sizes; defaults to zero. Not reserved heap/RSS credits. */
  blobLimits?: { inputBytes: number; outputBytes: number };
  /** Runs synchronously in the caller realm while holding a Worker. Keep CPU-heavy work in handlers. */
  prepare(context: { signal: AbortSignal }): PreparedInput<Input>;
  priority?: Priority;
  /** Fairness is per scope + group, not merely per task. */
  group?: string;
  /** Soft, bounded cache affinity. Use a Session for required affinity. */
  affinity?: TaskAffinity;
  cancellation?: Cancellation;
  signal?: AbortSignal;
  queueTimeoutMs?: number;
  /** Includes startup, prepare, and physical worker execution, but not queue waiting. */
  executionTimeoutMs?: number;
  /** Discard successful values and return output credits before settled. result resolves a released lease. */
  discardResult?: boolean;
  onProgress?: (value: unknown) => void;
}

/** Budgeted asynchronous production before Worker admission. */
export interface PreparedTaskOptions<Input> extends Omit<TaskOptions<Input>, 'prepare'> {
  /** Runs in the caller realm without a Worker; async I/O can overlap, synchronous work still blocks. */
  prepareAsync(context: {
    signal: AbortSignal;
  }): PreparedInput<Input> | Promise<PreparedInput<Input>>;
  /** Main-realm temporary data upper bound; reserved together with the task budget. */
  preparationScratchBytes: number;
  /** Bounds the producer phase; executionTimeoutMs covers all admitted phases together. */
  preparationTimeoutMs?: number;
}
export type SessionPreparedTaskOptions<Input> = Omit<
  PreparedTaskOptions<Input>,
  'pool' | 'affinity'
>;

export type SessionTaskOptions<Input> = Omit<TaskOptions<Input>, 'pool' | 'affinity'>;

export interface TaskTiming {
  queueMs: number;
  startupMs: number;
  prepareMs: number;
  roundTripMs: number;
  workerMs: number;
  totalMs: number;
}

export interface ResultLease<T> {
  /** Throws after release. External references retained by the caller cannot be erased. */
  readonly value: T;
  readonly byteLength: number;
  readonly released: boolean;
  release(): void;
}

export interface TaskHandle<T> {
  readonly id: string;
  readonly state: TaskState;
  readonly timing: Readonly<TaskTiming>;
  readonly result: Promise<ResultLease<T>>;
  /** Physical completion, including cancelled work. Never rejects. */
  readonly settled: Promise<void>;
  cancel(reason?: unknown): void;
}

export interface PoolOptions {
  factory: () => WorkerEndpoint;
  size: number;
  /** Per-Worker cache hard bound and default reservation. Zero disables persistent storage. */
  cacheBytes?: number;
  cacheEntries?: number;
  /** Hard cancellation may discard opportunistic caches. Sessions always have exclusive slots. */
  allowHardCancel?: boolean;
  /** A pooled, unpinned idle worker is reclaimed after this interval; 0 disables idle expiry. */
  idleTimeoutMs?: number;
  /** Worker slots unavailable to non-interactive admissions. */
  interactiveWorkers?: number;
  /** Opt-in feedback controller. size/cacheBytes remain hard upper bounds. */
  adaptive?: AdaptivePoolOptions;
}

export interface AdaptivePoolOptions {
  minWorkers?: number;
  minCacheBytes?: number;
  sampleMs?: number;
  idleMs?: number;
  missRatio?: number;
}
export interface InteractiveReserve {
  workers?: number;
  activeTasks?: number;
  preparingTasks?: number;
  resultLeases?: number;
  budgets?: Partial<RuntimeBudgets>;
}
export interface PoolSizing {
  size?: number;
  cacheBytes?: number;
}
export type MemoryPressure = 'normal' | 'moderate' | 'critical';
export interface TrimOptions {
  pool?: string;
  cacheBytesPerWorker?: number;
  workersPerPool?: number;
  reclaimSessions?: boolean;
}
export interface MaintenanceReport {
  workersReclaimed: number;
  cacheBytesReleased: number;
  failures: readonly { pool: string; worker: number; message: string }[];
}
export type ReclaimReason = 'capacity' | 'resident' | 'pressure' | 'resize' | 'adaptive';
export interface ReclaimStats {
  attempts: number;
  succeeded: number;
  failed: number;
  byReason: Record<ReclaimReason, number>;
}
/** Absolute counters and a complete replacement snapshot of a reader's current cached keys. */
export interface ResourceCacheReport extends CacheStats {
  usedBytes: number;
  keys: readonly string[];
}
export interface ResourceCacheSnapshot extends ResourceCacheReport {
  id: string;
  scope: string;
  session: string;
  resource: string;
  reservedBytes: number;
}

export interface RuntimeOptions {
  interactiveReserve?: InteractiveReserve;
  pools: Record<string, PoolOptions>;
  maxWorkers?: number;
  maxActiveTasks?: number;
  /** Bounds asynchronous producers plus prepared inputs waiting for a Worker. Default 2. */
  maxPreparingTasks?: number;
  maxQueuedTasks?: number;
  /** Bounds held leases plus admitted work, including zero-binary-byte results. */
  maxResultLeases?: number;
  /** Bounds resident resource handles, including zero-byte reservations. Default 4096. */
  maxResourceLeases?: number;
  /** Maximum simultaneously open scopes, including children. */
  maxScopes?: number;
  budgets?: Partial<RuntimeBudgets>;
  startupTimeoutMs?: number;
  queueTimeoutMs?: number;
  executionTimeoutMs?: number;
  /** Strict priorities by default; opt into cross-priority ageing explicitly. */
  priorityPolicy?: 'strict' | 'ageing';
  /** Promotion interval for the ageing policy. */
  ageingMs?: number;
  maxAffinityEntries?: number;
  releaseTimeoutMs?: number;
  budgetWaitMs?: number;
  /** Observer exceptions are isolated from task execution. */
  onDiagnostic?: (error: RuntimeError) => void;
}

/** Cumulative CacheStore lookups and automatic LRU evictions; opaque resource internals are excluded. */
export interface CacheStats {
  hits: number;
  misses: number;
  evictions: number;
}
export interface RuntimeStats {
  resourceCacheStats: Readonly<CacheStats>;
  reclaim: Readonly<ReclaimStats>;
  scheduler: { blockedBuckets: number; eligibilityChecks: number; wakeups: number };
  cacheStats: Readonly<CacheStats>;
  queued: number;
  active: number;
  preparing: number;
  prepared: number;
  /** Reserved input/scratch/output envelopes of preparing and prepared tasks. */
  preparationReserved: TaskBudget;
  workers: number;
  closingWorkers: number;
  leases: number;
  resourceLeases: number;
  sessionsReclaimed: number;
  scopes: number;
  quarantinedWorkers: number;
  reserved: Required<RuntimeBudgets>;
  peakReserved: Required<RuntimeBudgets>;
  cacheUsedBytes: number;
  completed: number;
  cancelled: number;
  failed: number;
  workerStarts: number;
  workerTerminations: number;
  inputBytes: number;
  outputBytes: number;
  observerErrors: number;
}

export type AdmissionBlocker =
  | 'interactive-reserve'
  | 'maintenance'
  | 'pool-capacity'
  | 'worker-capacity'
  | 'cache-budget'
  | 'resident-budget'
  | 'resource-leases'
  | 'worker-closing'
  | 'session-busy'
  | 'session-starting'
  | 'session-priority'
  | 'session-lost'
  | 'scope-release'
  | 'preparation-window'
  | 'active-tasks'
  | 'result-leases'
  | 'input-budget'
  | 'scratch-budget'
  | 'output-budget'
  | 'budget-reservation'
  | 'lane-order'
  | 'preparing'
  | 'scheduler-turn';
export interface SessionAdmissionOptions extends SessionOptions {
  /** Reserve lifetime bytes together with the Worker; exposed as session.resident. */
  residentBytes?: number;
  /** Immediate reserves available capacity or rejects; wait queues until capacity is available. */
  mode?: 'immediate' | 'wait';
  signal?: AbortSignal;
  /** Includes waiting and startup; defaults to queueTimeoutMs. */
  timeoutMs?: number;
}
export interface PoolDiagnostics {
  readonly resourceCacheStats: Readonly<CacheStats>;
  readonly resources: readonly ResourceCacheSnapshot[];
  readonly reclaim: Readonly<ReclaimStats>;
  readonly maxCapacity: number;
  readonly maxCacheBytesPerWorker: number;
  readonly adaptive: boolean;
  readonly cacheStats: Readonly<CacheStats>;
  readonly name: string;
  readonly capacity: number;
  readonly workers: number;
  readonly starting: number;
  readonly running: number;
  readonly idle: number;
  readonly sessionIdle: number;
  readonly closing: number;
  readonly quarantined: number;
  readonly boundSessions: number;
  readonly reclaimableSessions: number;
  readonly waitingSessions: number;
  readonly queuedTasks: number;
  readonly cacheBytesPerWorker: number;
  readonly cacheReservedBytes: number;
  readonly cacheUsedBytes: number;
  readonly blockers: readonly AdmissionBlocker[];
}
export interface RuntimeDiagnostics {
  readonly memoryPressure: MemoryPressure;
  readonly limits: Readonly<RuntimeBudgets> & {
    readonly maxWorkers: number;
    readonly maxActiveTasks: number;
    readonly maxPreparingTasks: number;
    readonly maxResultLeases: number;
    readonly maxQueuedTasks: number;
    readonly maxResourceLeases: number;
  };
  readonly pools: readonly PoolDiagnostics[];
  readonly waiting: readonly {
    readonly id: string;
    readonly pool: string;
    readonly session?: string;
    readonly kind: 'task' | 'session';
    readonly reasons: readonly AdmissionBlocker[];
  }[];
}
