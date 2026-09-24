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
}

export interface PreparedInput<T> {
  payload: T;
  /** Explicit ownership transfer. The runtime never automatically detaches input buffers. */
  transfer?: readonly Transferable[];
}

export interface TaskOptions<Input> {
  pool: string;
  budget: TaskBudget;
  prepare(context: { signal: AbortSignal }): PreparedInput<Input> | Promise<PreparedInput<Input>>;
  priority?: Priority;
  /** Fairness is per scope + group, not merely per task. */
  group?: string;
  /** Soft, bounded cache affinity. Use a Session for required affinity. */
  affinity?: string;
  cancellation?: Cancellation;
  signal?: AbortSignal;
  queueTimeoutMs?: number;
  /** Includes startup, prepare, and physical worker execution, but not queue waiting. */
  executionTimeoutMs?: number;
  onProgress?: (value: unknown) => void;
}

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
  /** Fixed reservation per live worker. Zero means no persistent cache storage. */
  cacheBytes?: number;
  cacheEntries?: number;
  /** Hard cancellation may discard opportunistic caches. Sessions always have exclusive slots. */
  allowHardCancel?: boolean;
  /** A pooled, unpinned idle worker is reclaimed after this interval; 0 disables idle expiry. */
  idleTimeoutMs?: number;
}

export interface RuntimeOptions {
  pools: Record<string, PoolOptions>;
  maxWorkers?: number;
  maxActiveTasks?: number;
  maxQueuedTasks?: number;
  budgets?: Partial<RuntimeBudgets>;
  startupTimeoutMs?: number;
  queueTimeoutMs?: number;
  executionTimeoutMs?: number;
  /** Ageing eventually promotes waiting background tasks. */
  ageingMs?: number;
  maxAffinityEntries?: number;
  /** Observer exceptions are isolated from task execution. */
  onDiagnostic?: (error: RuntimeError) => void;
}

export interface RuntimeStats {
  queued: number;
  active: number;
  workers: number;
  closingWorkers: number;
  leases: number;
  reserved: RuntimeBudgets;
  peakReserved: RuntimeBudgets;
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
