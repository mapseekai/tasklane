/** Stable error codes are part of the public contract; messages are diagnostic. */
export const ERROR_CODES = [
  'ABORTED',
  'CLOSED',
  'QUEUE_FULL',
  'QUEUE_TIMEOUT',
  'EXECUTION_TIMEOUT',
  'STARTUP_TIMEOUT',
  'WORKER_FAILED',
  'PROTOCOL_ERROR',
  'UNKNOWN_TASK',
  'BUDGET_EXCEEDED',
  'INVALID_ARGUMENT',
  'SESSION_LOST',
  'HARD_CANCEL_DENIED',
  'RESULT_RELEASED',
  'REMOTE_ERROR',
] as const;
export type ErrorCode = (typeof ERROR_CODES)[number];

export type ErrorDetail =
  | null
  | boolean
  | number
  | string
  | ErrorDetail[]
  | { [key: string]: ErrorDetail };
export interface RemoteErrorInfo {
  name: string;
  code?: string;
  message: string;
  stack?: string;
  details?: ErrorDetail;
  detailsOmitted?: boolean;
  truncated?: boolean;
}

export class RuntimeError extends Error {
  override readonly name = 'RuntimeError';
  readonly remoteError?: Readonly<RemoteErrorInfo>;
  constructor(
    readonly code: ErrorCode,
    message: string,
    options?: ErrorOptions & { remoteError?: RemoteErrorInfo },
  ) {
    super(message, options);
    this.remoteError = options?.remoteError;
  }
}

export function asError(value: unknown): Error {
  return value instanceof Error ? value : new Error(String(value));
}

export function aborted(reason?: unknown): RuntimeError {
  return new RuntimeError(
    'ABORTED',
    reason === undefined ? 'Task cancelled' : asError(reason).message,
    {
      cause: reason,
    },
  );
}

export function integer(value: number, name: string, minimum = 0): number {
  if (!Number.isSafeInteger(value) || value < minimum) {
    throw new RuntimeError('INVALID_ARGUMENT', `${name} must be a safe integer >= ${minimum}`);
  }
  return value;
}

export function timeout(value: number, name: string): number {
  integer(value, name, 1);
  if (value > 2_147_483_647) {
    throw new RuntimeError('INVALID_ARGUMENT', `${name} exceeds the platform timer range`);
  }
  return value;
}

/** Explicit invariant check at protocol/lifecycle boundaries. */
export function required<T>(value: T | undefined | null, name: string): T {
  if (value === undefined || value === null)
    throw new RuntimeError('PROTOCOL_ERROR', name + ' is missing');
  return value;
}
