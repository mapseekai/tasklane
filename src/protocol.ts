import type { ErrorCode } from './errors.js';

export const PROTOCOL_VERSION = 1;
export const PROTOCOL_TAG = '@mapseekai/worker-runtime';

export interface Header {
  tag: typeof PROTOCOL_TAG;
  version: typeof PROTOCOL_VERSION;
  epoch: number;
}
export interface WireError {
  code: ErrorCode;
  name: string;
  message: string;
  stack?: string;
}
export type RequestMessage = Header & {
  type: 'request';
  id: string;
  scope: string;
  session?: string;
  task: string;
  payload: unknown;
  maxOutputBytes: number;
};
export type ToWorker =
  | (Header & { type: 'hello'; cacheBytes: number; cacheEntries: number })
  | RequestMessage
  | (Header & { type: 'cancel'; id: string; scope: string })
  | (Header & { type: 'release-scope'; scope: string });
export type FromWorker =
  | (Header & { type: 'ready'; tasks: string[] })
  | (Header & { type: 'progress'; id: string; scope: string; value: unknown })
  | (Header & {
      type: 'result';
      id: string;
      scope: string;
      value: unknown;
      byteLength: number;
      workerMs: number;
      cacheBytes: number;
    })
  | (Header & {
      type: 'error' | 'cancelled';
      id: string;
      scope: string;
      error: WireError;
      workerMs: number;
      cacheBytes: number;
    })
  | (Header & { type: 'released'; scope: string; cacheBytes: number });

export function header(epoch: number): Header {
  return { tag: PROTOCOL_TAG, version: PROTOCOL_VERSION, epoch };
}

export function isHeader(value: unknown): value is Header & { type: string } {
  if (!value || typeof value !== 'object') return false;
  const message = value as Record<string, unknown>;
  return (
    message.tag === PROTOCOL_TAG &&
    message.version === PROTOCOL_VERSION &&
    typeof message.type === 'string' &&
    Number.isSafeInteger(message.epoch) &&
    (message.epoch as number) > 0
  );
}
