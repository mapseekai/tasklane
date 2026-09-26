import type { Packet } from './packet.js';
import type { ErrorCode, RemoteErrorInfo } from './errors.js';
import type { CacheStats, ResourceCacheSnapshot } from './types.js';

export const PROTOCOL_VERSION = 7;
export const PROTOCOL_TAG = '@mapseekai/tasklane';

export interface Header {
  tag: typeof PROTOCOL_TAG;
  version: typeof PROTOCOL_VERSION;
  epoch: number;
}
export interface WireError extends Omit<RemoteErrorInfo, 'code'> {
  code: ErrorCode;
  remoteCode?: string;
}

export type RequestMessage = Header & {
  type: 'request';
  id: string;
  scope: string;
  session?: string;
  task: string;
  payload: Packet;
  maxOutputBytes: number;
  maxOutputBlobBytes: number;
  maxScratchBytes: number;
};
export type ToWorker =
  | (Header & { type: 'cache-control'; id: string; limit: number; trim: boolean })
  | (Header & { type: 'hello'; cacheBytes: number; cacheEntries: number })
  | RequestMessage
  | (Header & { type: 'progress-ack'; id: string; scope: string })
  | (Header & { type: 'cancel'; id: string; scope: string })
  | (Header & { type: 'release-scope'; scope: string });
export type FromWorker =
  | (Header & {
      type: 'cache-controlled';
      id: string;
      limit: number;
      cacheBytes: number;
      cacheStats?: CacheStats;
      resourceCacheStats?: CacheStats;
      resourceReports?: ResourceCacheSnapshot[];
      error?: WireError;
    })
  | (Header & { type: 'ready'; tasks: string[] })
  | (Header & { type: 'progress'; id: string; scope: string; value: unknown })
  | (Header & {
      type: 'result';
      id: string;
      scope: string;
      value: Packet;
      byteLength: number;
      workerMs: number;
      cacheBytes: number;
      cacheStats?: CacheStats;
      resourceCacheStats?: CacheStats;
      resourceReports?: ResourceCacheSnapshot[];
    })
  | (Header & {
      type: 'error' | 'cancelled';
      id: string;
      scope: string;
      error: WireError;
      workerMs: number;
      cacheBytes: number;
      cacheStats?: CacheStats;
      resourceCacheStats?: CacheStats;
      resourceReports?: ResourceCacheSnapshot[];
    })
  | (Header & {
      type: 'released';
      scope: string;
      cacheBytes: number;
      cacheStats?: CacheStats;
      resourceCacheStats?: CacheStats;
      resourceReports?: ResourceCacheSnapshot[];
      error?: WireError;
    });

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
