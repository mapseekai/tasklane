import type { TaskOutput } from './host.js';
import { decodePacket, encodePacket, packetBytes, type Packet } from './packet.js';
import { RuntimeError } from './errors.js';

// Private association: callers cannot forge a packet or mutate its encoded metadata.
const packets = new WeakMap<TaskOutput<unknown>, { packet: Packet; limit: number }>();

/** Snapshot once before plan disposal; binary stores retain normal ownership semantics. */
export function prepareOutput<T>(result: TaskOutput<T>, limit: number): TaskOutput<T> {
  const packet = encodePacket(result.value, limit, 0);
  let decoded = false;
  let value: T;
  const prepared = Object.freeze({
    get value(): T {
      if (!decoded) {
        value = decodePacket(packet) as T;
        decoded = true;
      }
      return value;
    },
    transfer: Object.freeze([...(result.transfer ?? [])]),
  });
  packets.set(prepared, { packet, limit });
  return prepared;
}

export function encodeOutput(
  result: TaskOutput<unknown>,
  limit: number,
  blobLimit: number,
): Packet {
  const prepared = packets.get(result);
  if (!prepared) return encodePacket(result.value, limit, blobLimit);
  // Resizable backing buffers can change while plan disposal is awaited.
  if (packetBytes(prepared.packet) > Math.min(limit, prepared.limit))
    throw new RuntimeError('BUDGET_EXCEEDED', 'Prepared output exceeds its announced bytes');
  return prepared.packet;
}
