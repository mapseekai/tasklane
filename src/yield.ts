/** Yield to message delivery without the nested timer clamp. */
export function yieldTask(): Promise<void> {
  if (typeof globalThis.setImmediate === 'function')
    return new Promise((resolve) => globalThis.setImmediate(resolve));
  const scheduler = (globalThis as typeof globalThis & { scheduler?: { yield(): Promise<void> } })
    .scheduler;
  if (scheduler?.yield) return scheduler.yield();
  return new Promise((resolve) => {
    const channel = new MessageChannel();
    channel.port1.onmessage = () => {
      channel.port1.close();
      channel.port2.close();
      resolve();
    };
    channel.port2.postMessage(null);
  });
}
