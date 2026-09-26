import { serve, browserHost, output } from '../../dist/host.js';

const port = browserHost(self);
const gates = new Map(),
  released = new Set();
port.onMessage((message) => {
  if (Number.isInteger(message?.maintenanceGate)) {
    released.add(message.maintenanceGate);
    gates.get(message.maintenanceGate)?.();
  }
});
serve(port, {
  ping: () => output(null),
  async hold(id) {
    if (!released.has(id)) await new Promise((resolve) => gates.set(id, resolve));
    return output(null);
  },
  churn(_, ctx) {
    ctx.cache.get('missing');
    ctx.cache.setBinary('a', new Uint8Array(24));
    ctx.cache.setBinary('b', new Uint8Array(24));
    return output(null);
  },
});
