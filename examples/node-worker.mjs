import { output, serve } from '../dist/host.js';
import { nodeHost } from '../dist/adapters/node.js';
import { transferBuffers } from '../dist/index.js';

serve(nodeHost(), {
  scale({ values, scale }) {
    const result = Float64Array.from(values, (value) => value * scale);
    return output(result, transferBuffers(result));
  },
});
