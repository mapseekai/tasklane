import { browserHost, output, serve } from '../dist/host.js';
import { transferBuffers } from '../dist/index.js';
import { convert } from '../benchmarks/workloads.mjs';
serve(browserHost(self), {
  convert(payload) {
    const result = convert(payload);
    return output(result, transferBuffers(result.vertices, result.bounds));
  },
});
