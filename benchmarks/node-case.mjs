import { nodeWorker } from '../dist/adapters/node.js';
import { runCase } from './runner.mjs';
const config = JSON.parse(process.argv[2]);
const result = await runCase(
  config,
  nodeWorker(new URL('../test/fixtures/node-worker.mjs', import.meta.url)),
  nodeWorker(new URL('./raw-node-worker.mjs', import.meta.url)),
  () => process.memoryUsage().rss,
);
console.log(JSON.stringify(result));
