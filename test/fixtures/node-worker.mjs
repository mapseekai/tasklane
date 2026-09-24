import { serve } from '../../dist/host.js';
import { nodeHost } from '../../dist/adapters/node.js';
import { handlers } from './handlers.mjs';
serve(nodeHost(), handlers);
