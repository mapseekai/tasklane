import { serve } from '../../dist/host.js';
import { nodeHost } from '../../dist/adapters/node.js';
import { fileHandlers } from '../../examples/file-chunks/handlers.mjs';

serve(nodeHost(), fileHandlers);
