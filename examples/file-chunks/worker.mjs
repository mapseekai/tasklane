import { browserHost, serve } from '../../dist/host.js';
import { fileHandlers } from './handlers.mjs';

serve(browserHost(self), fileHandlers);
