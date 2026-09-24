import { serve, browserHost } from '../../dist/host.js';
import { handlers } from './handlers.mjs';
serve(browserHost(self), handlers);
