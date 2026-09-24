import { createServer } from 'node:http';
import { readFile, realpath } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve, relative, extname, sep } from 'node:path';

const root = fileURLToPath(new URL('../', import.meta.url));
const mime = {
  '.mjs': 'text/javascript',
  '.js': 'text/javascript',
  '.html': 'text/html',
  '.css': 'text/css',
  '.json': 'application/json',
};
export async function startServer(port = 4196) {
  const server = createServer(async (req, res) => {
    try {
      const pathname = decodeURIComponent(new URL(req.url, 'http://localhost').pathname);
      const path = await realpath(
        resolve(root, `.${pathname === '/' ? '/examples/index.html' : pathname}`),
      );
      const local = relative(root, path);
      if (
        local.startsWith(`..${sep}`) ||
        local === '..' ||
        local.split(sep).some((part) => part.startsWith('.'))
      )
        throw new Error('Outside public root');
      if (!mime[extname(path)]) throw new Error('Unsupported resource');
      res.writeHead(200, {
        'Content-Type': mime[extname(path)],
        'Cache-Control': 'no-store',
        'Content-Security-Policy':
          "default-src 'self'; script-src 'self'; worker-src 'self'; style-src 'self'",
      });
      res.end(await readFile(path));
    } catch {
      res.writeHead(404);
      res.end('Not found');
    }
  });
  await new Promise((resolve, reject) => {
    server.once('error', reject);
    server.listen(port, '127.0.0.1', resolve);
  });
  return server;
}
if (process.argv[1] === fileURLToPath(import.meta.url)) {
  await startServer(Number(process.env.PORT || 4196));
  console.log('tasklane: http://127.0.0.1:4196');
}
