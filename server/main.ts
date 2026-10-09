import http from 'node:http';
import { readFile } from 'node:fs/promises';
import { extname, resolve, sep } from 'node:path';
import { fileURLToPath } from 'node:url';
import { TcpJobServer } from './tcp.js';
import { runScenario, validateRun, type Emit, type RunInput } from './scenarios.js';
import { attachLiveServer } from './live.js';
import type { JobEvent } from './pool.js';
const positiveInt = (name: string, fallback: number) => {
  const value = Number(process.env[name] ?? fallback);
  if (!Number.isInteger(value) || value < 1 || value > 65535) throw new Error(`Invalid ${name}`);
  return value;
};
const tcp = new TcpJobServer({ workers: positiveInt('WORKERS', 4), queueCapacity: positiveInt('QUEUE_CAPACITY', 16) });
const port = await tcp.listen(positiveInt('TCP_PORT', 9000));
const webRoot = resolve(fileURLToPath(new URL('../web/', import.meta.url)));
let running = false;
let stopping = false;
const controllers = new Set<AbortController>();
const active: Record<string, JobEvent> = {};
tcp.on('job', (event: JobEvent) => {
  if (event.type === 'start') active[event.worker] = event;
  else delete active[event.worker];
});
const snapshot = () => ({ ...tcp.state, tcpPort: port, running, active });
async function execute(input: RunInput, emit: Emit, signal: AbortSignal): Promise<void> {
  if (running || stopping) throw new Error('Another batch is running. Try again when it finishes.');
  running = true;
  const controller = new AbortController(); controllers.add(controller);
  const abort = () => controller.abort();
  signal.addEventListener('abort', abort, { once: true });
  if (signal.aborted) abort();
  const onJob = (event: JobEvent) => emit({ ...event });
  tcp.on('job', onJob);
  try {
    emit({ type: 'accepted' });
    await runScenario(tcp, port, input, emit, controller.signal);
    emit({ type: 'done' });
  } catch (error) {
    emit({ type: controller.signal.aborted ? 'cancelled' : 'error', message: (error as Error).message });
  } finally {
    tcp.off('job', onJob); signal.removeEventListener('abort', abort);
    controllers.delete(controller); running = false;
  }
}
const json = (res: http.ServerResponse, status: number, body: unknown) => {
  res.writeHead(status, { 'Content-Type': 'application/json' }); res.end(JSON.stringify(body));
};
const app = http.createServer(async (req, res) => {
  try {
    const path = new URL(req.url ?? '/', 'http://localhost').pathname;
    if (req.method === 'GET' && path === '/api/status') { json(res, 200, snapshot()); return; }
    if (req.method === 'POST' && path === '/api/run') {
      if (running || stopping) { json(res, 409, { error: 'Another demonstration is running. Try again when it finishes.' }); return; }
      if (!req.headers['content-type']?.startsWith('application/json')) { json(res, 415, { error: 'Use application/json' }); return; }
      if (req.headers.origin && req.headers.origin !== `http://${req.headers.host}`) { json(res, 403, { error: 'Use the local demo page' }); return; }
      const chunks: Buffer[] = [];
      let bytes = 0;
      for await (const chunk of req) {
        bytes += chunk.length;
        if (bytes > 32768) throw new Error('Batch exceeds 32KB');
        chunks.push(chunk);
      }
      const input = validateRun(JSON.parse(Buffer.concat(chunks).toString('utf8')));
      // Recheck after reading the body: another request may have acquired the demo slot.
      if (running || stopping) { json(res, 409, { error: 'Another demonstration is running' }); return; }
      const controller = new AbortController();
      res.writeHead(200, { 'Content-Type': 'application/x-ndjson', 'Cache-Control': 'no-store', 'X-Content-Type-Options': 'nosniff' });
      res.flushHeaders();
      const emit: Emit = event => {
        if (res.destroyed || res.writableEnded) return;
        if (res.writableLength > 262144) { controller.abort(); res.destroy(); return; }
        res.write(JSON.stringify(event) + '\n');
      };
      res.on('close', () => { if (!res.writableEnded) controller.abort(); });
      try { await execute(input, emit, controller.signal); }
      catch (error) { emit({ type: 'error', message: (error as Error).message }); }
      finally { res.end(); }
      return;
    }
    if (req.method !== 'GET') { json(res, 404, { error: 'Not found' }); return; }
    const file = resolve(webRoot, '.' + decodeURIComponent(path === '/' ? '/index.html' : path));
    if (!file.startsWith(webRoot + sep) && file !== resolve(webRoot, 'index.html')) { json(res, 403, { error: 'Forbidden' }); return; }
    const types: Record<string, string> = { '.html': 'text/html', '.js': 'text/javascript', '.css': 'text/css', '.svg': 'image/svg+xml' };
    try {
      const data = await readFile(file);
      res.writeHead(200, { 'Content-Type': types[extname(file)] ?? 'application/octet-stream' }); res.end(data);
    } catch { json(res, 404, { error: 'Not found. Run npm run build first.' }); }
  } catch (error) {
    if (!res.headersSent) json(res, 400, { error: (error as Error).message });
    else res.end();
  }
});
const live = attachLiveServer(app, snapshot, execute);
app.requestTimeout = 15000;
app.listen(positiveInt('PORT', 3000), '127.0.0.1', () => {
  console.log(`Frontend: http://127.0.0.1:${positiveInt('PORT', 3000)} | TCP: 127.0.0.1:${port}`);
});
async function shutdown() {
  if (stopping) return;
  stopping = true;
  console.log('Shutdown: stop accepting, pause readers, drain accepted jobs (5s timeout).');
  const httpClosed = new Promise<void>(resolve => app.close(() => resolve()));
  live.close();
  for (const controller of controllers) controller.abort();
  const drained = await tcp.close(5000);
  app.closeAllConnections(); await httpClosed;
  console.log(drained ? 'Shutdown complete: accepted jobs drained.' : 'Shutdown complete: timeout; remaining jobs cancelled.');
}
process.on('SIGINT', () => { void shutdown(); });
process.on('SIGTERM', () => { void shutdown(); });
