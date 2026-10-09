import net from 'node:net';
import { EventEmitter } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { WorkerPool, type JobEvent } from './pool.js';
import { LineFramer, parseRequest, responseId } from './protocol.js';
export type ServerOptions = { workers?: number; queueCapacity?: number; maxClients?: number; log?: boolean };
export class TcpJobServer extends EventEmitter {
  private listener = net.createServer({ allowHalfOpen: true }, socket => this.accept(socket));
  private clients = new Set<net.Socket>();
  private nextClient = 0;
  private stopping = false;
  private pool: WorkerPool;
  readonly options: Required<ServerOptions>;
  constructor(options: ServerOptions = {}) {
    super();
    this.options = { workers: options.workers ?? 4, queueCapacity: options.queueCapacity ?? 16,
      maxClients: options.maxClients ?? 5, log: options.log ?? true };
    this.pool = this.makePool(this.options.workers);
  }
  private makePool(workers: number): WorkerPool {
    return new WorkerPool(workers, this.options.queueCapacity, event => {
      if (this.options.log) console.log(`${new Date(event.at).toISOString().slice(11, 23)} ${event.type.toUpperCase().padEnd(5)} client=${event.client} request=${event.request} worker=${event.worker}${event.durationMs === undefined ? '' : ` duration=${event.durationMs}ms`}`);
      this.emit('job', event satisfies JobEvent);
    });
  }
  async listen(port = 9000): Promise<number> {
    try {
      await new Promise<void>((resolve, reject) => {
        this.listener.once('error', reject);
        this.listener.listen(port, '127.0.0.1', () => { this.listener.off('error', reject); resolve(); });
      });
    } catch (error) {
      this.stopping = true;
      await this.pool.close(0);
      throw error;
    }
    return (this.listener.address() as net.AddressInfo).port;
  }
  get state() { return { workers: this.pool.size, queueCapacity: this.options.queueCapacity,
    queued: this.pool.queued, pending: this.pool.pending, clients: this.clients.size, maxClients: this.options.maxClients }; }
  async waitUntilIdle(timeoutMs = 1000): Promise<void> {
    const deadline = Date.now() + timeoutMs;
    while (this.clients.size || this.pool.pending) {
      if (Date.now() >= deadline) throw new Error('Close other TCP clients and wait for pending work before running a demo');
      await delay(5);
    }
  }
  async setWorkers(workers: number): Promise<void> {
    if (![1, 4].includes(workers)) throw new Error('Choose 1 or 4 workers');
    if (this.stopping || this.clients.size || this.pool.pending) throw new Error('Server must be idle to change workers');
    await this.pool.close(1000);
    this.pool = this.makePool(workers);
  }
  private accept(socket: net.Socket): void {
    socket.on('error', () => { socket.destroy(); });
    if (this.stopping || this.clients.size >= this.options.maxClients) {
      socket.end('-|BUSY|Connection limit reached\n', () => socket.destroy()); return;
    }
    const client = `C${++this.nextClient}`;
    this.clients.add(socket); socket.setNoDelay(true);
    const inFlight = new Set<string>();
    let readerEnded = false;
    // Only the main thread writes sockets. Each response is one complete write.
    const reply = (id: string, status: string, result: string) => {
      if (socket.destroyed || !socket.writable) return;
      if (socket.writableLength > 65536) { socket.destroy(); return; }
      socket.write(`${id}|${status}|${result.replace(/[\r\n|]/g, ' ')}\n`, error => {
        if (error) socket.destroy();
      });
    };
    const framer = new LineFramer(line => {
      if (this.stopping) return;
      try {
        const request = parseRequest(line);
        if (inFlight.has(request.id)) { reply(request.id, 'ERROR', 'Request ID already in flight'); return; }
        inFlight.add(request.id);
        const accepted = this.pool.submit({ client, request, reply: (status, result) => {
          inFlight.delete(request.id); reply(request.id, status, result);
          if (readerEnded && inFlight.size === 0) socket.end();
        } });
        if (!accepted) { inFlight.delete(request.id); reply(request.id, 'BUSY', 'Job queue is full'); }
      } catch (error) { reply(responseId(line), 'ERROR', (error as Error).message); }
    }, () => reply('-', 'ERROR', 'Message exceeds 4096 bytes'));
    socket.on('data', chunk => framer.push(chunk));
    socket.on('end', () => { readerEnded = true; if (inFlight.size === 0) socket.end(); });
    socket.on('close', () => this.clients.delete(socket));
    // An incomplete final line is discarded on disconnect; it was never a job.
  }
  async close(timeoutMs = 5000): Promise<boolean> {
    if (this.stopping) return false;
    this.stopping = true;
    const listenerClosed = new Promise<void>(resolve => this.listener.close(() => resolve()));
    for (const socket of this.clients) socket.pause();
    const drained = await this.pool.close(timeoutMs);
    for (const socket of this.clients) socket.end();
    const timer = setTimeout(() => { for (const socket of this.clients) socket.destroy(); }, 250);
    await listenerClosed; clearTimeout(timer);
    return drained;
  }
}
