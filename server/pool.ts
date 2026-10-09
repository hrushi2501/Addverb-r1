import { Worker } from 'node:worker_threads';
import type { Request } from './protocol.js';
export type Job = { client: string; request: Request; reply: (status: string, result: string) => void };
export type JobEvent = { type: 'start' | 'end'; at: number; worker: string; client: string; request: string;
  operation: string; durationMs?: number; status?: string; result?: string };
type Slot = { worker: Worker; name: string; job?: Job; startedAt?: number; failed: boolean };
export class WorkerPool {
  private queue: Job[] = [];
  private slots: Slot[] = [];
  private closing = false;
  private idleWaiters: (() => void)[] = [];
  constructor(readonly size: number, readonly capacity: number, private event: (event: JobEvent) => void) {
    if (!Number.isInteger(size) || size < 1 || size > 4 || !Number.isInteger(capacity) || capacity < 1)
      throw new Error('Invalid pool configuration');
    for (let i = 1; i <= size; i++) this.slots.push(this.createSlot(`worker-${i}`));
  }
  private createSlot(name: string): Slot {
    const worker = new Worker(new URL('./worker.js', import.meta.url), { workerData: { name } });
    const slot: Slot = { worker, name, failed: false };
    worker.on('message', message => {
      const job = slot.job;
      if (!job || slot.failed) return;
      if (message.type === 'start') slot.startedAt = message.at;
      this.event({ ...message, client: job.client, request: job.request.id, operation: job.request.operation });
      if (message.type === 'end') {
        job.reply(message.status, message.result);
        slot.job = undefined; slot.startedAt = undefined;
        this.dispatch();
      }
    });
    const fail = () => {
      if (slot.failed) return;
      slot.failed = true;
      if (slot.job) {
        this.event({ type: 'end', at: Date.now(), worker: name, client: slot.job.client,
          request: slot.job.request.id, operation: slot.job.request.operation, durationMs: slot.startedAt === undefined ? 0 : Date.now() - slot.startedAt, status: 'ERROR', result: 'Worker stopped' });
        slot.job.reply('ERROR', 'Worker stopped'); slot.job = undefined; slot.startedAt = undefined;
      }
      if (!this.closing) {
        const index = this.slots.indexOf(slot);
        if (index >= 0) this.slots[index] = this.createSlot(name);
      }
      this.dispatch();
    };
    worker.on('error', fail);
    worker.on('exit', code => { if (code !== 0) fail(); });
    return slot;
  }
  submit(job: Job): boolean {
    if (this.closing) return false;
    const idle = this.slots.find(slot => !slot.job && !slot.failed);
    if (idle) { idle.job = job; idle.worker.postMessage(job.request); return true; }
    if (this.queue.length >= this.capacity) return false;
    this.queue.push(job); return true;
  }
  private dispatch(): void {
    for (const slot of this.slots) {
      if (!slot.job && !slot.failed && this.queue.length) {
        slot.job = this.queue.shift()!; slot.worker.postMessage(slot.job.request);
      }
    }
    if (this.pending === 0) this.idleWaiters.splice(0).forEach(resolve => resolve());
  }
  get pending(): number { return this.queue.length + this.slots.filter(slot => slot.job).length; }
  get queued(): number { return this.queue.length; }
  async close(timeoutMs: number): Promise<boolean> {
    this.closing = true;
    let timer: NodeJS.Timeout | undefined;
    const drained = this.pending === 0 || await Promise.race([
      new Promise<boolean>(resolve => this.idleWaiters.push(() => resolve(true))),
      new Promise<boolean>(resolve => { timer = setTimeout(() => resolve(false), timeoutMs); })
    ]);
    clearTimeout(timer);
    if (!drained) {
      for (const job of this.queue.splice(0)) job.reply('ERROR', 'Shutdown timeout');
      for (const slot of this.slots) {
        if (slot.job) {
          this.event({ type: 'end', at: Date.now(), worker: slot.name, client: slot.job.client,
            request: slot.job.request.id, operation: slot.job.request.operation,
            durationMs: slot.startedAt === undefined ? 0 : Date.now() - slot.startedAt,
            status: 'ERROR', result: 'Shutdown timeout' });
          slot.job.reply('ERROR', 'Shutdown timeout'); slot.job = undefined; slot.startedAt = undefined;
        }
      }
    }
    await Promise.all(this.slots.map(slot => slot.worker.terminate()));
    return drained;
  }
}
