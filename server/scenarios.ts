import { performance } from 'node:perf_hooks';
import { sendBatch } from './client.js';
import { parseBatch } from './request.js';
import type { TcpJobServer } from './tcp.js';
export type RunInput = { mode: 'custom' | 'compare' | 'mixed' | 'clients'; workers?: number; messages?: string };
export type Emit = (event: Record<string, unknown>) => void;
export function validateRun(value: unknown): RunInput {
  if (!value || typeof value !== 'object') throw new Error('Expected a run object');
  const input = value as RunInput;
  if (!['custom', 'compare', 'mixed', 'clients'].includes(input.mode)) throw new Error('Unknown demonstration');
  if (input.workers !== undefined && ![1, 4].includes(input.workers)) throw new Error('Choose 1 or 4 workers');
  if (input.mode === 'custom') {
    if (typeof input.messages !== 'string') throw new Error('Enter requests');
    parseBatch(input.messages);
  }
  return input;
}
export async function runScenario(server: TcpJobServer, port: number, input: RunInput, emit: Emit, signal?: AbortSignal) {
  const phase = async (workers: number, batches: string[][], label: string) => {
    if (signal?.aborted) throw new Error('Run cancelled');
    // Client/server close events may land on adjacent event-loop turns.
    await server.waitUntilIdle();
    await server.setWorkers(workers);
    emit({ type: 'phase', label, workers, clients: batches.length });
    const started = performance.now();
    // allSettled keeps the phase alive until every TCP client has cleaned up.
    const results = await Promise.allSettled(batches.map((lines, index) =>
      sendBatch(port, lines, `Client ${index + 1}`, response => emit({ type: 'response', ...response }), signal)));
    const error = results.find(result => result.status === 'rejected');
    if (error?.status === 'rejected') throw error.reason;
    const elapsedMs = Math.round(performance.now() - started);
    emit({ type: 'summary', label, workers, elapsedMs });
    return elapsedMs;
  };
  if (input.mode === 'compare') {
    const jobs = Array.from({ length: 4 }, (_, i) => `r${i + 1}|DELAY|2000`);
    const sequentialMs = await phase(1, [jobs], 'One worker');
    const parallelMs = await phase(4, [jobs], 'Four workers');
    emit({ type: 'comparison', sequentialMs, parallelMs, speedup: Number((sequentialMs / parallelMs).toFixed(2)) });
  } else if (input.mode === 'mixed') {
    await phase(4, [['slow|DELAY|3000', 'fast|UPPER|hello world']], 'Slow + fast');
  } else if (input.mode === 'clients') {
    await phase(4, Array.from({ length: 3 }, (_, c) => [
      `c${c + 1}-a|DELAY|1000`, `c${c + 1}-b|PRIME_COUNT|500000`, `c${c + 1}-c|UPPER|hello from client ${c + 1}`
    ]), 'Three clients');
  } else await phase(input.workers ?? server.state.workers, [parseBatch(input.messages!)], 'Custom batch');
}
