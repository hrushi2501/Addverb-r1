import { parentPort, workerData } from 'node:worker_threads';
import { performance } from 'node:perf_hooks';
import type { Request } from './protocol.js';
if (!parentPort) throw new Error('Run this file as a worker');
const port = parentPort;
port.on('message', async (request: Request) => {
  const started = performance.now();
  port.postMessage({ type: 'start', at: Date.now(), worker: workerData.name });
  try {
    let result: string;
    if (request.operation === 'UPPER') result = request.argument.toUpperCase();
    else if (request.operation === 'DELAY') {
      await new Promise(resolve => setTimeout(resolve, Number(request.argument)));
      result = `Simulated processing: ${request.argument}ms`;
    } else {
      // Trial division is intentionally straightforward CPU work, not a benchmark library.
      const limit = Number(request.argument);
      let count = 0;
      for (let n = 2; n <= limit; n++) {
        let prime = true;
        for (let divisor = 2; divisor * divisor <= n; divisor++) {
          if (n % divisor === 0) { prime = false; break; }
        }
        if (prime) count++;
      }
      result = String(count);
    }
    port.postMessage({ type: 'end', at: Date.now(), worker: workerData.name,
      durationMs: Math.round(performance.now() - started), status: 'OK', result });
  } catch (error) {
    port.postMessage({ type: 'end', at: Date.now(), worker: workerData.name,
      durationMs: Math.round(performance.now() - started), status: 'ERROR', result: String(error) });
  }
});
