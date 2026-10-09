import net from 'node:net';
import { performance } from 'node:perf_hooks';
import { LineFramer, type Response } from './protocol.js';
export type ClientResponse = Response & { client: string; elapsedMs: number };
// Send the entire batch before collecting responses: one socket, many independent jobs.
export function sendBatch(port: number, lines: string[], client = 'Client 1',
  onResponse: (response: ClientResponse) => void = () => {}, signal?: AbortSignal): Promise<ClientResponse[]> {
  return new Promise((resolve, reject) => {
    const socket = net.createConnection({ host: '127.0.0.1', port });
    const started = performance.now();
    const responses: ClientResponse[] = [];
    let finished = false;
    let failure: Error | undefined;
    const cleanup = () => { clearTimeout(timer); signal?.removeEventListener('abort', abort); };
    const fail = (error: Error) => { if (!finished) { failure = error; finished = true; socket.destroy(); } };
    const abort = () => fail(new Error('Run cancelled'));
    const timer = setTimeout(() => fail(new Error('TCP batch timed out')), 60000);
    signal?.addEventListener('abort', abort, { once: true });
    const framer = new LineFramer(line => {
      const [id, status, ...result] = line.split('|');
      const response = { id, status, result: result.join('|'), client, elapsedMs: Math.round(performance.now() - started) };
      responses.push(response); onResponse(response);
      if (id === '-' && status === 'BUSY') { fail(new Error('TCP connection limit reached')); return; }
      if (responses.length === lines.length) { finished = true; socket.end(); }
    }, () => fail(new Error('Oversized server response')), 16384);
    socket.on('connect', () => {
      if (signal?.aborted) { abort(); return; }
      if (lines.length === 0) { finished = true; socket.end(); }
      else socket.write(lines.join('\n') + '\n');
    });
    socket.on('data', chunk => framer.push(chunk));
    socket.on('error', fail);
    socket.on('close', () => {
      cleanup();
      if (failure) reject(failure);
      else if (finished) resolve(responses);
      else reject(new Error(`Connection closed after ${responses.length}/${lines.length} responses`));
    });
  });
}
