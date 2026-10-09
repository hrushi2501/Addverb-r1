import type http from 'node:http';
import WebSocket, { WebSocketServer } from 'ws';
import { validateRun, type Emit, type RunInput } from './scenarios.js';
export type Execute = (input: RunInput, emit: Emit, signal: AbortSignal) => Promise<void>;
// WebSocket transport only. Jobs still enter the backend through a real TCP client.
export function attachLiveServer(app: http.Server, snapshot: () => Record<string, unknown>, execute: Execute) {
  const sockets = new WebSocketServer({ noServer: true, maxPayload: 32768, perMessageDeflate: false });
  const alive = new Map<WebSocket, boolean>();
  const runs = new Map<WebSocket, { id: string; controller: AbortController }>();
  const send = (socket: WebSocket, event: Record<string, unknown>) => {
    if (socket.readyState !== WebSocket.OPEN) return;
    if (socket.bufferedAmount > 262144) { socket.terminate(); return; }
    socket.send(JSON.stringify(event), error => { if (error) socket.terminate(); });
  };
  const broadcastStatus = () => {
    const event = { type: 'snapshot', ...snapshot() };
    for (const socket of sockets.clients) send(socket, event);
  };
  app.on('upgrade', (req, socket, head) => {
    if (req.url !== '/ws' || (req.headers.origin && req.headers.origin !== `http://${req.headers.host}`) || sockets.clients.size >= 16) {
      socket.destroy(); return;
    }
    sockets.handleUpgrade(req, socket, head, ws => sockets.emit('connection', ws, req));
  });
  sockets.on('connection', socket => {
    alive.set(socket, true);
    send(socket, { type: 'snapshot', ...snapshot() });
    socket.on('pong', () => alive.set(socket, true));
    socket.on('error', () => socket.terminate());
    socket.on('close', () => {
      alive.delete(socket); runs.get(socket)?.controller.abort(); runs.delete(socket);
    });
    socket.on('message', (data, binary) => {
      let id: string | undefined;
      try {
        if (binary) throw new Error('Send JSON text');
        const command = JSON.parse(data.toString());
        id = typeof command.runId === 'string' && /^[\w-]{1,64}$/.test(command.runId) ? command.runId : undefined;
        if (!id) throw new Error('A valid runId is required');
        if (command.type === 'cancel') {
          const run = runs.get(socket);
          if (!run || run.id !== id) throw new Error('No matching batch is running');
          run.controller.abort(); return;
        }
        if (command.type !== 'run') throw new Error('Unknown WebSocket command');
        if (runs.has(socket)) throw new Error('Wait for your current batch to finish');
        const input = validateRun(command.input);
        if (input.mode !== 'custom') throw new Error('Enter your own batch; presets are available through the CLI only');
        const controller = new AbortController(); runs.set(socket, { id, controller });
        const runId = id;
        void execute(input, event => send(socket, { ...event, runId }), controller.signal)
          .catch(error => send(socket, { type: 'error', runId, message: (error as Error).message }))
          .finally(() => { runs.delete(socket); broadcastStatus(); });
      } catch (error) { send(socket, { type: 'error', runId: id, message: (error as Error).message }); }
    });
  });
  const statusTimer = setInterval(broadcastStatus, 250);
  const heartbeat = setInterval(() => {
    for (const [socket, responsive] of alive) {
      if (!responsive) { socket.terminate(); continue; }
      alive.set(socket, false); socket.ping();
    }
  }, 15000);
  return {
    broadcastStatus,
    close() {
      clearInterval(statusTimer); clearInterval(heartbeat);
      for (const run of runs.values()) run.controller.abort();
      for (const socket of sockets.clients) socket.terminate();
      sockets.close();
    }
  };
}
