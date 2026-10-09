import test from 'node:test';
import assert from 'node:assert/strict';
import http from 'node:http';
import type net from 'node:net';
import { Duplex } from 'node:stream';
import { setTimeout as delay } from 'node:timers/promises';
import { attachLiveServer, type Execute } from '../live.js';
// Exercise real ws upgrade/frame handling in memory. No network listener is opened.
class MemorySocket extends Duplex {
  output: Buffer[] = [];
  _read() {}
  _write(chunk: Buffer, _encoding: string, callback: (error?: Error | null) => void) { this.output.push(Buffer.from(chunk)); callback(); }
  setTimeout() { return this; }
  setNoDelay() { return this; }
  messages(): Record<string, unknown>[] {
    const bytes = Buffer.concat(this.output); let offset = bytes.indexOf('\r\n\r\n') + 4;
    const messages: Record<string, unknown>[] = [];
    while (offset >= 4 && offset + 2 <= bytes.length) {
      const opcode = bytes[offset] & 15; let length = bytes[offset + 1] & 127; offset += 2;
      if (length === 126) { if (offset + 2 > bytes.length) break; length = bytes.readUInt16BE(offset); offset += 2; }
      if (length === 127 || offset + length > bytes.length) break;
      if (opcode === 1) messages.push(JSON.parse(bytes.subarray(offset, offset + length).toString()));
      offset += length;
    }
    return messages;
  }
  command(value: unknown) { this.text(JSON.stringify(value)); }
  text(value: string, opcode = 1) {
    const data = Buffer.from(value); const header = Buffer.alloc(data.length < 126 ? 2 : 4);
    header[0] = 0x80 | opcode;
    if (data.length < 126) header[1] = 0x80 | data.length;
    else { header[1] = 0x80 | 126; header.writeUInt16BE(data.length, 2); }
    const mask = Buffer.from([1, 2, 3, 4]);
    const masked = Buffer.from(data); for (let i = 0; i < masked.length; i++) masked[i] ^= mask[i % 4];
    this.push(Buffer.concat([header, mask, masked]));
  }
}
async function waitFor(check: () => boolean) {
  const deadline = Date.now() + 1000;
  while (!check()) { if (Date.now() > deadline) throw new Error('Timed out'); await delay(5); }
}
function fixture(execute: Execute = async (_, emit) => { emit({ type: 'accepted' }); emit({ type: 'done' }); }) {
  const app = http.createServer();
  const live = attachLiveServer(app, () => ({ workers: 4, running: false, active: {} }), execute);
  const connect = (origin = 'http://127.0.0.1:3000') => {
    const socket = new MemorySocket();
    const req = new http.IncomingMessage(socket as unknown as net.Socket);
    req.url = '/ws'; req.method = 'GET'; req.headers = { host: '127.0.0.1:3000', origin,
      upgrade: 'websocket', connection: 'Upgrade', 'sec-websocket-key': Buffer.alloc(16, 1).toString('base64'), 'sec-websocket-version': '13' };
    app.emit('upgrade', req, socket, Buffer.alloc(0)); return socket;
  };
  return { live, connect };
}
test('actual WebSocket handshake sends status and correlated run results', async () => {
  const { live, connect } = fixture(async (_, emit) => {
    emit({ type: 'response', id: 'a', status: 'OK', result: 'HELLO' }); emit({ type: 'done' });
  }); const socket = connect();
  try {
    assert.match(Buffer.concat(socket.output).toString(), /101 Switching Protocols/);
    assert.equal(socket.messages()[0].type, 'snapshot');
    socket.command({ type: 'run', runId: 'run-1', input: { mode: 'custom', messages: 'a|UPPER|hello' } });
    await waitFor(() => socket.messages().some(e => e.type === 'done'));
    const result = socket.messages().find(e => e.type === 'response');
    assert.equal(result?.runId, 'run-1'); assert.equal(result?.result, 'HELLO');
  } finally { live.close(); }
});
test('WebSocket origin checks reject foreign pages', () => {
  const { live, connect } = fixture();
  try { const socket = connect('https://foreign.example'); assert.equal(socket.destroyed, true); }
  finally { live.close(); }
});
test('malformed JSON, invalid batches, and preset commands return errors', async () => {
  let executions = 0;
  const { live, connect } = fixture(async () => { executions++; }); const socket = connect();
  try {
    socket.text('{broken');
    socket.command({ type: 'run', runId: 'bad', input: { mode: 'custom', messages: 'invalid' } });
    socket.command({ type: 'run', runId: 'preset', input: { mode: 'compare' } });
    await waitFor(() => socket.messages().filter(e => e.type === 'error').length === 3);
    assert.equal(executions, 0);
    assert.equal(socket.messages().filter(e => e.type === 'error')[1].runId, 'bad');
  } finally { live.close(); }
});
test('cancel targets the matching run and aborts its TCP bridge signal', async () => {
  let cancelled = false;
  const { live, connect } = fixture(async (_, emit, signal) => {
    emit({ type: 'accepted' });
    await new Promise<void>(resolve => signal.addEventListener('abort', () => { cancelled = true; resolve(); }, { once: true }));
    emit({ type: 'cancelled', message: 'Run cancelled' });
  }); const socket = connect();
  try {
    socket.command({ type: 'run', runId: 'active', input: { mode: 'custom', messages: 'a|DELAY|500' } });
    await waitFor(() => socket.messages().some(e => e.type === 'accepted'));
    socket.command({ type: 'cancel', runId: 'wrong' });
    await waitFor(() => socket.messages().some(e => e.type === 'error')); assert.equal(cancelled, false);
    socket.command({ type: 'cancel', runId: 'active' });
    await waitFor(() => cancelled && socket.messages().some(e => e.type === 'cancelled'));
  } finally { live.close(); }
});
test('WebSocket disconnect cancels its run without replaying it', async () => {
  let cancelled = false;
  const { live, connect } = fixture(async (_, emit, signal) => {
    emit({ type: 'accepted' });
    await new Promise<void>(resolve => signal.addEventListener('abort', () => { cancelled = true; resolve(); }, { once: true }));
  }); const socket = connect();
  try {
    socket.command({ type: 'run', runId: 'active', input: { mode: 'custom', messages: 'a|DELAY|500' } });
    await waitFor(() => socket.messages().some(e => e.type === 'accepted')); socket.destroy();
    await waitFor(() => cancelled);
  } finally { live.close(); }
});
