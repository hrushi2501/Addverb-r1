import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
import { TcpJobServer } from '../tcp.js';
import { sendBatch } from '../client.js';
import type { JobEvent } from '../pool.js';
async function fixture(options: ConstructorParameters<typeof TcpJobServer>[0] = {}) {
  const server = new TcpJobServer({ log: false, ...options });
  const port = await server.listen(0);
  return { server, port };
}
async function waitFor(check: () => boolean, timeout = 2000) {
  const end = Date.now() + timeout;
  while (!check()) { if (Date.now() > end) throw new Error('Condition timed out'); await delay(5); }
}
async function raw(port: number) {
  const socket = net.createConnection({ port, host: '127.0.0.1' });
  const lines: string[] = []; let buffer = '';
  socket.on('error', () => {});
  socket.on('data', chunk => { buffer += chunk.toString(); let end: number;
    while ((end = buffer.indexOf('\n')) >= 0) { lines.push(buffer.slice(0, end)); buffer = buffer.slice(end + 1); }
  });
  await once(socket, 'connect');
  return { socket, lines };
}
test('one TCP client feeds four simultaneously active workers', async () => {
  const { server, port } = await fixture(); const events: JobEvent[] = [];
  server.on('job', event => events.push(event));
  try {
    const responses = await sendBatch(port, Array.from({ length: 4 }, (_, i) => `r${i}|DELAY|180`));
    assert.equal(responses.length, 4);
    const starts = events.filter(e => e.type === 'start'); const ends = events.filter(e => e.type === 'end');
    assert.equal(new Set(starts.map(e => e.client)).size, 1);
    assert.equal(new Set(starts.map(e => e.worker)).size, 4);
    assert.ok(Math.max(...starts.map(e => e.at)) < Math.min(...ends.map(e => e.at)), 'all four start before the first ends');
  } finally { await server.close(); }
});
test('same-client CPU jobs run on separate workers and compute correct counts', async () => {
  const { server, port } = await fixture(); const events: JobEvent[] = [];
  server.on('job', event => events.push(event));
  try {
    const responses = await sendBatch(port, Array.from({ length: 4 }, (_, i) => `p${i}|PRIME_COUNT|1000000`));
    assert.ok(responses.every(r => r.result === '78498' && r.status === 'OK'));
    assert.equal(new Set(events.filter(e => e.type === 'start').map(e => e.worker)).size, 4);
    const starts = events.filter(e => e.type === 'start'); const ends = events.filter(e => e.type === 'end');
    assert.ok(Math.max(...starts.map(e => e.at)) < Math.min(...ends.map(e => e.at)));
  } finally { await server.close(); }
});
test('UPPER finishes before the preceding slow request', async () => {
  const { server, port } = await fixture();
  try { const responses = await sendBatch(port, ['slow|DELAY|200', 'fast|UPPER|hello']);
    assert.deepEqual(responses.map(r => r.id), ['fast', 'slow']); assert.equal(responses[0].result, 'HELLO');
  } finally { await server.close(); }
});
test('three clients receive only their own results', async () => {
  const { server, port } = await fixture();
  try {
    const groups = await Promise.all(Array.from({ length: 3 }, (_, c) => sendBatch(port,
      Array.from({ length: 4 }, (_, i) => `c${c}-${i}|UPPER|client ${c} message ${i}`))));
    groups.forEach((group, c) => { assert.equal(group.length, 4); assert.ok(group.every(r => r.id.startsWith(`c${c}-`) && r.result.startsWith(`CLIENT ${c}`))); });
  } finally { await server.close(); }
});
test('split UTF-8 frames and coalesced frames are reconstructed', async () => {
  const { server, port } = await fixture(); const { socket, lines } = await raw(port);
  try {
    const message = Buffer.from('u|UPPER|héllo\n'); const split = message.indexOf(0xc3) + 1;
    socket.write(message.subarray(0, split)); await delay(20); assert.equal(lines.length, 0);
    socket.write(Buffer.concat([message.subarray(split), Buffer.from('v|UPPER|two\nw|UPPER|three\n')]));
    await waitFor(() => lines.length === 3);
    assert.deepEqual(new Set(lines), new Set(['u|OK|HÉLLO', 'v|OK|TWO', 'w|OK|THREE']));
  } finally { socket.destroy(); await server.close(); }
});
test('invalid and oversized messages return ERROR and framing recovers', async () => {
  const { server, port } = await fixture(); const { socket, lines } = await raw(port);
  try {
    socket.write('bad\na|NOPE|x\nb|DELAY|-1\nc|PRIME_COUNT|5000001\n');
    socket.write('x'.repeat(5000)); await waitFor(() => lines.length === 5);
    socket.write('\nz|UPPER|still alive\n'); await waitFor(() => lines.length === 6);
    assert.equal(lines.filter(line => line.includes('|ERROR|')).length, 5);
    assert.ok(lines.includes('z|OK|STILL ALIVE'));
  } finally { socket.destroy(); await server.close(); }
});
test('full bounded queue returns BUSY for the rejected request', async () => {
  const { server, port } = await fixture({ workers: 1, queueCapacity: 1 });
  try {
    const responses = await sendBatch(port, ['a|DELAY|150', 'b|DELAY|150', 'c|UPPER|reject me']);
    assert.equal(responses.find(r => r.id === 'c')?.status, 'BUSY');
    assert.ok(responses.filter(r => r.id !== 'c').every(r => r.status === 'OK'));
  } finally { await server.close(); }
});
test('five-client connection cap is enforced', async () => {
  const { server, port } = await fixture(); const connections: net.Socket[] = [];
  try {
    for (let i = 0; i < 5; i++) connections.push((await raw(port)).socket);
    await waitFor(() => server.state.clients === 5);
    const sixth = await raw(port); connections.push(sixth.socket);
    await waitFor(() => sixth.lines.length === 1);
    assert.equal(sixth.lines[0], '-|BUSY|Connection limit reached'); assert.equal(server.state.clients, 5);
  } finally { connections.forEach(s => s.destroy()); await server.close(); }
});
test('disconnect during processing does not break workers or other clients', async () => {
  const { server, port } = await fixture(); const { socket } = await raw(port);
  try {
    socket.write('gone|DELAY|100\n'); await waitFor(() => server.state.pending === 1); socket.destroy();
    await waitFor(() => server.state.pending === 0 && server.state.clients === 0);
    const [response] = await sendBatch(port, ['alive|UPPER|yes']); assert.equal(response.result, 'YES');
  } finally { socket.destroy(); await server.close(); }
});
test('graceful shutdown completes accepted running and queued jobs', async () => {
  const { server, port } = await fixture({ workers: 1 }); const { socket, lines } = await raw(port);
  try {
    socket.write('a|DELAY|50\nb|DELAY|50\n'); await waitFor(() => server.state.pending === 2);
    assert.equal(await server.close(2000), true);
    await waitFor(() => lines.length === 2); assert.ok(lines.every(line => line.includes('|OK|')));
    await assert.rejects(sendBatch(port, ['late|UPPER|x']));
  } finally { socket.destroy(); }
});
test('shutdown timeout cancels remaining work and terminates workers', async () => {
  const { server, port } = await fixture({ workers: 1 }); const { socket, lines } = await raw(port);
  try {
    socket.write('a|DELAY|10000\nb|DELAY|10000\n'); await waitFor(() => server.state.pending === 2);
    const started = Date.now(); assert.equal(await server.close(50), false); assert.ok(Date.now() - started < 1500);
    await waitFor(() => lines.length === 2); assert.ok(lines.every(line => line.includes('|ERROR|Shutdown timeout')));
  } finally { socket.destroy(); }
});
test('half-closed send side still receives accepted job responses', async () => {
  const { server, port } = await fixture(); const { socket, lines } = await raw(port);
  try {
    socket.end('a|DELAY|100\nb|UPPER|hello\n');
    await waitFor(() => lines.length === 2);
    assert.deepEqual(new Set(lines.map(line => line.split('|')[0])), new Set(['a', 'b']));
    assert.ok(lines.every(line => line.includes('|OK|')));
  } finally { socket.destroy(); await server.close(); }
});
test('duplicate pending IDs are rejected without replacing the accepted job', async () => {
  const { server, port } = await fixture();
  try {
    const responses = await sendBatch(port, ['same|DELAY|100', 'same|UPPER|invalid duplicate']);
    assert.equal(responses.filter(r => r.status === 'ERROR').length, 1);
    assert.equal(responses.filter(r => r.status === 'OK').length, 1);
  } finally { await server.close(); }
});
test('Unicode uppercase expansion is supported at the maximum request length', async () => {
  const { server, port } = await fixture();
  try {
    const text = 'ΐ'.repeat(2044); // two-byte input expands to six-byte uppercase characters
    const [response] = await sendBatch(port, [`u|UPPER|${text}`]);
    assert.equal(response.status, 'OK'); assert.equal(response.result, text.toUpperCase());
  } finally { await server.close(); }
});
