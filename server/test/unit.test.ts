import test from 'node:test';
import assert from 'node:assert/strict';
import { LineFramer, parseRequest } from '../protocol.js';
import { WorkerPool, type JobEvent } from '../pool.js';
import { validateRun } from '../scenarios.js';
import { setTimeout as delay } from 'node:timers/promises';
test('framer reconstructs split UTF-8 and multiple messages in one chunk', () => {
  const lines: string[] = []; const frame = new LineFramer(line => lines.push(line), () => assert.fail());
  const bytes = Buffer.from('a|UPPER|héllo\nb|UPPER|two\n'); const index = bytes.indexOf(0xc3) + 1;
  frame.push(bytes.subarray(0, index)); assert.equal(lines.length, 0);
  frame.push(bytes.subarray(index)); assert.deepEqual(lines, ['a|UPPER|héllo', 'b|UPPER|two']);
});
test('framer bounds messages, discards oversized remainder, then recovers', () => {
  const lines: string[] = []; let errors = 0;
  const frame = new LineFramer(line => lines.push(line), () => errors++);
  frame.push(Buffer.from('x'.repeat(4096))); assert.equal(errors, 0);
  frame.push(Buffer.from('y')); frame.push(Buffer.from('z'.repeat(10000))); assert.equal(errors, 1);
  frame.push(Buffer.from('\nok|UPPER|yes\n')); assert.deepEqual(lines, ['ok|UPPER|yes']);
});
test('protocol accepts limits and rejects malformed arguments and operations', () => {
  assert.equal(parseRequest('p|PRIME_COUNT|5000000').argument, '5000000');
  assert.equal(parseRequest('d|DELAY|10000').operation, 'DELAY');
  for (const line of ['bad', '|UPPER|a', 'a|NOPE|b', 'a|DELAY|-1', 'a|DELAY|10001', 'a|PRIME_COUNT|5000001', 'a|UPPER|b|c', 'a|UPPER|x\r']) assert.throws(() => parseRequest(line));
});
test('bridge validation rejects duplicate IDs and oversized batches', () => {
  assert.throws(() => validateRun({ mode: 'custom', messages: 'x|UPPER|a\nx|UPPER|b' }));
  assert.throws(() => validateRun({ mode: 'custom', messages: Array(41).fill('a|UPPER|x').join('\n') }));
  assert.throws(() => validateRun({ mode: 'custom', messages: 'a|UPPER|' + 'x'.repeat(4096) }));
  assert.throws(() => validateRun({ mode: 'custom', workers: 2, messages: 'a|UPPER|x' }));
});
test('four worker threads start same-client jobs before any finishes', async () => {
  const events: JobEvent[] = []; const pool = new WorkerPool(4, 2, event => events.push(event));
  try {
    await Promise.all(Array.from({ length: 4 }, (_, i) => new Promise<void>(resolve => {
      assert.ok(pool.submit({ client: 'C1', request: parseRequest(`r${i}|DELAY|150`), reply: status => { assert.equal(status, 'OK'); resolve(); } }));
    })));
    const starts = events.filter(event => event.type === 'start'); const ends = events.filter(event => event.type === 'end');
    assert.equal(new Set(starts.map(e => e.worker)).size, 4);
    assert.ok(Math.max(...starts.map(e => e.at)) < Math.min(...ends.map(e => e.at)));
  } finally { await pool.close(2000); }
});
test('CPU work executes on separate threads with correct prime counts', async () => {
  const events: JobEvent[] = []; const pool = new WorkerPool(4, 2, event => events.push(event));
  try {
    const results = await Promise.all(Array.from({ length: 4 }, (_, i) => new Promise<string>(resolve => {
      pool.submit({ client: 'C1', request: parseRequest(`r${i}|PRIME_COUNT|1000000`), reply: (_, result) => resolve(result) });
    })));
    assert.deepEqual(results, Array(4).fill('78498'));
    const starts = events.filter(e => e.type === 'start'); const ends = events.filter(e => e.type === 'end');
    assert.equal(new Set(starts.map(e => e.worker)).size, 4);
    assert.ok(Math.max(...starts.map(e => e.at)) < Math.min(...ends.map(e => e.at)));
  } finally { await pool.close(2000); }
});
test('bounded queue rejects excess work and drains accepted work at shutdown', async () => {
  const pool = new WorkerPool(1, 1, () => {}); const results: string[] = [];
  const job = (id: string) => ({ client: 'C1', request: parseRequest(`${id}|DELAY|40`), reply: (status: string) => results.push(status) });
  assert.equal(pool.submit(job('a')), true); assert.equal(pool.submit(job('b')), true); assert.equal(pool.submit(job('c')), false);
  assert.equal(pool.queued, 1); assert.equal(await pool.close(2000), true); assert.deepEqual(results, ['OK', 'OK']);
  assert.equal(pool.submit(job('d')), false);
});
test('shutdown timeout cancels queued and active jobs without waiting for delay', async () => {
  const events: JobEvent[] = [];
  const pool = new WorkerPool(1, 1, event => events.push(event)); const results: string[] = [];
  for (const id of ['a', 'b']) pool.submit({ client: 'C1', request: parseRequest(`${id}|DELAY|10000`), reply: (status, result) => results.push(`${status}:${result}`) });
  const started = Date.now(); assert.equal(await pool.close(50), false);
  assert.ok(Date.now() - started < 1500); assert.deepEqual(results, Array(2).fill('ERROR:Shutdown timeout'));
  const end = events.find(event => event.type === 'end');
  assert.equal(end?.request, 'a'); assert.equal(end?.status, 'ERROR');
  assert.equal(end?.worker, 'worker-1'); assert.equal(typeof end?.durationMs, 'number');
});
test('fast work can finish before a slow job from the same client', async () => {
  const pool = new WorkerPool(4, 2, () => {}); const order: string[] = [];
  try {
    await Promise.all(['slow|DELAY|150', 'fast|UPPER|hello'].map(line => new Promise<void>(resolve => {
      const request = parseRequest(line); pool.submit({ client: 'C1', request, reply: () => { order.push(request.id); resolve(); } });
    })));
    assert.deepEqual(order, ['fast', 'slow']);
  } finally { await pool.close(2000); }
});
