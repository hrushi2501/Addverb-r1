import test from 'node:test';
import assert from 'node:assert/strict';
import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { App } from './App.js';
import { initialState, reducer, type ServerState } from './model.js';
import { parseBatch } from '../server/request.js';
const server: ServerState = { workers: 4, queueCapacity: 16, queued: 0, pending: 0, clients: 0, maxClients: 5,
  tcpPort: 9000, running: false, active: {} };
test('initial frontend has empty requests, server configuration, and no demo presets', () => {
  assert.equal(initialState.messages, ''); assert.equal(initialState.workers, '');
  const html = renderToStaticMarkup(createElement(App));
  assert.match(html, /<textarea[^>]*><\/textarea>/); assert.match(html, /Use server configuration/);
  assert.doesNotMatch(html, /Try a demonstration|One worker vs|Slow job \+|Three clients|r1\|DELAY/);
  assert.match(html, /WebSocket/); assert.match(html, /disabled=""[^>]*>Send batch/);
});
test('shared browser/server validation preserves arguments and accepts CRLF batches', () => {
  assert.deepEqual(parseBatch('a|UPPER|  keep spaces  \r\nb|DELAY|0\r\n'), ['a|UPPER|  keep spaces  ', 'b|DELAY|0']);
  assert.throws(() => parseBatch('a|UPPER|x\na|UPPER|y'));
  assert.throws(() => parseBatch('a|UPPER|x\n\nb|UPPER|y'));
});
test('live snapshot supplies authoritative workers, queue, and active jobs', () => {
  const active = { 'worker-2': { type: 'start', request: 'job', client: 'C2', operation: 'DELAY' } };
  const next = reducer(initialState, { type: 'snapshot', server: { ...server, workers: 1, queued: 3, active } });
  assert.equal(next.server?.workers, 1); assert.equal(next.server?.queued, 3); assert.equal(next.server?.active['worker-2'].request, 'job');
  assert.equal(next.workers, '');
});
test('responses preserve arrival order and other run IDs cannot mix into a batch', () => {
  let state = reducer(initialState, { type: 'submit', runId: 'current' });
  state = reducer(state, { type: 'event', event: { type: 'response', runId: 'other', id: 'wrong' } });
  assert.equal(state.responses.length, 0);
  for (const id of ['fast', 'slow']) state = reducer(state, { type: 'event', event: { type: 'response', runId: 'current', id } });
  assert.deepEqual(state.responses.map(r => r.id), ['fast', 'slow']);
  state = reducer(state, { type: 'event', event: { type: 'summary', runId: 'current', elapsedMs: 1234 } });
  state = reducer(state, { type: 'event', event: { type: 'done', runId: 'current' } });
  assert.equal(state.running, false); assert.equal(state.elapsedMs, 1234); assert.equal(state.phase, 'Batch complete');
});
test('disconnect releases UI controls, preserves input/results, and does not replay work', () => {
  let state = reducer({ ...initialState, messages: 'a|UPPER|keep this', server }, { type: 'submit', runId: 'old' });
  state = reducer(state, { type: 'event', event: { type: 'response', runId: 'old', id: 'a' } });
  state = reducer(state, { type: 'connection', value: 'reconnecting' });
  assert.equal(state.running, false); assert.equal(state.server, undefined); assert.equal(state.runId, undefined);
  assert.equal(state.messages, 'a|UPPER|keep this'); assert.equal(state.responses.length, 1); assert.match(state.error, /not resent/);
  state = reducer(state, { type: 'event', event: { type: 'response', runId: 'old', id: 'late' } });
  assert.equal(state.responses.length, 1);
  state = reducer(state, { type: 'connection', value: 'connected' }); assert.equal(state.connection, 'connected');
});
test('cancel and failed runs release pending state and keep error visible', () => {
  for (const type of ['cancelled', 'error']) {
    let state = reducer(initialState, { type: 'submit', runId: 'run' });
    state = reducer(state, { type: 'event', event: { type, runId: 'run', message: 'explanation' } });
    assert.equal(state.running, false); assert.equal(state.error, 'explanation');
  }
});
