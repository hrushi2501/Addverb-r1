import test from 'node:test';
import assert from 'node:assert/strict';
import net from 'node:net';
import { spawn } from 'node:child_process';
import { once } from 'node:events';
import { setTimeout as delay } from 'node:timers/promises';
async function freePort() {
  const server = net.createServer();
  await new Promise<void>((resolve, reject) => { server.once('error', reject); server.listen(0, '127.0.0.1', resolve); });
  const port = (server.address() as net.AddressInfo).port;
  await new Promise<void>(resolve => server.close(() => resolve())); return port;
}
test('HTTP bridge serves UI, streams real TCP results, and rejects overlapping runs', async () => {
  const webPort = await freePort(); let tcpPort = await freePort();
  while (tcpPort === webPort) tcpPort = await freePort();
  const child = spawn(process.execPath, [new URL('../main.js', import.meta.url).pathname],
    { env: { ...process.env, PORT: String(webPort), TCP_PORT: String(tcpPort) }, stdio: ['ignore', 'pipe', 'pipe'] });
  let output = ''; child.stdout.on('data', chunk => output += chunk); child.stderr.on('data', chunk => output += chunk);
  const base = `http://127.0.0.1:${webPort}`;
  try {
    const deadline = Date.now() + 5000;
    while (!output.includes('Frontend:')) {
      if (child.exitCode !== null || Date.now() > deadline) throw new Error(`HTTP startup failed: ${output}`);
      await delay(20);
    }
    const page = await fetch(base); assert.equal(page.status, 200);
    const html = await page.text(); assert.match(html, /TCP Worker Lab/);
    for (const asset of html.matchAll(/(?:src|href)="(\/assets\/[^"]+)"/g)) assert.equal((await fetch(base + asset[1])).status, 200);
    assert.equal((await fetch(base + '/api/status')).status, 200);
    const post = (body: unknown) => fetch(base + '/api/run', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) });
    assert.equal((await post({ mode: 'custom', messages: 'bad' })).status, 400);
    const run = await post({ mode: 'custom', messages: 'slow|DELAY|500\nfast|UPPER|hello', workers: 4 });
    assert.equal(run.status, 200);
    assert.equal((await post({ mode: 'mixed' })).status, 409);
    const events = (await run.text()).trim().split('\n').map(line => JSON.parse(line));
    assert.deepEqual(events.filter(e => e.type === 'response').map(e => e.id), ['fast', 'slow']);
    assert.ok(events.some(e => e.type === 'start')); assert.equal(events.at(-1).type, 'done');
    const multiple = await post({ mode: 'clients' });
    const multiEvents = (await multiple.text()).trim().split('\n').map(line => JSON.parse(line));
    assert.equal(multiEvents.filter(e => e.type === 'response').length, 9);
    assert.equal(new Set(multiEvents.filter(e => e.type === 'response').map(e => e.client)).size, 3);
    assert.equal(multiEvents.at(-1).type, 'done');
  } finally {
    if (child.exitCode === null) {
      const exited = once(child, 'exit'); child.kill('SIGTERM');
      const timer = setTimeout(() => child.kill('SIGKILL'), 7000);
      await exited; clearTimeout(timer);
    }
  }
});
