import { sendBatch } from './client.js';
const lines = process.argv.slice(2);
const port = Number(process.env.TCP_PORT ?? 9000);
if (lines.length === 0) lines.push('r1|DELAY|2000', 'r2|DELAY|2000', 'r3|DELAY|2000', 'r4|DELAY|2000');
try {
  console.log(`Sending ${lines.length} requests together on ONE TCP connection`);
  await sendBatch(port, lines, 'CLI', response => console.log(`${response.id}|${response.status}|${response.result} (${response.elapsedMs}ms)`));
} catch (error) { console.error((error as Error).message); process.exitCode = 1; }
