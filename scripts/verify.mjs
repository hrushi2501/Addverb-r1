import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
mkdirSync('evidence', { recursive: true });
let failed = false;
for (const [name, args] of [['build', ['run', 'build']], ['tests', ['test']], ['demos', ['run', 'demo']]]) {
  const result = spawnSync('npm', args, { encoding: 'utf8', timeout: 90000 });
  const output = (result.stdout ?? '') + (result.stderr ?? '') + (result.error ? String(result.error) : '');
  writeFileSync(`evidence/${name}.txt`, output);
  console.log(`${name}: ${result.status === 0 ? 'PASS' : 'FAIL'} — evidence/${name}.txt`);
  if (result.status !== 0) failed = true;
}
process.exitCode = failed ? 1 : 0;
