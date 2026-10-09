import { TcpJobServer } from './tcp.js';
import { runScenario } from './scenarios.js';
const server = new TcpJobServer();
try {
  const port = await server.listen(0);
  for (const mode of ['compare', 'mixed', 'clients'] as const) {
    console.log(`\nDEMO ${mode}`);
    await runScenario(server, port, { mode }, event => console.log(JSON.stringify(event)));
  }
} finally { await server.close(); }
