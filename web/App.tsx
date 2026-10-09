import { useReducer } from 'react';
import { parseBatch } from '../server/request.js';
import { initialState, reducer } from './model.js';
import { useLiveConnection } from './useLiveConnection.js';
const seconds = (ms: number) => `${(ms / 1000).toFixed(2)}s`;
export function App() {
  const [state, dispatch] = useReducer(reducer, initialState);
  const send = useLiveConnection(dispatch);
  const { server, running, messages, responses, events } = state;
  const available = state.connection === 'connected' && !!server;
  const busy = running || server?.running || (server?.pending ?? 0) > 0;
  const disabled = !available || busy;
  const submit = () => {
    try {
      parseBatch(messages);
      const runId = crypto.randomUUID();
      const command = { type: 'run', runId, input: { mode: 'custom', messages,
        ...(state.workers ? { workers: Number(state.workers) } : {}) } };
      if (new TextEncoder().encode(JSON.stringify(command)).length > 32768) throw new Error('Batch exceeds the 32KB WebSocket limit');
      dispatch({ type: 'submit', runId });
      if (!send(command)) {
        dispatch({ type: 'event', event: { type: 'error', runId, message: 'WebSocket disconnected. Reconnect before sending.' } });
      }
    } catch (error) { dispatch({ type: 'error', message: (error as Error).message }); }
  };
  const cancel = () => {
    if (!send({ type: 'cancel', runId: state.runId })) dispatch({ type: 'connection', value: 'reconnecting' });
  };
  return <main>
    <header>
      <div><div className="eyebrow">SOCKETS · QUEUES · THREADS</div><h1>TCP Worker Lab</h1><p>Send your requests. Watch independent jobs share a worker pool.</p></div>
      <span className={`connection ${available ? 'connected' : ''}`} role="status"><span className="dot"/>{available ? 'WebSocket connected' : state.connection === 'reconnecting' ? 'Reconnecting…' : 'Connecting…'}</span>
    </header>
    <section className="stats" aria-label="Live server status">
      {[['Workers', server?.workers], ['Jobs waiting', server?.queued], ['Queue capacity', server?.queueCapacity], ['TCP clients', server ? `${server.clients} / ${server.maxClients}` : undefined]].map(([label, value]) => <div key={label}><span>{label}</span><strong>{value ?? '—'}</strong></div>)}
    </section>
    <section className="panel compose">
      <div className="section-title"><h2>Send requests</h2><span>One TCP connection per batch</span></div>
      <p className="hint">Enter one request per line. Your entire batch is sent without waiting for each response.</p>
      <label htmlFor="requests">requestId|operation|argument</label>
      <textarea id="requests" value={messages} onChange={event => dispatch({ type: 'messages', value: event.target.value })} spellCheck={false} disabled={running} placeholder="Enter your requests here"/>
      <div className="send-row">
        <label htmlFor="workers">Worker pool <select id="workers" value={state.workers} onChange={event => dispatch({ type: 'workers', value: event.target.value })} disabled={!!busy}>
          <option value="">Use server configuration</option><option value="1">1 worker</option><option value="4">4 workers</option>
        </select></label>
        <div className="button-row"><button disabled={running} onClick={() => dispatch({ type: 'clear' })}>Clear</button>
          {running && <button onClick={cancel}>Cancel batch</button>}
          <button className="primary" disabled={!!disabled || !messages.trim()} onClick={submit}>{running ? 'Processing…' : 'Send batch'}</button>
        </div>
      </div>
      <div className="operations"><p><code>UPPER</code><span>Text to uppercase</span></p><p><code>PRIME_COUNT</code><span>CPU work · integer 0–5,000,000</span></p><p><code>DELAY</code><span>Simulated work · integer 0–10,000ms</span></p></div>
      <p className="hint input-note">Unique request IDs · up to 40 requests · 4,096 bytes per request · 32KB per batch. Responses can arrive out of order.</p>
      {server && !running && busy && <p className="note" role="status">The server is finishing accepted work. Wait before sending another batch.</p>}
    </section>
    {state.error && <div className="error" role="alert">{state.error}</div>}
    <section className="panel activity">
      <div className="section-title"><h2>Worker activity</h2><span role="status">{running && <span className="dot blue"/>}{state.phase}</span></div>
      {server ? <div className="worker-grid">{Array.from({ length: server.workers }, (_, index) => {
        const name = `worker-${index + 1}`; const job = server.active[name];
        return <div className={`worker ${job ? 'busy' : ''}`} key={name}><div><strong>{name}</strong><span className="badge">{job ? 'Processing' : 'Idle'}</span></div><p>{job ? `${job.client} · ${job.request}` : 'Waiting for a job'}</p><small>{job?.operation ?? '—'}</small></div>;
      })}</div> : <p className="empty">Waiting for live server status.</p>}
      {state.elapsedMs !== undefined && <p className="summary">Batch elapsed time: {seconds(state.elapsedMs)}</p>}
    </section>
    <section className="panel">
      <div className="section-title"><h2>Responses</h2><span>{responses.length} received · completion order</span></div>
      <div className="table-wrap"><table><thead><tr><th>#</th><th>Client</th><th>Request</th><th>Status</th><th>Result</th><th>Arrived after</th></tr></thead><tbody>
        {responses.map((response, index) => <tr key={index}><td>{index + 1}</td><td>{response.client}</td><td><code>{response.id}</code></td><td><span className={`status ${response.status === 'OK' ? 'ok' : 'bad'}`}>{response.status}</span></td><td>{response.result}</td><td>{seconds(response.elapsedMs!)}</td></tr>)}
        {!responses.length && <tr><td colSpan={6} className="empty">Send your requests to see responses here.</td></tr>}
      </tbody></table></div>
    </section>
    <section className="panel">
      <div className="section-title"><h2>Job log</h2><span>UTC time · worker execution duration</span></div>
      <div className="logs" role="log" aria-label="Job execution log">{events.length ? events.map((event, index) => <div key={index}><time>{new Date(event.at!).toISOString().slice(11, 23)}</time><b className={event.type}>{event.type.toUpperCase().padEnd(5)}</b><span>client={event.client} request={event.request} worker={event.worker}{event.durationMs === undefined ? '' : ` duration=${event.durationMs}ms`}</span></div>) : <p className="empty">START and END events will appear here.</p>}</div>
    </section>
    <footer>Browser → WebSocket → TCP :{server?.tcpPort ?? '—'} → bounded queue → worker threads → originating client</footer>
  </main>;
}
