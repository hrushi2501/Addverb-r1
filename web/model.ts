export type JobEvent = { type: string; worker?: string; client?: string; request?: string; operation?: string;
  at?: number; durationMs?: number; status?: string; result?: string; id?: string; elapsedMs?: number;
  label?: string; workers?: number; message?: string; runId?: string };
export type ServerState = { workers: number; queueCapacity: number; queued: number; pending: number;
  clients: number; maxClients: number; tcpPort: number; running: boolean; active: Record<string, JobEvent> };
export type ViewState = {
  connection: 'connecting' | 'connected' | 'reconnecting'; server?: ServerState;
  messages: string; workers: string; running: boolean; runId?: string; error: string;
  phase: string; events: JobEvent[]; responses: JobEvent[]; elapsedMs?: number;
};
export const initialState: ViewState = { connection: 'connecting', messages: '', workers: '', running: false,
  error: '', phase: 'Ready for requests', events: [], responses: [] };
export type Action = { type: 'messages'; value: string } | { type: 'workers'; value: string }
  | { type: 'connection'; value: ViewState['connection'] } | { type: 'snapshot'; server: ServerState }
  | { type: 'submit'; runId: string } | { type: 'event'; event: JobEvent }
  | { type: 'error'; message: string } | { type: 'clear' };
export function reducer(state: ViewState, action: Action): ViewState {
  if (action.type === 'messages') return { ...state, messages: action.value };
  if (action.type === 'workers') return { ...state, workers: action.value };
  if (action.type === 'snapshot') return { ...state, server: action.server };
  if (action.type === 'connection') {
    if (action.value === 'connected') return { ...state, connection: action.value };
    return { ...state, connection: action.value, server: undefined, running: false, runId: undefined,
      phase: state.running ? 'Connection interrupted' : state.phase,
      error: state.running ? 'Connection lost. Accepted jobs may still finish. Requests were not resent.' : state.error };
  }
  if (action.type === 'submit') return { ...state, running: true, runId: action.runId, error: '',
    phase: 'Submitting batch', events: [], responses: [], elapsedMs: undefined };
  if (action.type === 'error') return { ...state, error: action.message };
  if (action.type === 'clear') return state.running ? state : { ...state, messages: '', error: '',
    events: [], responses: [], elapsedMs: undefined, runId: undefined, phase: 'Ready for requests' };
  const event = action.event;
  // Old connections and other clients cannot append results to this batch.
  if (!state.runId || event.runId !== state.runId) return state;
  if (event.type === 'accepted') return { ...state, phase: 'Batch accepted' };
  if (event.type === 'phase') return { ...state, phase: 'Processing batch' };
  if (event.type === 'start' || event.type === 'end') return { ...state, events: [...state.events, event] };
  if (event.type === 'response') return { ...state, responses: [...state.responses, event] };
  if (event.type === 'summary') return { ...state, elapsedMs: event.elapsedMs };
  if (event.type === 'done') return { ...state, running: false, phase: 'Batch complete' };
  if (event.type === 'error' || event.type === 'cancelled') return { ...state, running: false,
    phase: event.type === 'cancelled' ? 'Batch cancelled' : 'Batch stopped', error: event.message ?? 'Batch failed' };
  return state;
}
