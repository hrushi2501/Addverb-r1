import { useEffect, useRef, type Dispatch } from 'react';
import type { Action, JobEvent, ServerState } from './model.js';
export function useLiveConnection(dispatch: Dispatch<Action>) {
  const socket = useRef<WebSocket | null>(null);
  useEffect(() => {
    let disposed = false;
    let retry: ReturnType<typeof setTimeout> | undefined;
    let attempts = 0;
    let connection: WebSocket;
    const connect = () => {
      if (disposed) return;
      dispatch({ type: 'connection', value: attempts ? 'reconnecting' : 'connecting' });
      connection = new WebSocket(`${location.protocol === 'https:' ? 'wss:' : 'ws:'}//${location.host}/ws`);
      socket.current = connection;
      const current = connection;
      const timeout = setTimeout(() => { if (current.readyState === WebSocket.CONNECTING) current.close(); }, 5000);
      current.onopen = () => { clearTimeout(timeout); attempts = 0; dispatch({ type: 'connection', value: 'connected' }); };
      current.onmessage = message => {
        if (disposed || socket.current !== current) return;
        try {
          const event = JSON.parse(message.data) as JobEvent & ServerState;
          if (event.type === 'snapshot') dispatch({ type: 'snapshot', server: event });
          else if (event.type === 'error' && !event.runId) dispatch({ type: 'error', message: event.message ?? 'Invalid server message' });
          else dispatch({ type: 'event', event });
        } catch { dispatch({ type: 'error', message: 'Invalid live update received' }); }
      };
      current.onerror = () => { current.close(); };
      current.onclose = () => {
        clearTimeout(timeout);
        if (disposed || socket.current !== current) return;
        socket.current = null;
        dispatch({ type: 'connection', value: 'reconnecting' });
        retry = setTimeout(connect, Math.min(1000 * 2 ** attempts++, 10000));
      };
    };
    connect();
    return () => { disposed = true; clearTimeout(retry); socket.current = null; connection?.close(); };
  }, [dispatch]);
  return (command: unknown): boolean => {
    if (socket.current?.readyState !== WebSocket.OPEN) return false;
    try { socket.current.send(JSON.stringify(command)); return true; }
    catch { return false; }
  };
}
