export const MAX_MESSAGE_BYTES = 4096;
export type Request = { id: string; operation: 'UPPER' | 'PRIME_COUNT' | 'DELAY'; argument: string };
export type Response = { id: string; status: string; result: string };
export function parseRequest(line: string): Request {
  const [id, operation, argument, ...extra] = line.split('|');
  if (!id || !/^[\w-]{1,64}$/.test(id) || argument === undefined || extra.length || /[\r\n]/.test(line))
    throw new Error('Expected requestId|operation|argument');
  if (operation !== 'UPPER' && operation !== 'PRIME_COUNT' && operation !== 'DELAY')
    throw new Error('Unknown operation');
  if (operation !== 'UPPER') {
    const max = operation === 'DELAY' ? 10000 : 5000000;
    if (!/^\d+$/.test(argument) || !Number.isSafeInteger(Number(argument)) || Number(argument) > max)
      throw new Error(`${operation} requires an integer from 0 to ${max}`);
  }
  return { id, operation, argument };
}

export function parseBatch(messages: string): string[] {
  const lines = messages.replace(/\r\n/g, '\n').split('\n');
  if (lines.at(-1) === '') lines.pop();
  if (!messages.trim() || lines.length > 40) throw new Error('Enter 1 to 40 requests');
  const ids = new Set<string>();
  for (const line of lines) {
    if (new TextEncoder().encode(line).length > MAX_MESSAGE_BYTES) throw new Error('Each message must be at most 4096 bytes');
    const request = parseRequest(line);
    if (ids.has(request.id)) throw new Error('Use a unique ID for each request');
    ids.add(request.id);
  }
  return lines;
}
