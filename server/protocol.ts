import { MAX_MESSAGE_BYTES } from './request.js';
export { MAX_MESSAGE_BYTES, parseRequest } from './request.js';
export type { Request, Response } from './request.js';
export function responseId(line: string): string {
  const id = line.split('|', 1)[0];
  return /^[\w-]{1,64}$/.test(id) ? id : '-';
}
// TCP reads can contain partial lines or many lines. Bound the bytes of each frame.
export class LineFramer {
  private parts: Buffer[] = [];
  private length = 0;
  private oversized = false;
  constructor(private onLine: (line: string) => void, private onTooLong: () => void, private maxBytes = MAX_MESSAGE_BYTES) {}
  push(chunk: Buffer): void {
    let offset = 0;
    while (offset < chunk.length) {
      const newline = chunk.indexOf(10, offset);
      const end = newline < 0 ? chunk.length : newline;
      const part = chunk.subarray(offset, end);
      if (!this.oversized) {
        this.length += part.length;
        if (this.length > this.maxBytes) {
          this.parts = []; this.oversized = true;
          this.onTooLong();
        } else if (part.length) this.parts.push(part);
      }
      if (newline >= 0) {
        if (!this.oversized) this.onLine(Buffer.concat(this.parts).toString('utf8'));
        this.parts = []; this.length = 0; this.oversized = false;
      }
      offset = end + 1;
    }
  }
}
