# Scalable Multithreaded TCP Socket Server

A high-performance TCP/IP socket server demonstrating **multithreading-based parallel processing** and **horizontal scalability** for incoming socket messages using Node.js `worker_threads`, paired with an interactive real-time dashboard.

---

## Problem & Solution Overview

### Problem Statement

> Develop a socket-based application (TCP/IP socket server) that processes incoming messages in parallel using multithreading, and demonstrates scalability under concurrent workloads.

### Solution Architecture

In standard single-threaded socket architectures, CPU-bound or long-running tasks block the event loop, causing latency spikes and head-of-line blocking across connections. This solution decouples **network I/O** from **message processing**:

1. **Non-Blocking I/O (Main Event Loop):** The main thread handles TCP connection handshakes (`net.createServer`), stream buffering, and newline byte framing (`LineFramer`). It never performs message work directly.
2. **True Multithreaded Processing (`worker_threads`):** A fixed pool of real OS worker threads (`WorkerPool`, default 4 workers) processes incoming messages in parallel across multiple CPU cores.
3. **Pipelining & Out-of-Order Execution:** Sockets can stream multiple requests concurrently without waiting for sequential responses. Requests are tagged with a unique `requestId`, allowing fast tasks to bypass slow ones without head-of-line blocking.
4. **Backpressure & Concurrency Control:** A bounded FIFO queue (capacity: 16 waiting jobs) absorbs bursts. When saturated, the server sheds load immediately with `BUSY` responses rather than unbounded memory growth.
5. **Thread-Safe Response Serialization:** Workers communicate via thread message passing (`postMessage`) and never touch raw sockets directly. The main thread writes atomic, serialized responses back to each originating socket, eliminating byte interleaving and race conditions.

```text
TCP Clients (Concurrent Connections / Pipelined Requests)
       │
       ▼ [TCP Stream, Port 9000]
Node.js TCP Acceptor & Line Framer (Main Thread Event Loop)
       │
       ▼ [Atomic Enqueue]
Bounded FIFO Queue (Max 16 Waiting Jobs)
       │
       ├── Worker Thread 1 ──┐
       ├── Worker Thread 2 ──┼── Parallel Execution (Real OS Threads)
       ├── Worker Thread 3 ──┤   [UPPER, DELAY, PRIME_COUNT (CPU)]
       └── Worker Thread 4 ──┘
       │
       ▼ [Thread Message Passing]
Main Thread Response Serializer ──► Socket Responses (`id|OK|result\n`)
```

---

## Demonstrating Multithreading & Scalability

The solution includes four tangible demonstrations of parallel processing and scalability:

| Demonstration | Scenario | What It Proves |
| --- | --- | --- |
| **Parallel Speedup** | 4 concurrent `DELAY\|2000` jobs on 1 worker vs. 4 workers | **~4x speedup** (8.0s sequential vs. 2.0s parallel). Proves parallel task distribution across worker threads. |
| **No Head-of-Line Blocking** | `slow\|DELAY\|3000` followed immediately by `fast\|UPPER\|hello` on one socket | Fast response arrives in **~25ms**, bypassing the active 3s task. Proves non-blocking, out-of-order execution. |
| **Multi-Client Scalability** | 3 independent TCP connections submitting batches concurrently | Workers are shared fairly across clients without starvation or cross-talk. |
| **CPU-Bound Multithreading** | Concurrent `PRIME_COUNT\|1000000` trial-division tasks | Proves true multi-core CPU parallelism, computing prime counts simultaneously without stalling the I/O event loop. |

---

## Quick Start

### Prerequisites

Node.js 22.12+ (or Node 24 LTS).

```sh
npm ci
npm run dev
```

Open [`http://127.0.0.1:3000`](http://127.0.0.1:3000) for the live dashboard, which visualizes active worker threads, queued jobs, client connections, and interactive scenario benchmarks.

### Configuration Options

```sh
PORT=3000 TCP_PORT=9000 WORKERS=4 QUEUE_CAPACITY=16 npm start
```

- `PORT`: HTTP/WebSocket dashboard port (default: `3000`).
- `TCP_PORT`: Raw TCP server port (default: `9000`).
- `WORKERS`: Worker thread pool size (default: `4`, toggleable between `1` and `4`).
- `QUEUE_CAPACITY`: Bounded waiting queue limit before returning `BUSY` (default: `16`).

---

## Headless Demos & CLI Usage

Run automated benchmarking scenarios directly in the terminal:

```sh
npm run demo
```

*Starts an isolated server, executes 1-vs-4 worker comparison, out-of-order execution, and multi-client benchmarks, prints exact timings, and shuts down cleanly.*

Send pipelined batches to a running TCP server via the built-in CLI client:

```sh
npm run client -- 'r1|DELAY|2000' 'r2|UPPER|hello' 'r3|PRIME_COUNT|1000000'
```

Or connect directly using `nc` (Netcat) or Telnet:

```sh
nc 127.0.0.1 9000
r1|UPPER|hello world
r2|PRIME_COUNT|500000
```

---

## Wire Protocol

Newline-delimited UTF-8 over TCP (`\n`):

- **Request:** `<requestId>|<operation>|<argument>\n`
- **Response:** `<requestId>|<status>|<result>\n`

### Operations

- `UPPER`: String uppercase conversion (`r1|UPPER|hello` $\to$ `r1|OK|HELLO`).
- `DELAY`: Simulated async processing with latency in ms (`r2|DELAY|2000`).
- `PRIME_COUNT`: CPU-bound prime count up to limit (`r3|PRIME_COUNT|1000000` $\to$ `r3|OK|78498`).

### Status Codes

- `OK`: Completed successfully.
- `BUSY`: Bounded queue capacity reached or connection cap exceeded.
- `ERROR`: Malformed input, unknown operation, or worker error.

---

## Request Templates

Paste these into the Web Dashboard (`http://127.0.0.1:3000`), CLI, or raw TCP socket:

### 1. 4-Worker Parallel Execution (Simultaneous Overlap)

Sends 4 concurrent 2-second jobs. In a 4-worker pool, all 4 threads activate at once and complete in **~2 seconds total** (instead of ~8 seconds):

```text
r1|DELAY|2000
r2|DELAY|2000
r3|DELAY|2000
r4|DELAY|2000
```

### 2. 4-Worker CPU Parallelism (Multi-Core Prime Count)

Executes compute-heavy prime number counts across all 4 worker threads concurrently:

```text
cpu-1|PRIME_COUNT|1000000
cpu-2|PRIME_COUNT|1000000
cpu-3|PRIME_COUNT|1000000
cpu-4|PRIME_COUNT|1000000
```

### 3. Mixed Workload & Out-of-Order Execution

Demonstrates that fast requests (like `UPPER`) complete in milliseconds and return to the client without waiting for long-running delays:

```text
job-1|DELAY|3000
job-2|PRIME_COUNT|500000
job-3|DELAY|1500
job-4|UPPER|parallel socket processing
```

### 4. Worker Pool Saturation + Queueing (8 Requests)

Fills all 4 workers with active work while testing FIFO queueing for the remaining 4 requests:

```text
w1|DELAY|2000
w2|DELAY|2000
w3|DELAY|2000
w4|DELAY|2000
q1|UPPER|queued task 1
q2|UPPER|queued task 2
q3|UPPER|queued task 3
q4|UPPER|queued task 4
```

---

## Project Structure

| File | Responsibility |
| --- | --- |
| [`server/tcp.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/tcp.ts) | TCP socket server, client connection management, and safe shutdown draining |
| [`server/pool.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/pool.ts) | Worker thread pool manager, task dispatching, bounded FIFO queue |
| [`server/worker.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/worker.ts) | Thread execution logic (CPU prime calculation, delay, string transform) |
| [`server/protocol.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/protocol.ts) | Stream byte framer (`LineFramer`), request parser, size bounds enforcement |
| [`server/client.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/client.ts) | Pipelined TCP client for batch transmission over single connections |
| [`server/scenarios.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/scenarios.ts) | Benchmarking suites (1 vs 4 workers, mixed speed, multi-client fairness) |
| [`server/main.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/main.ts) | Server entrypoint, HTTP & WebSocket live bridge for frontend monitoring |
| [`server/cli.ts`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/server/cli.ts) | Standalone command-line TCP client |
| [`web/App.tsx`](file:///Users/hrushi.2501/Desktop/Grind/Addverb/web/App.tsx) | Real-time React dashboard with live worker status cards and benchmark controls |

---

## Testing & Verification

Run the full automated test suite (24 tests covering framing, concurrency, backpressure, and shutdown):

```sh
npm test
```

Run unit tests only (listener-free):

```sh
npm run test:unit
```

Run end-to-end verification and record evidence files:

```sh
npm run verify
```
