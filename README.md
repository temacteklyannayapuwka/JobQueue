# JobQueue

A priority queue and worker pool for background tasks.

## Run

Requires Go 1.22 or newer. No external dependencies.

```sh
go run ./cmd/jobqueue -tasks examples/tasks.json -workers 2
```

The CLI reads a JSON array and processes it with a bounded worker pool. The built-in handler computes the SHA-256 digest of each UTF-8 payload. It prints one JSON result per dispatched task with `id`, `status`, and `output` or `error`.

Higher integer priorities are dispatched first; equal priorities preserve insertion order. Results are printed after the batch finishes, in dispatch order (`urgent-export`, `backup`, then `report` in the example). Concurrent handlers can start and finish in a different order. Use `-workers 1` for sequential execution.

A handler failure is recorded without stopping other tasks. Ctrl+C or SIGTERM stops dispatch and cancels the context passed to active handlers; the pool waits for them to return. Handlers must honor cancellation. This is a single in-memory batch with no persistence.

Each task has an `id`, integer `priority` (defaults to zero), and string `payload`. Empty IDs and duplicate IDs currently in the queue are rejected.

## HTTP mode

```sh
go run ./cmd/jobqueue -listen 127.0.0.1:8080 -workers 3 -attempts 3 -retry-delay 1s
curl http://127.0.0.1:8080/health
curl -X POST http://127.0.0.1:8080/run \
  -H 'Content-Type: application/json' \
  --data-binary @examples/tasks.json
```

`GET /health` returns `{"status":"ok"}`. `POST /run` accepts the same JSON array as the CLI and waits for execution to finish, returning `{"results":[...]}` in dispatch order. The task file is not loaded in HTTP mode. Task IDs must be unique within a batch; they can be reused in later requests.

Only one batch runs at a time; overlapping `/run` requests receive `429`. Health checks remain available while a batch is running. Requests are limited to 1 MiB, and batch execution has a 30-second deadline. Invalid input returns `400`, oversized input `413`, and execution timeout `504`. A timeout discards partial results and cancels remaining work. Client disconnects and server shutdown also cancel execution, including scheduled waits and retry pauses. Ctrl+C or SIGTERM shuts down the server.

This endpoint executes a batch synchronously. There is no background job ID or retained status. Keep the server bound to localhost for local use; authentication is not implemented.

## Scheduled tasks

An optional `run_at` field sets the earliest dispatch time in RFC 3339 format:

```json
[
  {"id": "later", "priority": 10, "payload": "scheduled text", "run_at": "2030-01-01T12:00:00Z"},
  {"id": "now", "priority": 1, "payload": "ready text"}
]
```

Omit `run_at` to run immediately. Past timestamps are ready immediately. Future tasks wait in a separate min-heap ordered by time, while ready tasks use the priority heap. Waiting for a scheduled task does not occupy a worker. The dispatcher wakes at the next due time without polling; Ctrl+C interrupts the wait.

Priority applies among ready tasks. Equal priorities use original input order, including tasks promoted from the delayed heap. Running tasks are not preempted. Results still print after the entire batch finishes, so a far-future task keeps the CLI running until its time arrives or the process is canceled. Retry delays continue to occupy their worker.

## Retries

```sh
go run ./cmd/jobqueue -workers 3 -attempts 3 -retry-delay 1s
```

`-attempts` includes the initial attempt and defaults to 1 (no retries). `-retry-delay` is a fixed pause after an error; negative delays and nonpositive attempt counts are rejected. Results include the number of handler invocations in `attempts`. Ordinary errors are retried up to the limit; context cancellation and deadline errors are not retried. Failed attempts do not publish partial output.

A worker stays occupied during its retry pause. Cancellation interrupts that pause. The built-in SHA-256 handler normally succeeds on the first attempt; retry scenarios are covered with failing handlers in tests. Handlers with external side effects must tolerate repeated execution, since a failed attempt may already have performed an action.

## Queue

The in-memory queue uses a binary heap: enqueue and dequeue take O(log n), and length takes O(1). A map detects duplicate queued IDs. A dequeued ID can be reused.

Only the dispatcher accesses the queue. Worker goroutines receive tasks through an unbuffered channel and write to separate result slots. Data exists only for the current process.

## Checks

```sh
go test -race ./...
go vet ./...
go build ./...
```
