# JobQueue

A priority queue and worker pool for background tasks.

## Run

Requires Go 1.22 or newer. No external dependencies.

```sh
go run ./cmd/jobqueue -tasks examples/tasks.json -workers 2
```

The CLI reads a JSON array and processes it with a bounded worker pool. The built-in handler computes the SHA-256 digest of each UTF-8 payload. It prints one JSON result per dispatched task with `id`, `status`, and `output` or `error`.

Higher integer priorities are dispatched first; equal priorities preserve insertion order. Results are printed after the batch finishes, in dispatch order (`urgent-export`, `backup`, then `report` in the example). Concurrent handlers can start and finish in a different order. Use `-workers 1` for sequential execution.

A handler failure is recorded without stopping other tasks. Ctrl+C or SIGTERM stops dispatch and cancels the context passed to active handlers; the pool waits for them to return. Handlers must honor cancellation. This is a single in-memory batch with no persistence or retries.

Each task has an `id`, integer `priority` (defaults to zero), and string `payload`. Empty IDs and duplicate IDs currently in the queue are rejected.

## Queue

The in-memory queue uses a binary heap: enqueue and dequeue take O(log n), and length takes O(1). A map detects duplicate queued IDs. A dequeued ID can be reused.

Only the dispatcher accesses the queue. Worker goroutines receive tasks through an unbuffered channel and write to separate result slots. Data exists only for the current process.

## Checks

```sh
go test -race ./...
go vet ./...
go build ./...
```
