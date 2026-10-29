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
