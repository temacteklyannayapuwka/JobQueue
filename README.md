# JobQueue

A priority queue for background tasks.

## Run

Requires Go 1.22 or newer. No external dependencies.

```sh
go run ./cmd/jobqueue -tasks examples/tasks.json
```

The CLI reads a JSON array and prints tasks as JSON lines in dequeue order. It does not execute the payload. Higher integer priorities come first; equal priorities preserve insertion order. The example prints `urgent-export`, `backup`, then `report`.

Each task has an `id`, integer `priority` (defaults to zero), and string `payload`. Empty IDs and duplicate IDs currently in the queue are rejected.

## Queue

The in-memory queue uses a binary heap: enqueue and dequeue take O(log n), and length takes O(1). A map detects duplicate queued IDs. A dequeued ID can be reused.

The queue is intended for use by a single goroutine. Data exists only for the current process.

## Checks

```sh
go test -race ./...
go vet ./...
go build ./...
```
