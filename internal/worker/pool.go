package worker

import (
	"context"
	"errors"
	"sync"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

type Handler func(context.Context, queue.Task) (string, error)

type Result struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	Output string `json:"output,omitempty"`
	Error  string `json:"error,omitempty"`
}

type job struct {
	index int
	task  queue.Task
}

func Run(ctx context.Context, tasks []queue.Task, workers int, handler Handler) ([]Result, error) {
	if workers < 1 {
		return nil, errors.New("workers must be positive")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	var q queue.Queue
	for _, task := range tasks {
		if err := q.Enqueue(task); err != nil {
			return nil, err
		}
	}
	results := make([]Result, len(tasks))
	if workers > len(tasks) {
		workers = len(tasks)
	}
	jobs := make(chan job)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				result := Result{ID: j.task.ID, Status: "succeeded"}
				var output string
				err := ctx.Err()
				if err == nil {
					output, err = handler(ctx, j.task)
				}
				if err != nil {
					result.Status = "failed"
					result.Error = err.Error()
				} else {
					result.Output = output
				}
				results[j.index] = result
			}
		}()
	}
	dispatched := 0
dispatch:
	for task, ok := q.Dequeue(); ok; task, ok = q.Dequeue() {
		if ctx.Err() != nil {
			break
		}
		select {
		case <-ctx.Done():
			break dispatch
		case jobs <- job{index: dispatched, task: task}:
			dispatched++
		}
	}
	close(jobs)
	wg.Wait()
	return results[:dispatched], ctx.Err()
}
