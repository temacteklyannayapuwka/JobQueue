package worker

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

type Handler func(context.Context, queue.Task) (string, error)

type Result struct {
	Attempts int    `json:"attempts"`
	ID       string `json:"id"`
	Status   string `json:"status"`
	Output   string `json:"output,omitempty"`
	Error    string `json:"error,omitempty"`
}

type job struct {
	index int
	task  queue.Task
}

func Run(ctx context.Context, tasks []queue.Task, workers int, handler Handler) ([]Result, error) {
	return RunWithRetry(ctx, tasks, workers, handler, RetryPolicy{MaxAttempts: 1})
}

func RunWithRetry(ctx context.Context, tasks []queue.Task, workers int, handler Handler, policy RetryPolicy) ([]Result, error) {
	if err := policy.validate(); err != nil {
		return nil, err
	}
	if workers < 1 {
		return nil, errors.New("workers must be positive")
	}
	if handler == nil {
		return nil, errors.New("handler is required")
	}
	schedule, err := queue.NewSchedule(tasks, time.Now())
	if err != nil {
		return nil, err
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
				result := execute(ctx, j.task, handler, policy)
				results[j.index] = result
			}
		}()
	}
	dispatched := 0
dispatch:
	for schedule.Len() > 0 {
		if ctx.Err() != nil {
			break
		}
		task, ready := schedule.Peek(time.Now())
		var send chan job
		if ready {
			send = jobs
		}
		var timer *time.Timer
		var wake <-chan time.Time
		if due, ok := schedule.NextRunAt(); ok {
			timer = time.NewTimer(time.Until(due))
			wake = timer.C
		}
		select {
		case <-ctx.Done():
			if timer != nil {
				timer.Stop()
			}
			break dispatch
		case <-wake:
		case send <- job{index: dispatched, task: task}:
			schedule.Take()
			dispatched++
		}
		if timer != nil {
			timer.Stop()
		}
	}

	close(jobs)
	wg.Wait()
	return results[:dispatched], ctx.Err()
}
