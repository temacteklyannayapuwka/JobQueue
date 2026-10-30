package worker

import (
	"context"
	"errors"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

type RetryPolicy struct {
	MaxAttempts int
	Delay       time.Duration
}

func (p RetryPolicy) Validate() error {
	if p.MaxAttempts < 1 {
		return errors.New("max attempts must be positive")
	}
	if p.Delay < 0 {
		return errors.New("retry delay must not be negative")
	}
	return nil
}

func execute(ctx context.Context, task queue.Task, handler Handler, policy RetryPolicy) Result {
	result := Result{ID: task.ID, Status: "failed"}
	for result.Attempts < policy.MaxAttempts {
		if err := ctx.Err(); err != nil {
			result.Error = err.Error()
			return result
		}
		result.Attempts++
		output, err := handler(ctx, task)
		if err == nil {
			result.Status = "succeeded"
			result.Output = output
			result.Error = ""
			return result
		}
		result.Error = err.Error()
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) || result.Attempts == policy.MaxAttempts {
			return result
		}
		timer := time.NewTimer(policy.Delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			result.Error = ctx.Err().Error()
			return result
		case <-timer.C:
		}
	}
	return result
}
