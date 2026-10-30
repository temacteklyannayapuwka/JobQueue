package batch

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
	"github.com/temacteklyannayapuwka/JobQueue/internal/worker"
)

func Run(ctx context.Context, input io.Reader, workers int, policy worker.RetryPolicy) ([]worker.Result, error) {
	var tasks []queue.Task
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&tasks); err != nil {
		return nil, fmt.Errorf("decode tasks: %w", err)
	}
	if tasks == nil {
		return nil, fmt.Errorf("expected a JSON array")
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err != nil {
			return nil, fmt.Errorf("read after task array: %w", err)
		}
		return nil, fmt.Errorf("expected one JSON array")
	}
	return worker.RunWithRetry(ctx, tasks, workers, func(ctx context.Context, task queue.Task) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", sha256.Sum256([]byte(task.Payload))), nil
	}, policy)
}
