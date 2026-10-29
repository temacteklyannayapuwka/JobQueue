package main

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
	"github.com/temacteklyannayapuwka/JobQueue/internal/worker"
)

func run(ctx context.Context, input io.Reader, output io.Writer, workers int) error {
	return runWithRetry(ctx, input, output, workers, worker.RetryPolicy{MaxAttempts: 1})
}

func runWithRetry(ctx context.Context, input io.Reader, output io.Writer, workers int, policy worker.RetryPolicy) error {
	var tasks []queue.Task
	decoder := json.NewDecoder(input)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&tasks); err != nil {
		return fmt.Errorf("decode tasks: %w", err)
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		return fmt.Errorf("expected one JSON array")
	}
	results, runErr := worker.RunWithRetry(ctx, tasks, workers, func(ctx context.Context, task queue.Task) (string, error) {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		return fmt.Sprintf("%x", sha256.Sum256([]byte(task.Payload))), nil
	}, policy)
	encoder := json.NewEncoder(output)
	for _, result := range results {
		if err := encoder.Encode(result); err != nil {
			return err
		}
	}
	if runErr != nil {
		return runErr
	}

	return nil
}

func main() {
	path := flag.String("tasks", "examples/tasks.json", "path to a JSON array of tasks")
	workers := flag.Int("workers", 2, "number of concurrent workers")
	attempts := flag.Int("attempts", 1, "maximum attempts per task, including the first")
	delay := flag.Duration("retry-delay", time.Second, "pause between failed attempts")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	file, err := os.Open(*path)
	if err == nil {
		defer file.Close()
		err = runWithRetry(ctx, file, os.Stdout, *workers, worker.RetryPolicy{MaxAttempts: *attempts, Delay: *delay})
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
