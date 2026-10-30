package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/batch"
	"github.com/temacteklyannayapuwka/JobQueue/internal/httpapi"
	"github.com/temacteklyannayapuwka/JobQueue/internal/worker"
)

func run(ctx context.Context, input io.Reader, output io.Writer, workers int) error {
	return runWithRetry(ctx, input, output, workers, worker.RetryPolicy{MaxAttempts: 1})
}

func runWithRetry(ctx context.Context, input io.Reader, output io.Writer, workers int, policy worker.RetryPolicy) error {
	results, runErr := batch.Run(ctx, input, workers, policy)
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
	listen := flag.String("listen", "", "HTTP listen address; empty runs the tasks file once")
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	policy := worker.RetryPolicy{MaxAttempts: *attempts, Delay: *delay}
	if *listen != "" {
		if err := httpapi.Serve(ctx, *listen, *workers, policy); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		return
	}
	file, err := os.Open(*path)
	if err == nil {
		defer file.Close()
		err = runWithRetry(ctx, file, os.Stdout, *workers, policy)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
