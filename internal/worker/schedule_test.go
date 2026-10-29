package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

func TestDelayedTaskDoesNotBlockReadyTask(t *testing.T) {
	due := time.Now().Add(30 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var order []string
	results, err := Run(ctx, []queue.Task{{ID: "delayed", Priority: 100, RunAt: due}, {ID: "ready"}}, 1, func(_ context.Context, task queue.Task) (string, error) {
		if task.ID == "delayed" && time.Now().Before(due) {
			t.Error("started before run_at")
		}
		order = append(order, task.ID)
		return task.ID, nil
	})
	if err != nil || len(results) != 2 || len(order) != 2 {
		t.Fatalf("results=%+v order=%v err=%v", results, order, err)
	}
	if order[0] != "ready" || order[1] != "delayed" {
		t.Fatal(order)
	}
}

func TestCancelWhileWaitingForScheduledTask(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	done := make(chan error, 1)
	go func() {
		results, err := Run(ctx, []queue.Task{{ID: "future", RunAt: time.Now().Add(time.Hour)}}, 1, func(context.Context, queue.Task) (string, error) {
			t.Error("future task executed")
			return "", nil
		})
		if len(results) != 0 {
			t.Errorf("unexpected results: %+v", results)
		}
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("cancellation did not interrupt schedule")
	}
}
