package worker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

func TestPoolConcurrencyAndResults(t *testing.T) {
	tasks := make([]queue.Task, 20)
	for i := range tasks {
		tasks[i] = queue.Task{ID: fmt.Sprint(i), Priority: i / 2}
	}
	started := make(chan struct{}, 20)
	release := make(chan struct{})
	var active, peak atomic.Int32
	done := make(chan struct{})
	var results []Result
	var runErr error
	go func() {
		defer close(done)
		results, runErr = Run(context.Background(), tasks, 3, func(_ context.Context, task queue.Task) (string, error) {
			n := active.Add(1)
			defer active.Add(-1)
			for old := peak.Load(); n > old; old = peak.Load() {
				if peak.CompareAndSwap(old, n) {
					break
				}
			}
			started <- struct{}{}
			<-release
			if task.ID == "19" {
				return "", errors.New("test failure")
			}
			return task.ID, nil
		})
	}()
	defer func() {
		select {
		case <-done:
		default:
			close(release)
			<-done
		}
	}()
	for i := 0; i < 3; i++ {
		select {
		case <-started:
		case <-time.After(3 * time.Second):
			t.Fatal("workers did not start concurrently")
		}
	}
	if active.Load() != 3 {
		t.Fatalf("active=%d", active.Load())
	}
	close(release)
	<-done
	if runErr != nil || len(results) != 20 {
		t.Fatalf("results=%d, err=%v", len(results), runErr)
	}
	if peak.Load() != 3 {
		t.Fatalf("peak=%d", peak.Load())
	}
	for i, r := range results {
		want := fmt.Sprint(18 - (i/2)*2 + i%2)
		if r.ID != want {
			t.Fatalf("result %d: got %s, want %s", i, r.ID, want)
		}
		if r.ID == "19" {
			if r.Status != "failed" || r.Error != "test failure" {
				t.Fatal(r)
			}
		} else if r.Status != "succeeded" || r.Output != r.ID {
			t.Fatal(r)
		}
	}
}

func TestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	results, err := Run(ctx, []queue.Task{{ID: "a", Priority: 2}, {ID: "b"}, {ID: "c"}}, 1, func(ctx context.Context, _ queue.Task) (string, error) {
		calls++
		cancel()
		return "", ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || calls != 1 || len(results) < 1 || len(results) > 2 || results[0].Status != "failed" {
		t.Fatalf("calls=%d results=%+v err=%v", calls, results, err)
	}
}

func TestValidationAndEmptyBatch(t *testing.T) {
	handler := func(context.Context, queue.Task) (string, error) { t.Fatal("unexpected execution"); return "", nil }
	for _, n := range []int{0, -1} {
		if _, err := Run(context.Background(), nil, n, handler); err == nil {
			t.Fatal("invalid worker count accepted")
		}
	}
	if _, err := Run(context.Background(), nil, 1, nil); err == nil {
		t.Fatal("nil handler accepted")
	}
	if _, err := Run(context.Background(), []queue.Task{{ID: "x"}, {ID: "x"}}, 1, handler); err == nil {
		t.Fatal("duplicate accepted")
	}
	if results, err := Run(context.Background(), nil, 100, handler); err != nil || len(results) != 0 {
		t.Fatalf("%v %v", results, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if results, err := Run(ctx, []queue.Task{{ID: "x"}}, 2, handler); !errors.Is(err, context.Canceled) || len(results) != 0 {
		t.Fatalf("%v %v", results, err)
	}
}
