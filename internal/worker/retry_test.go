package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

func TestRetries(t *testing.T) {
	for _, tc := range []struct {
		name                 string
		failures, max, calls int
		status               string
	}{
		{"first attempt", 0, 3, 1, "succeeded"},
		{"eventual success", 2, 3, 3, "succeeded"},
		{"exhausted", 10, 3, 3, "failed"},
		{"default single attempt", 1, 1, 1, "failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			results, err := RunWithRetry(context.Background(), []queue.Task{{ID: "x"}}, 1, func(context.Context, queue.Task) (string, error) {
				calls++
				if calls <= tc.failures {
					return "partial", errors.New("temporary failure")
				}
				return "done", nil
			}, RetryPolicy{MaxAttempts: tc.max})
			if err != nil || len(results) != 1 {
				t.Fatalf("%+v %v", results, err)
			}
			r := results[0]
			if calls != tc.calls || r.Attempts != tc.calls || r.Status != tc.status {
				t.Fatalf("calls=%d result=%+v", calls, r)
			}
			if tc.status == "succeeded" && (r.Output != "done" || r.Error != "") {
				t.Fatal(r)
			}
			if tc.status == "failed" && (r.Output != "" || r.Error != "temporary failure") {
				t.Fatal(r)
			}
		})
	}
}

func TestRetryCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	done := make(chan struct{})
	var results []Result
	var runErr error
	calls := 0
	go func() {
		defer close(done)
		results, runErr = RunWithRetry(ctx, []queue.Task{{ID: "x"}}, 1, func(context.Context, queue.Task) (string, error) {
			calls++
			close(started)
			return "", errors.New("temporary failure")
		}, RetryPolicy{MaxAttempts: 3, Delay: time.Hour})
	}()
	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("handler did not start")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("retry wait ignored cancellation")
	}
	if !errors.Is(runErr, context.Canceled) || calls != 1 || len(results) != 1 || results[0].Attempts != 1 {
		t.Fatalf("calls=%d results=%+v err=%v", calls, results, runErr)
	}
}

func TestContextErrorsAreNotRetried(t *testing.T) {
	for _, failure := range []error{context.Canceled, context.DeadlineExceeded} {
		calls := 0
		results, err := RunWithRetry(context.Background(), []queue.Task{{ID: "x"}}, 1, func(context.Context, queue.Task) (string, error) { calls++; return "", failure }, RetryPolicy{MaxAttempts: 3})
		if err != nil || calls != 1 || results[0].Status != "failed" {
			t.Fatalf("%+v %v calls=%d", results, err, calls)
		}
	}
}

func TestInvalidRetryPolicy(t *testing.T) {
	for _, policy := range []RetryPolicy{{MaxAttempts: 0}, {MaxAttempts: -1}, {MaxAttempts: 1, Delay: -time.Second}} {
		called := false
		_, err := RunWithRetry(context.Background(), []queue.Task{{ID: "x"}}, 1, func(context.Context, queue.Task) (string, error) { called = true; return "", nil }, policy)
		if err == nil || called {
			t.Fatal("invalid retry policy accepted")
		}
	}
}

func TestRetryDelay(t *testing.T) {
	calls := 0
	var failedAt time.Time
	const delay = 15 * time.Millisecond
	_, err := RunWithRetry(context.Background(), []queue.Task{{ID: "x"}}, 1, func(context.Context, queue.Task) (string, error) {
		calls++
		if calls == 1 {
			failedAt = time.Now()
			return "", errors.New("retry")
		}
		if time.Since(failedAt) < delay {
			t.Error("retried before delay elapsed")
		}
		return "ok", nil
	}, RetryPolicy{MaxAttempts: 2, Delay: delay})
	if err != nil || calls != 2 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}
