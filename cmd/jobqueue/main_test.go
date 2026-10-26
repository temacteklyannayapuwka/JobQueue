package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {
	input := `[{"id":"low","priority":1},{"id":"first","priority":9},{"id":"second","priority":9}]`
	var out bytes.Buffer
	if err := run(strings.NewReader(input), &out); err != nil {
		t.Fatal(err)
	}
	decoder := json.NewDecoder(&out)
	for _, want := range []string{"first", "second", "low"} {
		var task queue.Task
		if err := decoder.Decode(&task); err != nil {
			t.Fatal(err)
		}
		if task.ID != want {
			t.Fatalf("got %s, want %s", task.ID, want)
		}
	}
}

func TestRunRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{``, `{}`, `[{`, `[{"id":""}]`, `[{"id":"x"},{"id":"x"}]`, `[{"id":"x","prioritty":2}]`, `[] []`, `[] junk`} {
		t.Run(input, func(t *testing.T) {
			var out bytes.Buffer
			if err := run(strings.NewReader(input), &out); err == nil {
				t.Fatal("accepted invalid input")
			}
			if out.Len() != 0 {
				t.Fatal("partial output")
			}
		})
	}
}

type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errors.New("write failed") }
func TestRunWriteError(t *testing.T) {
	if err := run(strings.NewReader(`[{"id":"x"}]`), brokenWriter{}); err == nil {
		t.Fatal("ignored write error")
	}
}
