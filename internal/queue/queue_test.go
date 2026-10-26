package queue

import (
	"fmt"
	"math/rand"
	"sort"
	"testing"
)

func TestQueueOrder(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	var q Queue
	tasks := make([]Task, 500)
	for i := range tasks {
		tasks[i] = Task{ID: fmt.Sprint(i), Priority: rng.Intn(21) - 10, Payload: fmt.Sprint(i)}
		if err := q.Enqueue(tasks[i]); err != nil {
			t.Fatal(err)
		}
	}
	sort.SliceStable(tasks, func(i, j int) bool { return tasks[i].Priority > tasks[j].Priority })
	for i, want := range tasks {
		got, ok := q.Dequeue()
		if !ok || got != want {
			t.Fatalf("task %d: got %+v, %v; want %+v", i, got, ok, want)
		}
		if q.Len() != len(tasks)-i-1 {
			t.Fatal("incorrect length")
		}
	}
	if _, ok := q.Dequeue(); ok {
		t.Fatal("empty queue returned task")
	}
}

func TestQueueValidationAndReuse(t *testing.T) {
	var q Queue
	for _, id := range []string{"", "  ", "\t"} {
		if err := q.Enqueue(Task{ID: id}); err == nil {
			t.Fatal("accepted empty ID")
		}
	}
	task := Task{ID: "export", Priority: 5}
	if err := q.Enqueue(task); err != nil {
		t.Fatal(err)
	}
	if err := q.Enqueue(task); err == nil {
		t.Fatal("accepted duplicate")
	}
	if q.Len() != 1 {
		t.Fatal("rejected task changed queue")
	}
	q.Dequeue()
	if err := q.Enqueue(task); err != nil {
		t.Fatal(err)
	}
	got, ok := q.Dequeue()
	if !ok || got != task {
		t.Fatal("reused task missing")
	}
}

func TestInterleavedQueue(t *testing.T) {
	var q Queue
	q.Enqueue(Task{ID: "first", Priority: 1})
	q.Enqueue(Task{ID: "urgent", Priority: 2})
	if got, _ := q.Dequeue(); got.ID != "urgent" {
		t.Fatal(got)
	}
	q.Enqueue(Task{ID: "second", Priority: 1})
	for _, want := range []string{"first", "second"} {
		if got, _ := q.Dequeue(); got.ID != want {
			t.Fatalf("got %s, want %s", got.ID, want)
		}
	}
}
