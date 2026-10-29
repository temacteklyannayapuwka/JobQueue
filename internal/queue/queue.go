package queue

import (
	"container/heap"
	"errors"
	"strings"
	"time"
)

type Task struct {
	RunAt    time.Time `json:"run_at,omitempty"`
	ID       string    `json:"id"`
	Priority int       `json:"priority"`
	Payload  string    `json:"payload"`
}

type item struct {
	task     Task
	sequence uint64
}

type taskHeap []item

func (h taskHeap) Len() int { return len(h) }
func (h taskHeap) Less(i, j int) bool {
	if h[i].task.Priority == h[j].task.Priority {
		return h[i].sequence < h[j].sequence
	}
	return h[i].task.Priority > h[j].task.Priority
}
func (h taskHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *taskHeap) Push(v any)   { *h = append(*h, v.(item)) }
func (h *taskHeap) Pop() any {
	old := *h
	n := len(old) - 1
	v := old[n]
	old[n] = item{}
	*h = old[:n]
	return v
}

type Queue struct {
	items taskHeap
	ids   map[string]struct{}
	next  uint64
}

func (q *Queue) Enqueue(task Task) error {
	if strings.TrimSpace(task.ID) == "" {
		return errors.New("task ID is required")
	}
	if _, exists := q.ids[task.ID]; exists {
		return errors.New("task ID is already queued")
	}
	if q.ids == nil {
		q.ids = make(map[string]struct{})
	}
	heap.Push(&q.items, item{task: task, sequence: q.next})
	q.next++
	q.ids[task.ID] = struct{}{}
	return nil
}

func (q *Queue) Dequeue() (Task, bool) {
	if q.Len() == 0 {
		return Task{}, false
	}
	task := heap.Pop(&q.items).(item).task
	delete(q.ids, task.ID)
	return task, true
}

func (q *Queue) Len() int { return len(q.items) }
