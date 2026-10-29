package queue

import (
	"container/heap"
	"time"
)

type delayedHeap []item

func (h delayedHeap) Len() int { return len(h) }
func (h delayedHeap) Less(i, j int) bool {
	if h[i].task.RunAt.Equal(h[j].task.RunAt) {
		return h[i].sequence < h[j].sequence
	}
	return h[i].task.RunAt.Before(h[j].task.RunAt)
}
func (h delayedHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *delayedHeap) Push(v any)   { *h = append(*h, v.(item)) }
func (h *delayedHeap) Pop() any {
	old := *h
	n := len(old) - 1
	v := old[n]
	old[n] = item{}
	*h = old[:n]
	return v
}

type Schedule struct {
	ready   taskHeap
	delayed delayedHeap
}

func NewSchedule(tasks []Task, now time.Time) (*Schedule, error) {
	var validated Queue
	for _, task := range tasks {
		if err := validated.Enqueue(task); err != nil {
			return nil, err
		}
	}
	s := &Schedule{}
	for _, entry := range validated.items {
		if entry.task.RunAt.After(now) {
			s.delayed = append(s.delayed, entry)
		} else {
			s.ready = append(s.ready, entry)
		}
	}
	heap.Init(&s.ready)
	heap.Init(&s.delayed)
	return s, nil
}

func (s *Schedule) Peek(now time.Time) (Task, bool) {
	for len(s.delayed) > 0 && !s.delayed[0].task.RunAt.After(now) {
		heap.Push(&s.ready, heap.Pop(&s.delayed).(item))
	}
	if len(s.ready) == 0 {
		return Task{}, false
	}
	return s.ready[0].task, true
}

func (s *Schedule) Take() Task { return heap.Pop(&s.ready).(item).task }
func (s *Schedule) Len() int   { return len(s.ready) + len(s.delayed) }
func (s *Schedule) NextRunAt() (time.Time, bool) {
	if len(s.delayed) == 0 {
		return time.Time{}, false
	}
	return s.delayed[0].task.RunAt, true
}
