package queue

import (
	"testing"
	"time"
)

func TestScheduleReadinessAndPriority(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	tasks := []Task{
		{ID: "delayed-high", Priority: 100, RunAt: now.Add(time.Hour)},
		{ID: "ready", Priority: 1},
		{ID: "past", Priority: 2, RunAt: now.Add(-time.Hour)},
	}
	s, err := NewSchedule(tasks, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"past", "ready"} {
		got, ok := s.Peek(now)
		if !ok || got.ID != want {
			t.Fatalf("got %+v %v want %s", got, ok, want)
		}
		if s.Take() != got {
			t.Fatal("peek and take differ")
		}
	}
	if _, ok := s.Peek(now.Add(time.Hour - time.Nanosecond)); ok {
		t.Fatal("task ready early")
	}
	due, ok := s.NextRunAt()
	if !ok || !due.Equal(now.Add(time.Hour)) {
		t.Fatalf("due=%v ok=%v", due, ok)
	}
	got, ok := s.Peek(due)
	if !ok || got.ID != "delayed-high" {
		t.Fatal("task not promoted at deadline")
	}
	s.Take()
	if s.Len() != 0 {
		t.Fatal("schedule not empty")
	}
	if _, ok := s.NextRunAt(); ok {
		t.Fatal("unexpected deadline")
	}
}

func TestScheduleStableOrderAcrossPromotion(t *testing.T) {
	now := time.Now()
	s, err := NewSchedule([]Task{
		{ID: "first", Priority: 1, RunAt: now.Add(time.Second)},
		{ID: "second", Priority: 1},
		{ID: "third", Priority: 1, RunAt: now.Add(time.Second)},
		{ID: "urgent", Priority: 9, RunAt: now.Add(time.Second)},
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"urgent", "first", "second", "third"} {
		got, ok := s.Peek(now.Add(time.Second))
		if !ok || got.ID != want {
			t.Fatalf("got %s want %s", got.ID, want)
		}
		s.Take()
	}
}

func TestScheduleValidation(t *testing.T) {
	for _, tasks := range [][]Task{{{ID: ""}}, {{ID: "x"}, {ID: "x", RunAt: time.Now().Add(time.Hour)}}} {
		if _, err := NewSchedule(tasks, time.Now()); err == nil {
			t.Fatal("invalid tasks accepted")
		}
	}
	s, err := NewSchedule(nil, time.Now())
	if err != nil || s.Len() != 0 {
		t.Fatalf("%v %v", s, err)
	}
}
