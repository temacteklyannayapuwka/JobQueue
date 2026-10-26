package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/temacteklyannayapuwka/JobQueue/internal/queue"
)

func run(input io.Reader, output io.Writer) error {
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
	var q queue.Queue
	for _, task := range tasks {
		if err := q.Enqueue(task); err != nil {
			return fmt.Errorf("enqueue %q: %w", task.ID, err)
		}
	}
	encoder := json.NewEncoder(output)
	for task, ok := q.Dequeue(); ok; task, ok = q.Dequeue() {
		if err := encoder.Encode(task); err != nil {
			return err
		}
	}
	return nil
}

func main() {
	path := flag.String("tasks", "examples/tasks.json", "path to a JSON array of tasks")
	flag.Parse()
	file, err := os.Open(*path)
	if err == nil {
		defer file.Close()
		err = run(file, os.Stdout)
	}
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
