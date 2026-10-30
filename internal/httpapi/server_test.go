package httpapi

import (
	"context"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/worker"
)

func TestServerShutdownCancelsActiveBatch(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	mux, _ := NewHandler(2, worker.RetryPolicy{MaxAttempts: 1})
	entered := make(chan struct{})
	var once sync.Once
	wrapped := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/run" {
			once.Do(func() { close(entered) })
		}
		mux.ServeHTTP(w, r)
	})
	stopped := make(chan error, 1)
	go func() { stopped <- serve(ctx, listener, wrapped) }()
	client := &http.Client{Timeout: 3 * time.Second}
	defer client.CloseIdleConnections()
	requestDone := make(chan error, 1)
	go func() {
		response, err := client.Post("http://"+listener.Addr().String()+"/run", "application/json", strings.NewReader(`[{"id":"future","run_at":"2099-01-01T00:00:00Z"}]`))
		if err == nil {
			io.Copy(io.Discard, response.Body)
			response.Body.Close()
		}
		requestDone <- err
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("request did not enter server")
	}
	response, err := client.Get("http://" + listener.Addr().String() + "/health")
	if err != nil {
		t.Fatal(err)
	}
	response.Body.Close()
	if response.StatusCode != 200 {
		t.Fatal(response.StatusCode)
	}
	cancel()
	select {
	case err := <-stopped:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("shutdown hung")
	}
	select {
	case <-requestDone:
	case <-time.After(3 * time.Second):
		t.Fatal("request hung")
	}
}

func TestOccupiedPort(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := Serve(context.Background(), listener.Addr().String(), 1, worker.RetryPolicy{MaxAttempts: 1}); err == nil {
		t.Fatal("occupied port accepted")
	}
}
