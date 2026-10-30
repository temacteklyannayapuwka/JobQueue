package httpapi

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/worker"
)

func TestRunAndHealth(t *testing.T) {
	h, err := NewHandler(2, worker.RetryPolicy{MaxAttempts: 3})
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/run", strings.NewReader(`[{"id":"low","priority":1,"payload":"abc"},{"id":"high","priority":9,"payload":""}]`))
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	var response struct {
		Results []worker.Result `json:"results"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil || len(response.Results) != 2 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	if response.Results[0].ID != "high" || response.Results[1].ID != "low" {
		t.Fatal(response.Results)
	}
	if response.Results[1].Output != "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad" || response.Results[1].Attempts != 1 {
		t.Fatal(response.Results)
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("GET", "/health", nil))
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/json" {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestInputErrors(t *testing.T) {
	h, _ := NewHandler(1, worker.RetryPolicy{MaxAttempts: 1})
	for _, body := range []string{``, `null`, `{}`, `[] []`, `[{"id":"x","unknown":1}]`, `[{"id":""}]`, `[{"id":"x"},{"id":"x"}]`, `[{"id":"x","run_at":"later"}]`} {
		t.Run(body, func(t *testing.T) {
			w := httptest.NewRecorder()
			h.ServeHTTP(w, httptest.NewRequest("POST", "/run", strings.NewReader(body)))
			if w.Code != 400 || !json.Valid(w.Body.Bytes()) {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	body := `[{"id":"x","payload":"` + strings.Repeat("x", maxBodyBytes) + `"}]`
	w := httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/run", strings.NewReader(body)))
	if w.Code != 413 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/run", strings.NewReader(`[]`+strings.Repeat(" ", maxBodyBytes))))
	if w.Code != 413 {
		t.Fatalf("oversized trailing whitespace: %d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.ServeHTTP(w, httptest.NewRequest("POST", "/run", strings.NewReader(`[]`)))
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"results":[]`) {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
}

func TestRoutes(t *testing.T) {
	h, _ := NewHandler(1, worker.RetryPolicy{MaxAttempts: 1})
	for _, tc := range []struct {
		method, path string
		status       int
	}{{"GET", "/run", 405}, {"POST", "/health", 405}, {"GET", "/missing", 404}} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(tc.method, tc.path, nil))
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d", tc.method, tc.path, w.Code)
		}
	}
}

func TestBusyAndCancellation(t *testing.T) {
	h := &handler{workers: 1, policy: worker.RetryPolicy{MaxAttempts: 1}, active: make(chan struct{}, 1), timeout: time.Hour}
	h.active <- struct{}{}
	w := httptest.NewRecorder()
	h.run(w, httptest.NewRequest("POST", "/run", strings.NewReader(`[]`)))
	if w.Code != 429 {
		t.Fatal(w.Code)
	}
	<-h.active
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	w = httptest.NewRecorder()
	h.run(w, httptest.NewRequest("POST", "/run", strings.NewReader(`[{"id":"x"}]`)).WithContext(ctx))
	if w.Code != 408 || len(h.active) != 0 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	h.timeout = 10 * time.Millisecond
	w = httptest.NewRecorder()
	h.run(w, httptest.NewRequest("POST", "/run", strings.NewReader(`[{"id":"later","run_at":"2099-01-01T00:00:00Z"}]`)))
	if w.Code != 504 || len(h.active) != 0 {
		t.Fatalf("%d %s", w.Code, w.Body.String())
	}
	w = httptest.NewRecorder()
	h.run(w, httptest.NewRequest("POST", "/run", strings.NewReader(`[{"id":"ready"}]`)))
	if w.Code != 200 {
		t.Fatalf("slot not released: %d %s", w.Code, w.Body.String())
	}
}

func TestInvalidConfiguration(t *testing.T) {
	for _, tc := range []struct {
		workers int
		policy  worker.RetryPolicy
	}{{0, worker.RetryPolicy{MaxAttempts: 1}}, {1, worker.RetryPolicy{}}, {1, worker.RetryPolicy{MaxAttempts: 1, Delay: -1}}} {
		if _, err := NewHandler(tc.workers, tc.policy); err == nil {
			t.Fatal("invalid configuration accepted")
		}
	}
}
