package httpapi

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/temacteklyannayapuwka/JobQueue/internal/batch"
	"github.com/temacteklyannayapuwka/JobQueue/internal/worker"
)

const maxBodyBytes = 1 << 20
const batchTimeout = 30 * time.Second

type handler struct {
	workers int
	policy  worker.RetryPolicy
	active  chan struct{}
	timeout time.Duration
}

func NewHandler(workers int, policy worker.RetryPolicy) (http.Handler, error) {
	if workers < 1 {
		return nil, errors.New("workers must be positive")
	}
	if err := policy.Validate(); err != nil {
		return nil, err
	}
	h := &handler{workers: workers, policy: policy, active: make(chan struct{}, 1), timeout: batchTimeout}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /health", func(w http.ResponseWriter, r *http.Request) {
		writeJSON(w, http.StatusOK, map[string]string{"status": "ok"})
	})
	mux.HandleFunc("POST /run", h.run)
	return mux, nil
}

func (h *handler) run(w http.ResponseWriter, r *http.Request) {
	select {
	case h.active <- struct{}{}:
		defer func() { <-h.active }()
	default:
		writeError(w, http.StatusTooManyRequests, "a batch is already running")
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), h.timeout)
	defer cancel()
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	defer r.Body.Close()
	results, err := batch.Run(ctx, r.Body, h.workers, h.policy)
	if err != nil {
		var sizeErr *http.MaxBytesError
		switch {
		case errors.As(err, &sizeErr):
			writeError(w, http.StatusRequestEntityTooLarge, "request body exceeds 1 MiB")
		case errors.Is(err, context.DeadlineExceeded):
			writeError(w, http.StatusGatewayTimeout, "batch execution timed out")
		case errors.Is(err, context.Canceled):
			writeError(w, http.StatusRequestTimeout, "batch execution canceled")
		default:
			writeError(w, http.StatusBadRequest, err.Error())
		}
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"results": results})
}

func writeError(w http.ResponseWriter, status int, message string) {
	writeJSON(w, status, map[string]string{"error": message})
}
func writeJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
