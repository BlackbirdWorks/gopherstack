package lambda

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
)

// ErrInvocationTimeout is returned when a Lambda invocation exceeds its deadline.
var ErrInvocationTimeout = errors.New("lambda invocation timed out")

// pendingInvocation represents an in-flight Lambda invocation waiting for a container response.
type pendingInvocation struct {
	deadline  time.Time
	requestID string
	result    chan invocationResult
	payload   []byte
}

// invocationResult holds the outcome of a Lambda container invocation.
type invocationResult struct {
	headers    http.Header
	payload    []byte
	statusCode int
	isError    bool
}

// runtimeServer is a per-function Lambda Runtime API server.
// Containers call /next to receive invocations and /response or /error to return results.
type runtimeServer struct {
	srv     *http.Server
	queue   chan *pendingInvocation
	logger  *slog.Logger
	pending sync.Map
	port    int
}

// runtimeQueueSize is the buffered depth of the invocation queue per function.
const runtimeQueueSize = 10

// invocationPathParts is the expected number of path segments after stripping the prefix.
const invocationPathParts = 2

// runtimeReadHeaderTimeout limits time to read request headers, guarding against Slowloris.
const runtimeReadHeaderTimeout = 10 * time.Second

// newRuntimeServer creates a runtimeServer for the given port. Call start() to begin listening.
func newRuntimeServer(port int, log *slog.Logger) *runtimeServer {
	return &runtimeServer{
		port:   port,
		queue:  make(chan *pendingInvocation, runtimeQueueSize),
		logger: log,
	}
}

// start begins listening on the configured port and serves the Runtime API.
func (s *runtimeServer) start(ctx context.Context) error {
	mux := http.NewServeMux()
	mux.HandleFunc("/2018-06-01/runtime/invocation/next", s.handleNext)
	mux.HandleFunc("/2018-06-01/runtime/invocation/", s.handleInvocationResult)
	mux.HandleFunc("/2018-06-01/runtime/init/error", s.handleInitError)

	s.srv = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           mux,
		ReadHeaderTimeout: runtimeReadHeaderTimeout,
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("runtime server listen on :%d: %w", s.port, err)
	}

	go func() {
		if serveErr := s.srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			s.logger.ErrorContext(ctx, "lambda runtime server error", "port", s.port, "error", serveErr)
		}
	}()

	return nil
}

// stop shuts down the runtime server gracefully.
func (s *runtimeServer) stop(ctx context.Context) {
	if s.srv != nil {
		_ = s.srv.Shutdown(ctx)
	}
}

// invoke enqueues a payload and blocks until the container responds or timeout is reached.
// Returns the response payload, whether it was a function error, and any system error.
func (s *runtimeServer) invoke(ctx context.Context, payload []byte, timeout time.Duration) ([]byte, bool, error) {
	inv := &pendingInvocation{
		requestID: uuid.New().String(),
		payload:   payload,
		deadline:  time.Now().Add(timeout),
		result:    make(chan invocationResult, 1),
	}

	select {
	case s.queue <- inv:
	case <-ctx.Done():
		return nil, false, ctx.Err()
	}

	select {
	case res := <-inv.result:
		return res.payload, res.isError, nil
	case <-time.After(timeout):
		s.pending.Delete(inv.requestID)

		return nil, false, fmt.Errorf("%w after %s", ErrInvocationTimeout, timeout)
	case <-ctx.Done():
		s.pending.Delete(inv.requestID)

		return nil, false, ctx.Err()
	}
}

// handleNext serves GET /2018-06-01/runtime/invocation/next.
// It blocks until an invocation is available, then returns the payload with runtime headers.
func (s *runtimeServer) handleNext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	select {
	case inv := <-s.queue:
		s.pending.Store(inv.requestID, inv)
		w.Header().Set("Lambda-Runtime-Aws-Request-Id", inv.requestID)
		w.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(inv.deadline.UnixMilli(), 10))
		w.Header().Set("Lambda-Runtime-Invoked-Function-Arn", "arn:aws:lambda:us-east-1:000000000000:function:unknown")
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(inv.payload)
	case <-r.Context().Done():
		// Container disconnected or request cancelled — do nothing.
	}
}

// handleInvocationResult serves:
//
//	POST /2018-06-01/runtime/invocation/{requestId}/response
//	POST /2018-06-01/runtime/invocation/{requestId}/error
func (s *runtimeServer) handleInvocationResult(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	const prefix = "/2018-06-01/runtime/invocation/"

	rest := strings.TrimPrefix(r.URL.Path, prefix)
	parts := strings.SplitN(rest, "/", invocationPathParts)

	if len(parts) != invocationPathParts {
		http.Error(w, "invalid path", http.StatusBadRequest)

		return
	}

	requestID := parts[0]
	action := parts[1] // "response" or "error"

	body, readErr := io.ReadAll(r.Body)
	defer r.Body.Close()

	if readErr != nil {
		// Log but continue — partial body may still be useful.
		s.logger.Warn("lambda: error reading invocation result body", "requestID", requestID, "error", readErr)
	}

	raw, ok := s.pending.LoadAndDelete(requestID)
	if !ok {
		http.Error(w, "request not found", http.StatusNotFound)

		return
	}

	inv, isInv := raw.(*pendingInvocation)
	if !isInv {
		s.logger.Error("lambda: unexpected type in pending invocations map")
		http.Error(w, "internal error", http.StatusInternalServerError)

		return
	}

	inv.result <- invocationResult{
		payload:    body,
		statusCode: http.StatusOK,
		headers:    r.Header.Clone(),
		isError:    action == "error",
	}

	w.WriteHeader(http.StatusAccepted)
}

// handleInitError serves POST /2018-06-01/runtime/init/error.
// Called by a container that fails to initialize.
func (s *runtimeServer) handleInitError(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	body, readErr := io.ReadAll(r.Body)
	defer r.Body.Close()

	if readErr != nil {
		// Log but continue — partial body may still be useful.
		s.logger.Warn("lambda: error reading init error body", "error", readErr)
	}

	s.logger.Error("lambda runtime init error", "error", string(body))
	w.WriteHeader(http.StatusAccepted)
}
