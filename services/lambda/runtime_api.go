package lambda

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/arn"
	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

// ErrInvocationTimeout is returned when a Lambda invocation exceeds its deadline.
var ErrInvocationTimeout = errors.New("lambda invocation timed out")

// pendingInvocation represents an in-flight Lambda invocation waiting for a container response.
type pendingInvocation struct {
	deadline      time.Time
	createdAt     time.Time // when the event was first received (used for MaximumEventAgeInSeconds)
	requestID     string
	clientContext string
	// durableExecARN is set for an async (Event) invocation of a durable function so
	// the retry loop can record the execution's completion once retries are exhausted.
	durableExecARN string
	// qualifier is the version/alias the caller invoked ("" for the bare function);
	// executedVersion is the version it resolved to.
	qualifier       string
	executedVersion string
	result          chan invocationResult
	payload         []byte
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
	srv            *http.Server
	queue          chan *pendingInvocation
	done           chan struct{}
	exts           *extensionRegistry
	tel            *telemetryHub
	logCancel      atomic.Pointer[context.CancelFunc]
	shutdownReason atomic.Pointer[string]
	closing        chan struct{}
	pending        sync.Map
	port           int
	stopOnce       sync.Once
	logOnce        sync.Once
	traceActive    atomic.Bool
}

// runtimeQueueSize is the buffered depth of the invocation queue per function.
const runtimeQueueSize = 10

// invocationPathParts is the expected number of path segments after stripping the prefix.
const invocationPathParts = 2

// runtimeReadHeaderTimeout limits time to read request headers, guarding against Slowloris.
const runtimeReadHeaderTimeout = 10 * time.Second

// containerResponseGracePeriod is the brief window to wait for a container response
// that arrived concurrently with a timeout or context cancellation.
const containerResponseGracePeriod = 100 * time.Millisecond

// maxInvocationResponseBytes caps the body size for runtime POSTs from a container
// (invocation/response, invocation/error, init/error). AWS Lambda's documented
// synchronous response limit is 6 MiB; anything larger is rejected.
const maxInvocationResponseBytes = 6 * 1024 * 1024

// newRuntimeServer creates a runtimeServer for the given port. Call start() to begin listening.
func newRuntimeServer(port int) *runtimeServer {
	s := &runtimeServer{
		port:  port,
		queue: make(chan *pendingInvocation, runtimeQueueSize),
		done:  make(chan struct{}),

		exts:    newExtensionRegistry(),
		tel:     newTelemetryHub(),
		closing: make(chan struct{}),
	}
	s.exts.hub = s.tel

	return s
}

// setShutdownReason sets the reason carried by the SHUTDOWN event extensions receive.
func (s *runtimeServer) setShutdownReason(reason string) { s.shutdownReason.Store(&reason) }

// routes returns the Runtime API and Extensions API handler.
func (s *runtimeServer) routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/2018-06-01/runtime/invocation/next", s.handleNext)
	mux.HandleFunc("/2018-06-01/runtime/invocation/", s.handleInvocationResult)
	mux.HandleFunc("/2018-06-01/runtime/init/error", s.handleInitError)
	mux.HandleFunc(extensionPathPrefix+"register", s.handleExtensionRegister)
	mux.HandleFunc(extensionPathPrefix+"event/next", s.handleExtensionNext)
	mux.HandleFunc(extensionPathPrefix+"init/error", s.handleExtensionError(true))
	mux.HandleFunc(extensionPathPrefix+"exit/error", s.handleExtensionError(false))
	mux.HandleFunc(telemetryPath, s.handleSubscribe(apiTelemetry))
	mux.HandleFunc(logsPath, s.handleSubscribe(apiLogs))

	return mux
}

// start begins listening on the configured port and serves the Runtime API.
func (s *runtimeServer) start(ctx context.Context) error {
	s.srv = &http.Server{
		Addr:              fmt.Sprintf(":%d", s.port),
		Handler:           s.routes(),
		ReadHeaderTimeout: runtimeReadHeaderTimeout,
	}

	ln, err := (&net.ListenConfig{}).Listen(ctx, "tcp", s.srv.Addr)
	if err != nil {
		return fmt.Errorf("runtime server listen on :%d: %w", s.port, err)
	}

	log := logger.Load(ctx)

	go func() {
		defer close(s.done)

		if serveErr := s.srv.Serve(ln); serveErr != nil && !errors.Is(serveErr, http.ErrServerClosed) {
			log.ErrorContext(ctx, "lambda runtime server error", "port", s.port, "error", serveErr)
		}
	}()

	return nil
}

// stop shuts down the runtime server gracefully.
func (s *runtimeServer) stop(ctx context.Context) {
	reason := shutdownReasonSpindown
	if r := s.shutdownReason.Load(); r != nil {
		reason = *r
	}

	s.tel.invokePhaseEnded(ctx, time.Now())
	s.tel.flush(ctx)
	s.exts.shutdown(ctx, reason)
	s.tel.close(ctx)

	if cancel := s.logCancel.Load(); cancel != nil {
		(*cancel)()
	}

	s.stopOnce.Do(func() {
		close(s.closing)
		s.exts.close()
	})

	if s.srv != nil {
		_ = s.srv.Shutdown(ctx)

		select {
		case <-s.done:
		case <-ctx.Done():
		}
	}
}

// invoke enqueues a payload and blocks until the container responds or timeout is reached.
// Returns the response payload, whether it was a function error, and any system error.
func (s *runtimeServer) invoke(
	ctx context.Context,
	payload []byte,
	clientContext string,
	timeout time.Duration,
) ([]byte, bool, string, error) {
	reqID := uuid.New().String()
	inv := &pendingInvocation{
		requestID:     reqID,
		payload:       payload,
		clientContext: clientContext,
		deadline:      time.Now().Add(timeout),
		result:        make(chan invocationResult, 1),
	}

	select {
	case s.queue <- inv:
	case <-ctx.Done():
		return nil, false, "", ctx.Err()
	}

	timeoutTimer := time.NewTimer(timeout)
	defer timeoutTimer.Stop()

	select {
	case res := <-inv.result:
		return res.payload, res.isError, reqID, nil
	case <-timeoutTimer.C:
		// Atomically remove the invocation from the pending map.
		// If LoadAndDelete returns false, handleInvocationResult already claimed it
		// and will send on inv.result; wait briefly to avoid losing the result.
		if _, claimed := s.pending.LoadAndDelete(inv.requestID); !claimed {
			graceTimer := time.NewTimer(containerResponseGracePeriod)

			select {
			case res := <-inv.result:
				if !graceTimer.Stop() {
					<-graceTimer.C
				}

				return res.payload, res.isError, reqID, nil
			case <-graceTimer.C:
			}
		}

		s.tel.timedOut(ctx, reqID, time.Now())

		return nil, false, "", fmt.Errorf("%w after %s", ErrInvocationTimeout, timeout)
	case <-ctx.Done():
		if _, claimed := s.pending.LoadAndDelete(inv.requestID); !claimed {
			graceTimer := time.NewTimer(containerResponseGracePeriod)

			select {
			case res := <-inv.result:
				if !graceTimer.Stop() {
					<-graceTimer.C
				}

				return res.payload, res.isError, reqID, nil
			case <-graceTimer.C:
			}
		}

		return nil, false, "", ctx.Err()
	}
}

// handleNext serves GET /2018-06-01/runtime/invocation/next.
// It blocks until an invocation is available, then returns the payload with runtime headers.
func (s *runtimeServer) handleNext(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)

		return
	}

	s.exts.awaitInit(r.Context())
	s.tel.initFinished(time.Now(), "")
	s.exts.awaitIdle(r.Context())
	s.tel.invokePhaseEnded(r.Context(), time.Now())

	select {
	case inv := <-s.queue:
		s.pending.Store(inv.requestID, inv)
		w.Header().Set("Lambda-Runtime-Aws-Request-Id", inv.requestID)
		w.Header().Set("Lambda-Runtime-Deadline-Ms", strconv.FormatInt(inv.deadline.UnixMilli(), 10))
		fnARN := s.functionARN()
		w.Header().Set("Lambda-Runtime-Invoked-Function-Arn", fnARN)
		if inv.clientContext != "" {
			w.Header().Set("Lambda-Runtime-Client-Context", inv.clientContext)
		}
		trace := ""
		if s.traceActive.Load() {
			trace = newTraceHeader(time.Now())
			w.Header().Set("Lambda-Runtime-Trace-Id", trace)
		}

		s.tel.invokeStarted(inv.requestID, trace, time.Now())
		s.exts.fanoutInvoke(inv.requestID, inv.deadline, fnARN, trace)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(inv.payload)
	case <-r.Context().Done():
		// Container disconnected or request cancelled — do nothing.
	case <-s.closing:
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

	readStart := time.Now()
	body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxInvocationResponseBytes))
	readEnd := time.Now()

	defer r.Body.Close()

	if readErr != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](readErr); ok {
			http.Error(w, "response payload too large", http.StatusRequestEntityTooLarge)

			return
		}

		// Log but continue — partial body may still be useful.
		logger.Load(r.Context()).
			WarnContext(r.Context(), "lambda: error reading invocation result body", "requestID", requestID, "error", readErr)
	}

	raw, ok := s.pending.LoadAndDelete(requestID)
	if !ok {
		http.Error(w, "request not found", http.StatusNotFound)

		return
	}

	inv, isInv := raw.(*pendingInvocation)
	if !isInv {
		logger.Load(r.Context()).ErrorContext(r.Context(), "lambda: unexpected type in pending invocations map")
		http.Error(w, "internal error", http.StatusInternalServerError)

		return
	}

	s.tel.runtimeDone(requestID, action == statusError,
		functionErrorType(r.Header.Get("Lambda-Runtime-Function-Error-Type"), body), len(body), readStart, readEnd)

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

	body, readErr := io.ReadAll(http.MaxBytesReader(w, r.Body, maxInvocationResponseBytes))
	defer r.Body.Close()

	if readErr != nil {
		if _, ok := errors.AsType[*http.MaxBytesError](readErr); ok {
			http.Error(w, "init error payload too large", http.StatusRequestEntityTooLarge)

			return
		}

		// Log but continue — partial body may still be useful.
		logger.Load(r.Context()).WarnContext(r.Context(), "lambda: error reading init error body", "error", readErr)
	}

	logger.Load(r.Context()).ErrorContext(r.Context(), "lambda runtime init error", "error", string(body))

	s.setShutdownReason(shutdownReasonFailure)
	s.tel.initFinished(time.Now(), functionErrorType(r.Header.Get("Lambda-Runtime-Function-Error-Type"), body))
	s.failPending(body)

	w.WriteHeader(http.StatusAccepted)
}

// functionARN is the ARN reported to the runtime and extensions as the invoked function.
func (s *runtimeServer) functionARN() string {
	s.exts.mu.RLock("functionARN")
	defer s.exts.mu.RUnlock()

	if s.exts.meta.functionARN != "" {
		return s.exts.meta.functionARN
	}

	return arn.Build("lambda", config.DefaultRegion, config.DefaultAccountID, "function:unknown")
}

// failPending delivers an init error to the first pending invocation so the caller
// gets an immediate error response instead of timing out.
func (s *runtimeServer) failPending(body []byte) {
	s.pending.Range(func(key, value any) bool {
		inv, ok := value.(*pendingInvocation)
		if !ok {
			return true
		}

		select {
		case inv.result <- invocationResult{payload: body, isError: true, statusCode: http.StatusOK}:
		default:
		}

		s.pending.Delete(key)

		return false // stop after first
	})
}
