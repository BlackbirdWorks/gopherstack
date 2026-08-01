package logger

import (
	"bytes"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/labstack/echo/v5"
)

const (
	defaultChannelSize = 100

	// maxCapturedBodyBytes caps how much of each request body is kept in the
	// API console ring buffer. The handler still sees the full body via a
	// pass-through reader; only the captured copy is bounded so a few large
	// uploads (for example S3 PutObject) cannot pin hundreds of MiB in memory
	// for the lifetime of the ring buffer.
	maxCapturedBodyBytes = 8 * 1024
)

// CapturedRequest represents a single HTTP request captured by the console middleware.
type CapturedRequest struct {
	Timestamp time.Time         `json:"timestamp"`
	Headers   map[string]string `json:"headers"`
	ID        string            `json:"id"`
	Method    string            `json:"method"`
	Path      string            `json:"path"`
	Body      string            `json:"body,omitempty"`
	Status    int               `json:"status"`
	Duration  time.Duration     `json:"duration_ms"`
}

// RequestRingBuffer holds the last N captured requests.
type RequestRingBuffer struct {
	subs     map[chan *CapturedRequest]bool
	requests []*CapturedRequest
	maxSize  int
	cursor   int
	mu       sync.RWMutex
	subMu    sync.RWMutex
}

// NewRequestRingBuffer creates a new ring buffer for captured requests.
func NewRequestRingBuffer(maxSize int) *RequestRingBuffer {
	return &RequestRingBuffer{
		requests: make([]*CapturedRequest, 0, maxSize),
		maxSize:  maxSize,
		cursor:   0,
		subs:     make(map[chan *CapturedRequest]bool),
	}
}

// Subscribe adds a channel to receive incoming requests.
func (r *RequestRingBuffer) Subscribe() chan *CapturedRequest {
	ch := make(chan *CapturedRequest, defaultChannelSize)
	r.subMu.Lock()
	r.subs[ch] = true
	r.subMu.Unlock()

	return ch
}

// Unsubscribe removes a channel from receiving requests.
func (r *RequestRingBuffer) Unsubscribe(ch chan *CapturedRequest) {
	r.subMu.Lock()
	delete(r.subs, ch)
	r.subMu.Unlock()
}

// Add appends a new request into the ring buffer and notifies subscribers.
func (r *RequestRingBuffer) Add(req *CapturedRequest) {
	r.mu.Lock()
	if len(r.requests) < r.maxSize {
		r.requests = append(r.requests, req)
	} else {
		r.requests[r.cursor] = req
		r.cursor = (r.cursor + 1) % r.maxSize
	}
	r.mu.Unlock()

	r.subMu.RLock()
	for ch := range r.subs {
		select {
		case ch <- req:
		default:
			// If a subscriber's channel is full, we drop the message
			// so that we don't block the API proxying/logging path.
		}
	}
	r.subMu.RUnlock()
}

// GetAll returns all captured requests in chronological order.
func (r *RequestRingBuffer) GetAll() []*CapturedRequest {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*CapturedRequest, 0, len(r.requests))
	if len(r.requests) < r.maxSize {
		result = append(result, r.requests...)
	} else {
		result = append(result, r.requests[r.cursor:]...)
		result = append(result, r.requests[:r.cursor]...)
	}

	return result
}

const defaultBufferSize = 100

// GlobalRingBuffer is the global buffer for the Live API Console.
// It stores the last 100 requests.
//
//nolint:gochecknoglobals // required for shared state across middleware and console handlers
var GlobalRingBuffer = NewRequestRingBuffer(defaultBufferSize)

// redactedHeaderValue replaces the value of any sensitive request header
// captured for the console/log so secrets never appear in clear text.
const redactedHeaderValue = "[REDACTED]"

// skipAPIConsoleCapture reports whether a request should bypass capture
// entirely: dashboard requests (to avoid recursion/noise) and the
// health/root checks that would otherwise flood the console with no useful
// signal.
func skipAPIConsoleCapture(req *http.Request) bool {
	if strings.HasPrefix(req.URL.Path, "/dashboard") {
		return true
	}

	return req.URL.Path == "/_gopherstack/health" || (req.URL.Path == "/" && req.Method == http.MethodGet)
}

// captureRequestHeaders copies request headers into a plain map, redacting
// the values of any header in sensitiveHeaders (auth tokens, SigV4
// credentials/signatures, cookies, API keys) so secrets are never stored in
// the console ring buffer or logged in clear text.
func captureRequestHeaders(header http.Header, sensitiveHeaders map[string]struct{}) map[string]string {
	headers := make(map[string]string)
	for k, v := range header {
		if len(v) == 0 {
			continue
		}
		if _, secret := sensitiveHeaders[k]; secret {
			headers[k] = redactedHeaderValue
		} else {
			headers[k] = v[0]
		}
	}

	return headers
}

// attachBodyCapture wraps req.Body (if present) in a cappedTeeBody so that up
// to maxCapturedBodyBytes are copied into the returned buffer while every
// Read still passes through to the underlying body unchanged -- the
// downstream handler observes the exact same stream it always did, just
// teed. This avoids io.ReadAll-ing the entire body (which would
// double-buffer large uploads in memory) and there is nothing to restore
// afterward because the original body is never fully consumed or replaced,
// only wrapped. Returns nil if the request has no body.
func attachBodyCapture(req *http.Request) *bytes.Buffer {
	if req.Body == nil {
		return nil
	}

	bodyBuf := &bytes.Buffer{}
	req.Body = &cappedTeeBody{src: req.Body, buf: bodyBuf, cap: maxCapturedBodyBytes}

	return bodyBuf
}

// resolveResponseStatus determines the status code to record for the
// captured request: 500 when the handler returned an error, otherwise
// whatever status the echo response writer exposes (falling back to 200 if
// neither accessor is implemented).
func resolveResponseStatus(resp http.ResponseWriter, err error) int {
	if err != nil {
		return http.StatusInternalServerError
	}
	if rw, ok := resp.(interface{ StatusCode() int }); ok {
		return rw.StatusCode()
	}
	if rw, ok := resp.(interface{ Status() int }); ok {
		return rw.Status()
	}

	return http.StatusOK
}

// capturedBodyString returns the bytes captured into buf, resolved after the
// handler has had a chance to read req.Body. It is "" if nothing was
// captured (no body, or an empty body).
func capturedBodyString(buf *bytes.Buffer) string {
	if buf != nil && buf.Len() > 0 {
		return buf.String()
	}

	return ""
}

// APIConsoleMiddleware captures incoming API requests and stores them in the ring buffer.
// It should be injected after standard loggers but before request processing.
func APIConsoleMiddleware() echo.MiddlewareFunc {
	// sensitiveHeaders is the set of request headers whose values are secrets
	// (authorization tokens, SigV4 credentials/signatures, session cookies, API
	// keys) and must never be stored in the console ring buffer or logged. Keys
	// are in http.Header canonical form (as produced by Go's HTTP server).
	// Allocated once per middleware construction (i.e. once at setup).
	sensitiveHeaders := map[string]struct{}{
		"Authorization":        {},
		"Proxy-Authorization":  {},
		"Cookie":               {},
		"Set-Cookie":           {},
		"X-Api-Key":            {},
		"X-Amz-Security-Token": {},
		"X-Amz-Credential":     {},
		"X-Amz-Signature":      {},
	}

	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			start := time.Now()
			req := c.Request()

			if skipAPIConsoleCapture(req) {
				return next(c)
			}

			headers := captureRequestHeaders(req.Header, sensitiveHeaders)
			bodyBuf := attachBodyCapture(req)

			reqID := c.Response().Header().Get(echo.HeaderXRequestID)

			err := next(c)

			status := resolveResponseStatus(c.Response(), err)
			reqBody := capturedBodyString(bodyBuf)

			// Store in ring buffer
			GlobalRingBuffer.Add(&CapturedRequest{
				ID:        reqID,
				Method:    req.Method,
				Path:      req.URL.Path,
				Headers:   headers,
				Body:      reqBody,
				Status:    status,
				Duration:  time.Since(start),
				Timestamp: time.Now(),
			})

			return err
		}
	}
}

// cappedTeeBody wraps an http.Request body so that at most cap bytes are
// copied into buf while every Read passes through to the underlying body
// unchanged. Once buf is full, further reads are not copied; this bounds the
// memory the API console retains per request even if the body is large.
type cappedTeeBody struct {
	src io.ReadCloser
	buf *bytes.Buffer
	cap int
}

func (c *cappedTeeBody) Read(p []byte) (int, error) {
	n, err := c.src.Read(p)
	if n > 0 && c.buf.Len() < c.cap {
		room := min(c.cap-c.buf.Len(), n)
		c.buf.Write(p[:room])
	}

	return n, err //nolint:wrapcheck // pass-through reader; wrapping obscures the upstream error.
}

// Close closes the underlying body. It returns the underlying error directly
// so the caller observes the original close failure mode.
func (c *cappedTeeBody) Close() error {
	return c.src.Close() //nolint:wrapcheck // pass-through; underlying close error semantics must be preserved.
}
