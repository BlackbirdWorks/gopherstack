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

// APIConsoleMiddleware captures incoming API requests and stores them in the ring buffer.
// It should be injected after standard loggers but before request processing.
//
//nolint:gocognit // middleware performs explicit filtering, capture, and enrichment in one path.
func APIConsoleMiddleware() echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c *echo.Context) error {
			start := time.Now()
			req := c.Request()
			// Skip capturing dashboard requests to avoid recursion or noise
			if strings.HasPrefix(req.URL.Path, "/dashboard") {
				return next(c)
			}
			if req.URL.Path == "/_gopherstack/health" || (req.URL.Path == "/" && req.Method == http.MethodGet) {
				return next(c)
			}

			// Capture headers
			headers := make(map[string]string)
			for k, v := range req.Header {
				if len(v) > 0 {
					headers[k] = v[0]
				}
			}

			// Capture body if present. cappedTeeBody copies at most
			// maxCapturedBodyBytes into bodyBuf while passing the original
			// stream through to the handler unchanged. This avoids
			// io.ReadAll-ing the entire body (which would double-buffer
			// large uploads in memory) and bounds what the ring buffer can
			// retain per request.
			var bodyBuf *bytes.Buffer
			if req.Body != nil {
				bodyBuf = &bytes.Buffer{}
				req.Body = &cappedTeeBody{src: req.Body, buf: bodyBuf, cap: maxCapturedBodyBytes}
			}

			reqID := c.Response().Header().Get(echo.HeaderXRequestID)

			err := next(c)

			// try to get status code from our httputil or error
			status := 200
			if err != nil {
				status = 500
			} else if rw, ok := c.Response().(interface{ StatusCode() int }); ok {
				status = rw.StatusCode()
			} else if rw, ok2 := c.Response().(interface{ Status() int }); ok2 {
				status = rw.Status()
			}

			// Resolve captured body after the handler has read req.Body. The
			// teed buffer holds at most maxCapturedBodyBytes regardless of how
			// large the actual request body was.
			var reqBody string
			if bodyBuf != nil && bodyBuf.Len() > 0 {
				reqBody = bodyBuf.String()
			}

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
