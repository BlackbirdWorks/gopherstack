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
//nolint:gocognit // inherent complexity: captures headers, body, response status across middlewares
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

			// Capture body if present
			var reqBody string
			if req.Body != nil {
				bodyBytes, _ := io.ReadAll(req.Body)
				req.Body = io.NopCloser(bytes.NewBuffer(bodyBytes)) // restore
				if len(bodyBytes) > 0 {
					reqBody = string(bodyBytes)
				}
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
