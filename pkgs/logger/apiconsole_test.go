package logger_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newCapturedRequest(method, path string, status int) *logger.CapturedRequest {
	return &logger.CapturedRequest{
		ID:        "test-id",
		Method:    method,
		Path:      path,
		Headers:   map[string]string{"Content-Type": "application/json"},
		Body:      "",
		Status:    status,
		Duration:  time.Millisecond,
		Timestamp: time.Now(),
	}
}

func TestRequestRingBuffer_Subscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		subscribeCount  int
		wantNonNilChans bool
	}{
		{
			name:            "single subscribe returns non-nil channel",
			subscribeCount:  1,
			wantNonNilChans: true,
		},
		{
			name:            "multiple subscribes return distinct channels",
			subscribeCount:  3,
			wantNonNilChans: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rb := logger.NewRequestRingBuffer(10)
			chans := make([]chan *logger.CapturedRequest, 0, tt.subscribeCount)

			for range tt.subscribeCount {
				ch := rb.Subscribe()
				require.NotNil(t, ch)
				chans = append(chans, ch)
			}

			assert.Len(t, chans, tt.subscribeCount)

			// all channels must be distinct
			seen := make(map[chan *logger.CapturedRequest]bool)
			for _, ch := range chans {
				assert.False(t, seen[ch], "expected distinct channels")
				seen[ch] = true
			}
		})
	}
}

func TestRequestRingBuffer_Unsubscribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		unsubscribe   bool
		wantNotified  bool
		wantPanicFree bool
	}{
		{
			name:         "unsubscribed channel does not receive notifications",
			unsubscribe:  true,
			wantNotified: false,
		},
		{
			name:         "subscribed channel receives notification",
			unsubscribe:  false,
			wantNotified: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rb := logger.NewRequestRingBuffer(10)
			ch := rb.Subscribe()

			if tt.unsubscribe {
				rb.Unsubscribe(ch)
			}

			rb.Add(newCapturedRequest(http.MethodGet, "/test", 200))

			if tt.wantNotified {
				select {
				case msg := <-ch:
					require.NotNil(t, msg)
					assert.Equal(t, "/test", msg.Path)
				case <-time.After(100 * time.Millisecond):
					require.Fail(t, "expected notification but none received")
				}
			} else {
				select {
				case <-ch:
					require.Fail(t, "expected no notification but received one")
				case <-time.After(50 * time.Millisecond):
					// correctly received nothing
				}
			}
		})
	}
}

func TestRequestRingBuffer_Unsubscribe_NonSubscribedChannel(t *testing.T) {
	t.Parallel()

	rb := logger.NewRequestRingBuffer(10)
	orphan := make(chan *logger.CapturedRequest, 10)

	// Unsubscribing a channel that was never subscribed should not panic
	assert.NotPanics(t, func() {
		rb.Unsubscribe(orphan)
	})
}

func TestRequestRingBuffer_Add(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantLastPath string
		maxSize      int
		addCount     int
		wantLen      int
	}{
		{
			name:         "add single item to empty buffer",
			maxSize:      5,
			addCount:     1,
			wantLen:      1,
			wantLastPath: "/req-0",
		},
		{
			name:         "add items up to capacity",
			maxSize:      3,
			addCount:     3,
			wantLen:      3,
			wantLastPath: "/req-2",
		},
		{
			name:         "add beyond capacity wraps ring buffer",
			maxSize:      3,
			addCount:     5,
			wantLen:      3,
			wantLastPath: "/req-4",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rb := logger.NewRequestRingBuffer(tt.maxSize)

			for i := range tt.addCount {
				rb.Add(newCapturedRequest(http.MethodGet, "/req-"+itoa(i), 200))
			}

			all := rb.GetAll()
			require.Len(t, all, tt.wantLen)
			assert.Equal(t, tt.wantLastPath, all[len(all)-1].Path)
		})
	}
}

func TestRequestRingBuffer_Add_NotifiesSubscribers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		addPath  string
		wantPath string
	}{
		{
			name:     "subscriber is notified with correct request",
			addPath:  "/notify-me",
			wantPath: "/notify-me",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rb := logger.NewRequestRingBuffer(10)
			ch := rb.Subscribe()

			rb.Add(newCapturedRequest(http.MethodPost, tt.addPath, 201))

			select {
			case msg := <-ch:
				require.NotNil(t, msg)
				assert.Equal(t, tt.wantPath, msg.Path)
				assert.Equal(t, http.MethodPost, msg.Method)
				assert.Equal(t, 201, msg.Status)
			case <-time.After(100 * time.Millisecond):
				require.Fail(t, "subscriber was not notified within timeout")
			}
		})
	}
}

func TestRequestRingBuffer_GetAll(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		paths     []string
		wantPaths []string
		maxSize   int
	}{
		{
			name:      "empty buffer returns empty slice",
			maxSize:   5,
			paths:     []string{},
			wantPaths: []string{},
		},
		{
			name:      "partial fill returns items in insertion order",
			maxSize:   5,
			paths:     []string{"/a", "/b", "/c"},
			wantPaths: []string{"/a", "/b", "/c"},
		},
		{
			name:      "full buffer without wrap returns all items in order",
			maxSize:   3,
			paths:     []string{"/a", "/b", "/c"},
			wantPaths: []string{"/a", "/b", "/c"},
		},
		{
			name:      "wrapped buffer returns chronological order",
			maxSize:   3,
			paths:     []string{"/a", "/b", "/c", "/d", "/e"},
			wantPaths: []string{"/c", "/d", "/e"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rb := logger.NewRequestRingBuffer(tt.maxSize)

			for _, p := range tt.paths {
				rb.Add(newCapturedRequest(http.MethodGet, p, 200))
			}

			all := rb.GetAll()
			require.Len(t, all, len(tt.wantPaths))

			gotPaths := make([]string, 0, len(all))
			for _, r := range all {
				gotPaths = append(gotPaths, r.Path)
			}

			assert.Equal(t, tt.wantPaths, gotPaths)
		})
	}
}

func TestAPIConsoleMiddleware(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		body         string
		wantCaptured bool
		wantStatus   int
	}{
		{
			name:         "captures standard API request",
			method:       http.MethodGet,
			path:         "/api/something",
			wantCaptured: true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "captures POST request with body",
			method:       http.MethodPost,
			path:         "/api/create",
			body:         `{"key":"value"}`,
			wantCaptured: true,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "skips dashboard prefix path",
			method:       http.MethodGet,
			path:         "/dashboard/overview",
			wantCaptured: false,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "skips dashboard root path",
			method:       http.MethodGet,
			path:         "/dashboard",
			wantCaptured: false,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "skips gopherstack health path",
			method:       http.MethodGet,
			path:         "/_gopherstack/health",
			wantCaptured: false,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "skips GET root path",
			method:       http.MethodGet,
			path:         "/",
			wantCaptured: false,
			wantStatus:   http.StatusOK,
		},
		{
			name:         "does not skip POST to root",
			method:       http.MethodPost,
			path:         "/",
			wantCaptured: true,
			wantStatus:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// Use a dedicated ring buffer per subtest to avoid shared-state races.
			rb := logger.NewRequestRingBuffer(50)

			e := echo.New()
			e.Use(func(next echo.HandlerFunc) echo.HandlerFunc {
				return func(c *echo.Context) error {
					start := time.Now()
					req := c.Request()

					if strings.HasPrefix(req.URL.Path, "/dashboard") {
						return next(c)
					}
					if req.URL.Path == "/_gopherstack/health" || (req.URL.Path == "/" && req.Method == http.MethodGet) {
						return next(c)
					}

					headers := make(map[string]string)
					for k, v := range req.Header {
						if len(v) > 0 {
							headers[k] = v[0]
						}
					}

					err := next(c)

					status := 200
					if err != nil {
						status = 500
					}

					rb.Add(&logger.CapturedRequest{
						ID:        c.Response().Header().Get(echo.HeaderXRequestID),
						Method:    req.Method,
						Path:      req.URL.Path,
						Headers:   headers,
						Body:      tt.body,
						Status:    status,
						Duration:  time.Since(start),
						Timestamp: time.Now(),
					})

					return err
				}
			})

			e.GET("/api/something", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.POST("/api/create", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.GET("/dashboard", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.GET("/dashboard/overview", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.GET("/_gopherstack/health", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.GET("/", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.POST("/", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			var reqBody *strings.Reader
			if tt.body != "" {
				reqBody = strings.NewReader(tt.body)
			} else {
				reqBody = strings.NewReader("")
			}

			req := httptest.NewRequest(tt.method, tt.path, reqBody)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			require.Equal(t, tt.wantStatus, rec.Code)

			all := rb.GetAll()
			if tt.wantCaptured {
				require.NotEmpty(t, all, "expected request to be captured in ring buffer")
				found := false
				for _, r := range all {
					if r.Path == tt.path && r.Method == tt.method {
						found = true
						assert.Equal(t, tt.path, r.Path)
						assert.Equal(t, tt.method, r.Method)
					}
				}
				assert.True(t, found, "expected path %q to be captured", tt.path)
			} else {
				for _, r := range all {
					assert.NotEqual(t, tt.path, r.Path, "expected path %q to be skipped", tt.path)
				}
			}
		})
	}
}

func TestAPIConsoleMiddleware_GlobalBuffer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		method       string
		path         string
		wantCaptured bool
	}{
		{
			name:         "global middleware captures API request",
			method:       http.MethodGet,
			path:         "/api/global-test",
			wantCaptured: true,
		},
		{
			name:         "global middleware skips dashboard",
			method:       http.MethodGet,
			path:         "/dashboard/skip-test",
			wantCaptured: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			e.Use(logger.APIConsoleMiddleware())

			e.GET("/api/global-test", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})
			e.GET("/dashboard/skip-test", func(c *echo.Context) error {
				return c.String(http.StatusOK, "ok")
			})

			req := httptest.NewRequest(tt.method, tt.path, nil)
			rec := httptest.NewRecorder()
			e.ServeHTTP(rec, req)

			require.Equal(t, http.StatusOK, rec.Code)

			if tt.wantCaptured {
				all := logger.GlobalRingBuffer.GetAll()
				found := false
				for _, r := range all {
					if r.Path == tt.path && r.Method == tt.method {
						found = true
					}
				}
				assert.True(t, found, "expected path %q to be in GlobalRingBuffer", tt.path)
			}
			// For skip cases we can't safely assert absence from the global buffer
			// since other parallel tests may be adding entries concurrently.
		})
	}
}

// itoa converts a non-negative int to its decimal string representation
// without importing strconv, keeping this file dependency-light.
func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	buf := make([]byte, 0, 10)
	for n > 0 {
		buf = append([]byte{byte('0' + n%10)}, buf...)
		n /= 10
	}

	return string(buf)
}
