package sns_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// snsPost is a helper that sends a form-encoded SNS request to the handler.
func snsPost(t *testing.T, h *sns.Handler, form url.Values) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	body := form.Encode()
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// mustCreateTopic is a helper that creates a topic and returns its ARN.
func mustCreateTopic(t *testing.T, b *sns.InMemoryBackend, name string) string {
	t.Helper()

	topic, err := b.CreateTopic(name, nil)
	require.NoError(t, err)

	return topic.TopicArn
}

// mustSubscribe is a helper that creates a subscription and returns its ARN.
func mustSubscribe(t *testing.T, b *sns.InMemoryBackend, topicArn, protocol, endpoint string) string {
	t.Helper()

	sub, err := b.Subscribe(topicArn, protocol, endpoint, "")
	require.NoError(t, err)

	return sub.SubscriptionArn
}

// newTestHandler creates a handler and backend pair for testing.
func newTestHandler(t *testing.T) (*sns.Handler, *sns.InMemoryBackend) {
	t.Helper()
	b := sns.NewInMemoryBackend()

	return sns.NewHandler(b), b
}

// extractSNSHTTPMessage extracts the Message field from a standard SNS HTTP notification
// JSON envelope. If the body is not a JSON object with a "Message" field, the raw body is
// returned as-is. This allows tests to work correctly regardless of RawMessageDelivery setting.
func extractSNSHTTPMessage(body string) string {
	var env struct {
		Message string `json:"Message"`
		Type    string `json:"Type"`
	}
	if err := json.Unmarshal([]byte(body), &env); err == nil && env.Type == "Notification" {
		return env.Message
	}

	return body
}

func TestSNSHandler_Routing(t *testing.T) {
	t.Parallel()

	t.Run("unknown action", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		rec := snsPost(t, h, url.Values{
			"Action":  {"FakeAction"},
			"Version": {"2010-03-31"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
		assert.Contains(t, rec.Body.String(), "InvalidAction")
	})

	t.Run("parse form error", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodGet, "/", nil)
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		err := h.Handler()(c)
		require.NoError(t, err)
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})
}

func TestSNSHandler_RouteMatcher(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		body        string
		want        bool
	}{
		{
			name:        "SNS request",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateTopic&Version=2010-03-31&Name=test",
			want:        true,
		},
		{
			name:        "non-SNS request",
			contentType: "application/json",
			body:        `{"Action":"test"}`,
			want:        false,
		},
		{
			name:        "wrong version",
			contentType: "application/x-www-form-urlencoded",
			body:        "Action=CreateTopic&Version=2012-11-05&Name=test",
			want:        false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h, _ := newTestHandler(t)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(tt.body))
			req.Header.Set("Content-Type", tt.contentType)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			assert.Equal(t, tt.want, h.RouteMatcher()(c))
		})
	}
}

func TestSNSHandler_Introspection(t *testing.T) {
	t.Parallel()

	t.Run("Name", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		assert.Equal(t, "SNS", h.Name())
	})

	t.Run("MatchPriority", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		assert.Equal(t, 80, h.MatchPriority())
	})

	t.Run("GetSupportedOperations", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		ops := h.GetSupportedOperations()
		assert.Contains(t, ops, "CreateTopic")
		assert.Contains(t, ops, "Publish")
		assert.Contains(t, ops, "Subscribe")
	})

	t.Run("ExtractOperation", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=test"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "CreateTopic", h.ExtractOperation(c))
	})

	t.Run("ExtractOperation_EmptyBody", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(""))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "Unknown", h.ExtractOperation(c))
	})

	t.Run("ExtractResource_ByArn", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		body := "Action=DeleteTopic&Version=2010-03-31&TopicArn=arn:aws:sns:us-east-1:000000000000:my-topic"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "arn:aws:sns:us-east-1:000000000000:my-topic", h.ExtractResource(c))
	})

	t.Run("ExtractResource_ByName", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		body := "Action=CreateTopic&Version=2010-03-31&Name=my-topic"
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Equal(t, "my-topic", h.ExtractResource(c))
	})

	t.Run("ExtractResource_Empty", func(t *testing.T) {
		t.Parallel()
		h, _ := newTestHandler(t)
		e := echo.New()
		req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("Action=ListTopics"))
		req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
		rec := httptest.NewRecorder()
		c := e.NewContext(req, rec)
		assert.Empty(t, h.ExtractResource(c))
	})
}

// TestSNSHandler_Shutdown validates Handler.Shutdown behaviour: it must drain
// in-flight delivery goroutines when the context permits, and return promptly
// when the context is already cancelled.
func TestSNSHandler_Shutdown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		blockServer    bool // server blocks until unblocked by cleanup
		cancelCtxFirst bool // cancel the context before calling Shutdown
		wantFast       bool // Shutdown should return in <500 ms
		wantDelivered  bool // delivery must complete before Shutdown returns
	}{
		{
			name:          "drains_in_flight_delivery",
			blockServer:   false,
			wantDelivered: true,
		},
		{
			name:           "returns_promptly_on_cancelled_context",
			blockServer:    true,
			cancelCtxFirst: true,
			wantFast:       true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			delivered := make(chan struct{}, 1)
			unblock := make(chan struct{})

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				if tt.blockServer {
					<-unblock
				}
				delivered <- struct{}{}
				w.WriteHeader(http.StatusOK)
			}))
			defer func() {
				select {
				case <-unblock: // already closed
				default:
					close(unblock)
				}
				ts.Close()
			}()

			b := sns.NewInMemoryBackend()
			b.SetHTTPDeliveryClient(&http.Client{Timeout: 10 * time.Second})
			h := sns.NewHandler(b)

			tp, err := b.CreateTopic("shutdown-"+tt.name, nil)
			require.NoError(t, err)
			_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
			require.NoError(t, err)

			_, err = b.Publish(tp.TopicArn, "msg", "", "", nil)
			require.NoError(t, err)

			ctx, cancel := context.WithTimeout(t.Context(), 3*time.Second)
			defer cancel()

			if tt.cancelCtxFirst {
				cancel()
			}

			start := time.Now()
			h.Shutdown(ctx)
			elapsed := time.Since(start)

			if tt.wantFast {
				assert.Less(t, elapsed, 500*time.Millisecond,
					"Shutdown with cancelled context must return immediately")
			}

			if tt.wantDelivered {
				select {
				case <-delivered:
					// delivery completed before Shutdown returned
				default:
					require.FailNow(t, "delivery goroutine had not completed when Shutdown returned")
				}
			}
		})
	}
}
