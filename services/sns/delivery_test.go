package sns_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// TestSNSHTTPDelivery verifies Publish attempts HTTP delivery to http/https subscriptions.
func TestSNSHTTPDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("http-topic", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "test-message", "", "", nil)
	require.NoError(t, err)

	// Verify message was delivered
	select {
	case msg := <-received:
		assert.Equal(t, "test-message", extractSNSHTTPMessage(msg))
	case <-time.After(500 * time.Millisecond):
		require.FailNow(t, "HTTP delivery did not arrive in time")
	}
}

// TestSNSHTTPDeliveryBadEndpoint verifies that bad HTTP endpoints don't panic.
func TestSNSHTTPDeliveryBadEndpoint(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	tp, err := b.CreateTopic("http-err-topic", nil)
	require.NoError(t, err)
	_, err = b.Subscribe(tp.TopicArn, "http", "http://localhost:1", "")
	require.NoError(t, err)

	// Should not panic or return error; delivery is best-effort
	_, err = b.Publish(tp.TopicArn, "test", "", "", nil)
	require.NoError(t, err)
}

// TestSNSHTTPDelivery_Robustness is a table-driven test that validates delivery
// behaviour under various adverse conditions (large server response, all
// concurrency slots occupied by slow handlers).
func TestSNSHTTPDelivery_Robustness(t *testing.T) {
	t.Parallel()

	tests := []struct {
		serverFn    func() (http.Handler, <-chan string)
		name        string
		wantBody    string
		numSubs     int
		wantFastPub bool
	}{
		{
			name:    "large_response_bounded",
			numSubs: 1,
			serverFn: func() (http.Handler, <-chan string) {
				received := make(chan string, 1)
				large := bytes.Repeat([]byte("x"), 2*1024*1024)
				h := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					body, _ := io.ReadAll(r.Body)
					received <- string(body)
					w.WriteHeader(http.StatusOK)
					_, _ = w.Write(large)
				})

				return h, received
			},
			wantBody: "ping",
		},
		{
			name:    "non_blocking_under_load",
			numSubs: 12, // more than snsMaxConcurrentDeliveries (8)
			serverFn: func() (http.Handler, <-chan string) {
				// Sleep briefly so that goroutines are still in-flight when we
				// assert on elapsed time, but ensure the server completes well
				// before any test timeout. This avoids the deadlock that occurs
				// when httptest.Server.Close() (called via defer) waits for
				// handlers that are blocked indefinitely.
				h := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
					time.Sleep(200 * time.Millisecond)
					w.WriteHeader(http.StatusOK)
				})

				return h, nil
			},
			wantFastPub: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, received := tt.serverFn()
			ts := httptest.NewServer(h)
			defer ts.Close()

			b := sns.NewInMemoryBackend()
			b.SetHTTPDeliveryClient(&http.Client{Timeout: 5 * time.Second})
			tp, err := b.CreateTopic("robustness-"+tt.name, nil)
			require.NoError(t, err)

			for range tt.numSubs {
				_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
				require.NoError(t, err)
			}

			// Ensure goroutines are drained before the test exits to avoid leaks.
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				done := make(chan struct{})
				go func() { b.WaitDeliveries(); close(done) }()
				select {
				case <-done:
				case <-ctx.Done():
					t.Errorf("WaitDeliveries timed out; goroutines may have leaked")
				}
			})

			start := time.Now()
			_, err = b.Publish(tp.TopicArn, "ping", "", "", nil)
			require.NoError(t, err)
			elapsed := time.Since(start)

			if tt.wantFastPub {
				assert.Less(t, elapsed, 500*time.Millisecond,
					"Publish must not block waiting for slow HTTP endpoints")
			}

			if tt.wantBody != "" {
				select {
				case got := <-received:
					assert.Equal(t, tt.wantBody, extractSNSHTTPMessage(got))
				case <-time.After(3 * time.Second):
					require.FailNow(t, "timed out waiting for HTTP delivery")
				}
			}
		})
	}
}

// mockSQSSender records calls to SendMessageToQueue.
type mockSQSSender struct {
	messages []sqsMessage
	mu       sync.Mutex
}

type sqsMessage struct {
	QueueARN string
	Body     string
}

func (m *mockSQSSender) SendMessageToQueue(_ context.Context, queueARN, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.messages = append(m.messages, sqsMessage{QueueARN: queueARN, Body: body})

	return nil
}

func (m *mockSQSSender) Count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.messages)
}

func (m *mockSQSSender) Last() sqsMessage {
	m.mu.Lock()
	defer m.mu.Unlock()

	if len(m.messages) == 0 {
		return sqsMessage{}
	}

	return m.messages[len(m.messages)-1]
}

// TestHTTPDeliveryFailureSendsToDLQ verifies that when an HTTP
// subscription endpoint returns a non-2xx status, the message body is forwarded
// to the configured DLQ via SQSSender.
func TestHTTPDeliveryFailureSendsToDLQ(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		serverStatus int
		expectDLQ    bool
	}{
		{
			name:         "server_returns_200_no_dlq",
			serverStatus: http.StatusOK,
			expectDLQ:    false,
		},
		{
			name:         "server_returns_500_triggers_dlq",
			serverStatus: http.StatusInternalServerError,
			expectDLQ:    true,
		},
		{
			name:         "server_returns_404_triggers_dlq",
			serverStatus: http.StatusNotFound,
			expectDLQ:    true,
		},
	}

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:my-dlq"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			// HTTP server that returns the configured status.
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tt.serverStatus)
			}))
			t.Cleanup(srv.Close)

			sqsSender := &mockSQSSender{}
			b := sns.NewInMemoryBackend()
			b.SetSQSSender(sqsSender)
			b.SetHTTPDeliveryClient(srv.Client())

			topic, err := b.CreateTopic("dlq-test-topic", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, "http", srv.URL, "")
			require.NoError(t, err)

			// HTTP subscriptions start pending — confirm before delivery.
			_, err = b.ConfirmSubscription(topic.TopicArn, "any-token")
			require.NoError(t, err)

			redrivePolicy, _ := json.Marshal(map[string]string{
				"deadLetterTargetArn": dlqARN,
			})
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
			require.NoError(t, err)

			_, err = b.Publish(topic.TopicArn, "test message", "subject", "", nil)
			require.NoError(t, err)

			// HTTP delivery is async — wait for goroutines to finish.
			sns.WaitDeliveriesForTest(b)

			if tt.expectDLQ {
				assert.Equal(t, 1, sqsSender.Count(), "DLQ message must be sent on delivery failure")

				msg := sqsSender.Last()
				assert.Equal(t, dlqARN, msg.QueueARN, "DLQ ARN must match subscription redrivePolicy")
				assert.NotEmpty(t, msg.Body, "DLQ message body must not be empty")
			} else {
				assert.Equal(t, 0, sqsSender.Count(), "DLQ must not be called on successful delivery")
			}
		})
	}
}

// TestHTTPDeliveryFailureNoSQSSenderNoError verifies that when no SQSSender is
// configured, delivery failures do not panic or return errors.
func TestHTTPDeliveryFailureNoSQSSenderNoError(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(srv.Close)

	b := sns.NewInMemoryBackend()
	b.SetHTTPDeliveryClient(srv.Client())

	topic, err := b.CreateTopic("no-sender-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(topic.TopicArn, "http", srv.URL, "")
	require.NoError(t, err)

	_, err = b.ConfirmSubscription(topic.TopicArn, "any-token")
	require.NoError(t, err)

	_, err = b.Publish(topic.TopicArn, "msg", "", "", nil)
	require.NoError(t, err, "Publish must not error when no SQSSender is configured")
	sns.WaitDeliveriesForTest(b)
}

// TestGoroutineContextCancellationDrains verifies that goroutines waiting on
// the semaphore exit promptly when the context is cancelled rather than
// blocking indefinitely.
func TestGoroutineContextCancellationDrains(t *testing.T) {
	t.Parallel()

	// Use a slow HTTP server to keep delivery slots occupied so the extra goroutines
	// block on the semaphore, exercising the context-cancel escape path.
	slow := make(chan struct{})
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-slow:
		case <-r.Context().Done():
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	cancelCh := make(chan struct{})
	doneCh := make(chan struct{})

	b := sns.NewInMemoryBackendWithContext(t.Context(), "000000000000", "us-east-1")
	tp, err := b.CreateTopic("ctx-cancel-topic", nil)
	require.NoError(t, err)

	// Subscribe more endpoints than the concurrency cap (8) so goroutines block.
	for range 20 {
		_, err = b.Subscribe(tp.TopicArn, "http", ts.URL, "")
		require.NoError(t, err)
	}

	go func() {
		defer close(doneCh)
		_, _ = b.Publish(tp.TopicArn, "drain-test", "", "", nil)
	}()

	select {
	case <-doneCh:
	case <-time.After(2 * time.Second):
		t.Fatal("Publish did not return in time")
	}

	// Unblock the slow server so in-flight goroutines can complete.
	close(cancelCh)
	close(slow)

	// WaitDeliveries must return within a reasonable time.
	waitDone := make(chan struct{})
	go func() {
		b.WaitDeliveries()
		close(waitDone)
	}()

	select {
	case <-waitDone:
		// All delivery goroutines completed.
	case <-time.After(5 * time.Second):
		t.Fatal("WaitDeliveries did not return (possible goroutine leak)")
	}
}

// TestHTTPSSubscriptionDelivery verifies that HTTPS subscriptions are delivered
// the same way as HTTP ones.
func TestHTTPSSubscriptionDelivery(t *testing.T) {
	t.Parallel()

	received := make(chan string, 1)
	ts := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		received <- string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer ts.Close()

	b := newA1679Backend(t)
	b.SetHTTPDeliveryClient(ts.Client())

	tp, err := b.CreateTopic("https-topic", nil)
	require.NoError(t, err)

	_, err = b.Subscribe(tp.TopicArn, "https", ts.URL, "")
	require.NoError(t, err)

	_, err = b.Publish(tp.TopicArn, "secure-msg", "", "", nil)
	require.NoError(t, err)

	select {
	case raw := <-received:
		env := parseNotificationEnvelope(t, raw)
		assert.Equal(t, "secure-msg", env.Message)
		assert.NotEqual(t, "MOCK-SIGNATURE", env.Signature)
	case <-time.After(2 * time.Second):
		t.Fatal("HTTPS delivery did not arrive")
	}
}
