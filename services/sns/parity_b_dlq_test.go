package sns_test

// Tests for parity §B: SNS HTTP subscriptions send to DLQ on delivery failure (go-nace).

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

// mockSQSSender records calls to SendMessageToQueue.
type mockSQSSender struct {
	mu       sync.Mutex
	messages []sqsMessage
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

// TestParityB_SNS_DLQ_OnHTTPDeliveryFailure verifies that when an HTTP
// subscription endpoint returns a non-2xx status, the message body is forwarded
// to the configured DLQ via SQSSender.
func TestParityB_SNS_DLQ_OnHTTPDeliveryFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		serverStatus   int
		expectDLQ      bool
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

// TestParityB_SNS_DLQ_NoSQSSender_NoError verifies that when no SQSSender is
// configured, delivery failures do not panic or return errors.
func TestParityB_SNS_DLQ_NoSQSSender_NoError(t *testing.T) {
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
