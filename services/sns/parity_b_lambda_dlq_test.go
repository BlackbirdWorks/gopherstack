package sns_test

// Tests for parity §B: SNS Lambda subscriptions send to DLQ on delivery failure.

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/sns"
)

var errSimulatedFailure = errors.New("simulated invocation failure")

// failingLambdaInvoker always returns an error from InvokeFunction.
type failingLambdaInvoker struct{}

func (f *failingLambdaInvoker) InvokeFunction(
	_ context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	return nil, 500, errSimulatedFailure
}

// successLambdaInvoker always succeeds.
type successLambdaInvoker struct{}

func (s *successLambdaInvoker) InvokeFunction(
	_ context.Context, _, _ string, _ []byte,
) ([]byte, int, error) {
	return []byte(`"ok"`), 200, nil
}

// mockLambdaDLQSender records SQS messages sent.
type mockLambdaDLQSender struct {
	messages []string // before mu to minimize GC pointer scan span
	mu       sync.Mutex
}

func (m *mockLambdaDLQSender) SendMessageToQueue(_ context.Context, _ string, body string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, body)

	return nil
}

func (m *mockLambdaDLQSender) count() int {
	m.mu.Lock()
	defer m.mu.Unlock()

	return len(m.messages)
}

func TestParityB_SNS_Lambda_DLQ_OnDeliveryFailure(t *testing.T) {
	t.Parallel()

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:lambda-dlq"

	tests := []struct {
		invoker   sns.LambdaInvoker
		name      string
		expectDLQ bool
	}{
		{
			name:      "lambda_success_no_dlq",
			invoker:   &successLambdaInvoker{},
			expectDLQ: false,
		},
		{
			name:      "lambda_failure_triggers_dlq",
			invoker:   &failingLambdaInvoker{},
			expectDLQ: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsSender := &mockLambdaDLQSender{}
			b := sns.NewInMemoryBackend()
			b.SetLambdaBackend(tt.invoker)
			b.SetSQSSender(sqsSender)

			topic, err := b.CreateTopic("lambda-dlq-test", nil)
			require.NoError(t, err)

			sub, err := b.Subscribe(topic.TopicArn, "lambda", "arn:aws:lambda:us-east-1:123:function:my-fn", "")
			require.NoError(t, err)

			redrivePolicy, _ := json.Marshal(map[string]string{
				"deadLetterTargetArn": dlqARN,
			})
			err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
			require.NoError(t, err)

			_, err = b.Publish(topic.TopicArn, "test-message", "subject", "", nil)
			require.NoError(t, err)

			sns.WaitDeliveriesForTest(b)

			if tt.expectDLQ {
				assert.Equal(t, 1, sqsSender.count(), "DLQ must receive message on Lambda failure")
			} else {
				assert.Equal(t, 0, sqsSender.count(), "DLQ must not be called on Lambda success")
			}
		})
	}
}

// TestParityB_SNS_Lambda_DLQ_NoSender verifies no panic when no SQSSender configured.
func TestParityB_SNS_Lambda_DLQ_NoSender(t *testing.T) {
	t.Parallel()

	b := sns.NewInMemoryBackend()
	b.SetLambdaBackend(&failingLambdaInvoker{})

	topic, err := b.CreateTopic("no-sender-topic", nil)
	require.NoError(t, err)

	sub, err := b.Subscribe(topic.TopicArn, "lambda", "arn:aws:lambda:us-east-1:123:function:fn", "")
	require.NoError(t, err)

	redrivePolicy, _ := json.Marshal(map[string]string{
		"deadLetterTargetArn": "arn:aws:sqs:us-east-1:123456789012:dlq",
	})
	err = b.SetSubscriptionAttributes(sub.SubscriptionArn, "RedrivePolicy", string(redrivePolicy))
	require.NoError(t, err)

	_, err = b.Publish(topic.TopicArn, "msg", "", "", nil)
	require.NoError(t, err, "must not error when SQSSender absent")
	sns.WaitDeliveriesForTest(b)
}
