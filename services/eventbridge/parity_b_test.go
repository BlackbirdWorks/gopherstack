package eventbridge_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

var errSimulatedLambdaFailure = errors.New("simulated lambda invocation failure")

// failingLambdaInvoker always returns an error from InvokeFunction.
type failingLambdaInvoker struct{}

func (f *failingLambdaInvoker) InvokeFunction(
	_ context.Context,
	_, _ string,
	_ []byte,
) ([]byte, int, error) {
	return nil, 500, errSimulatedLambdaFailure
}

func TestParity_DLQ_RoutedOnDeliveryFailure(t *testing.T) {
	t.Parallel()

	const (
		dlqARN    = "arn:aws:sqs:us-east-1:123456789012:dlq"
		lambdaARN = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
	)

	tests := []struct {
		name      string
		dlqARN    string
		wantInDLQ bool
	}{
		{
			name:      "failed_delivery_routes_to_dlq",
			dlqARN:    dlqARN,
			wantInDLQ: true,
		},
		{
			name:      "failed_delivery_no_dlq_silent",
			dlqARN:    "",
			wantInDLQ: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sqsMock := newMockSQSSender()
			backend := eventbridge.NewInMemoryBackend()
			backend.SetDeliveryTargets(&eventbridge.DeliveryTargets{
				SQS:    sqsMock,
				Lambda: &failingLambdaInvoker{},
			})

			_, err := backend.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:         "dlq-rule-" + tt.name,
				EventPattern: `{"source": ["parity.test"]}`,
				State:        "ENABLED",
			})
			require.NoError(t, err)

			target := eventbridge.Target{
				ID:  "t1",
				Arn: lambdaARN,
				RetryPolicy: &eventbridge.RetryPolicy{
					MaximumRetryAttempts: 0,
				},
			}
			if tt.dlqARN != "" {
				target.DeadLetterConfig = &eventbridge.DeadLetterConfig{Arn: tt.dlqARN}
			}

			_, err = backend.PutTargets(
				context.Background(),
				"dlq-rule-"+tt.name,
				"default",
				[]eventbridge.Target{target},
			)
			require.NoError(t, err)

			backend.PutEvents(context.Background(), []eventbridge.EventEntry{
				{Source: "parity.test", DetailType: "TestEvent", Detail: `{"key": "val"}`},
			})

			if tt.wantInDLQ {
				require.Eventually(t, func() bool {
					return len(sqsMock.MessagesFor(tt.dlqARN)) > 0
				}, 2*time.Second, 10*time.Millisecond, "DLQ should receive the failed event")

				msgs := sqsMock.MessagesFor(tt.dlqARN)
				assert.NotEmpty(t, msgs)
			} else {
				time.Sleep(150 * time.Millisecond)
				// No DLQ configured — nothing should be sent anywhere.
				assert.Empty(t, sqsMock.MessagesFor(dlqARN))
			}
		})
	}
}
