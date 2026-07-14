package eventbridge_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	errSimulatedPrimaryFailure = errors.New("simulated primary queue failure")
	errSimulatedFailure        = errors.New("simulated failure")
)

// auditFailingSQSSender fails for the primary target but succeeds for all others.
type auditFailingSQSSender struct {
	delegate eventbridge.SQSSender
	failARN  string
}

func (f *auditFailingSQSSender) SendMessageToQueue(ctx context.Context, queueARN, body string) error {
	if queueARN == f.failARN {
		return errSimulatedPrimaryFailure
	}

	return f.delegate.SendMessageToQueue(ctx, queueARN, body)
}

func TestDelivery_DLQCalledOnFailure(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newMockSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:my-dlq"
	targetARN := "arn:aws:sqs:us-east-1:123456789012:my-queue"

	sender := &auditFailingSQSSender{delegate: dlqSink, failARN: targetARN}
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sender})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "rule",
		EventPattern: `{"source":["dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "rule", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              targetARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
			RetryPolicy: &eventbridge.RetryPolicy{
				MaximumRetryAttempts: 0,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "dlq-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should have received the failed event")
}

// auditCountingSQSSender counts calls per queue and always fails delivery.
type auditCountingSQSSender struct {
	count map[string]int
	mu    sync.Mutex
}

func (c *auditCountingSQSSender) SendMessageToQueue(_ context.Context, queueARN, _ string) error {
	c.mu.Lock()
	c.count[queueARN]++
	c.mu.Unlock()

	return errSimulatedFailure
}

func (c *auditCountingSQSSender) CountFor(queueARN string) int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.count[queueARN]
}

func TestDelivery_RetryPolicyZeroAttemptsNeverRetries(t *testing.T) {
	t.Parallel()
	b := newBackend()

	counter := &auditCountingSQSSender{count: make(map[string]int)}
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: counter})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "retry-rule",
		EventPattern: `{"source":["retry-test"]}`,
	})
	require.NoError(t, err)

	targetARN := "arn:aws:sqs:us-east-1:123456789012:target-q"
	_, err = b.PutTargets(context.Background(), "retry-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: targetARN,
			RetryPolicy: &eventbridge.RetryPolicy{
				MaximumRetryAttempts: 0,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "retry-test", DetailType: "T", Detail: `{}`},
	})

	// Wait for delivery to complete (1 attempt only).
	require.Eventually(t, func() bool {
		return counter.CountFor(targetARN) >= 1
	}, 2*time.Second, 10*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	// With 0 retry attempts, should call exactly once.
	assert.Equal(t, 1, counter.CountFor(targetARN))
}

func TestDelivery_DefaultRetryAttempts(t *testing.T) {
	t.Parallel()
	b := newBackend()

	counter := &auditCountingSQSSender{count: make(map[string]int)}
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: counter})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "default-retry-rule",
		EventPattern: `{"source":["default-retry"]}`,
	})
	require.NoError(t, err)

	targetARN := "arn:aws:sqs:us-east-1:123456789012:target-q2"
	_, err = b.PutTargets(context.Background(), "default-retry-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: targetARN,
			// No RetryPolicy set → use defaults (2 retries = 3 total attempts).
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "default-retry", DetailType: "T", Detail: `{}`},
	})

	// Default 2 retries = 1 initial + 2 retries = 3 total attempts.
	require.Eventually(t, func() bool {
		return counter.CountFor(targetARN) >= 3
	}, 2*time.Second, 10*time.Millisecond)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 3, counter.CountFor(targetARN))
}

func TestCustomBus_DeliverToSQS(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
	const (
		busName  = "my-custom-bus"
		queueARN = "arn:aws:sqs:us-east-1:123456789012:custom-bus-queue"
		ruleName = "custom-rule"
	)

	_, err := b.CreateEventBus(context.Background(), busName, "")
	require.NoError(t, err)

	_, err = b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         ruleName,
		EventBusName: busName,
		EventPattern: `{"source": ["custom.src"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), ruleName, busName, []eventbridge.Target{
		{ID: "t1", Arn: queueARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "custom.src", DetailType: "Evt", Detail: `{"x":1}`, EventBusName: busName},
	})

	require.Eventually(t, func() bool {
		return len(sqsMock.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 20*time.Millisecond, "expected delivery to custom bus SQS target")
}

func TestInputTransformer_TemplateApplied(t *testing.T) {
	t.Parallel()

	sqsMock := newMockSQSSender()
	b := setupDeliveryBackend(t, sqsMock, newMockLambdaInvoker())
	const (
		queueARN = "arn:aws:sqs:us-east-1:123456789012:transformer-queue"
		ruleName = "transformer-rule"
	)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         ruleName,
		EventPattern: `{"source": ["svc"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), ruleName, "default", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{"env": "$.detail.env"},
				InputTemplate: `{"environment": "<env>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "svc", DetailType: "Evt", Detail: `{"env": "production"}`},
	})

	require.Eventually(t, func() bool {
		msgs := sqsMock.MessagesFor(queueARN)

		return len(msgs) > 0 && strings.Contains(msgs[0], "production")
	}, 2*time.Second, 20*time.Millisecond)
}

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

func TestDLQ_RoutedOnDeliveryFailure(t *testing.T) {
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
