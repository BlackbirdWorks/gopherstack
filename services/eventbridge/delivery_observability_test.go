package eventbridge_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/cwmetric"
	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

var errDLQDenied = errors.New("denied")

type metricRecorder struct {
	names map[string]float64
	mu    sync.Mutex
}

func (m *metricRecorder) EmitMetric(p cwmetric.Point) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.names[p.Name] += p.Value

	return nil
}

type attrSQS struct {
	*mockSQSSender
	attrs map[string]string
}

func (a *attrSQS) SendMessageWithAttributes(ctx context.Context, q, body string, attrs map[string]string) error {
	a.attrs = attrs

	return a.SendMessageToQueue(ctx, q, body)
}

type denyAuth struct{}

func (denyAuth) AuthorizeRole(_, _, _, _ string) error { return errDLQDenied }

func (denyAuth) AuthorizeServiceResource(_, _, _, _ string) error { return errDLQDenied }

func TestDeliveryDLQAttributesAndMetrics(t *testing.T) {
	t.Parallel()

	const (
		dlqARN    = "arn:aws:sqs:us-east-1:123456789012:dlq"
		lambdaARN = "arn:aws:lambda:us-east-1:123456789012:function:my-fn"
	)

	tests := []struct {
		name        string
		wantMetrics []string
		denyDLQ     bool
		wantSent    bool
	}{
		{
			name: "dlq_send", wantSent: true,
			wantMetrics: []string{
				"InvocationAttempts", "InvocationsSentToDlq", "IngestionToInvocationStartLatency",
				"IngestionToInvocationCompleteLatency", "PutEventsApproximateCallCount",
				"PutEventsApproximateSuccessCount", "PutEventsEntriesCount", "PutEventsRequestSize", "PutEventsLatency",
			},
		},
		{name: "dlq_denied_by_policy", denyDLQ: true, wantMetrics: []string{"InvocationsFailedToBeSentToDlq"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			synctest.Test(t, func(t *testing.T) {
				sqs := &attrSQS{mockSQSSender: newMockSQSSender()}
				rec := &metricRecorder{names: map[string]float64{}}
				backend := eventbridge.NewInMemoryBackend()
				backend.SetMetricEmitter(rec)

				targets := &eventbridge.DeliveryTargets{SQS: sqs, Lambda: &failingLambdaInvoker{}}
				if tt.denyDLQ {
					targets.RoleAuth = denyAuth{}
				}

				backend.SetDeliveryTargets(targets)

				_, err := backend.PutRule(t.Context(), eventbridge.PutRuleInput{
					Name: "r", EventPattern: `{"source": ["parity.test"]}`, State: "ENABLED",
				})
				require.NoError(t, err)

				_, err = backend.PutTargets(t.Context(), "r", "default", []eventbridge.Target{{
					ID: "t1", Arn: lambdaARN,
					RetryPolicy:      &eventbridge.RetryPolicy{MaximumRetryAttempts: 1},
					DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
				}})
				require.NoError(t, err)

				_, err = backend.PutEvents(t.Context(), []eventbridge.EventEntry{
					{Source: "parity.test", DetailType: "T", Detail: `{"k":"v"}`},
				})
				require.NoError(t, err)
				time.Sleep(time.Minute)
				synctest.Wait()

				rec.mu.Lock()
				defer rec.mu.Unlock()

				for _, name := range tt.wantMetrics {
					assert.Contains(t, rec.names, name)
				}

				if !tt.wantSent {
					assert.Empty(t, sqs.MessagesFor(dlqARN))

					return
				}

				assert.Equal(t, "ERROR_FROM_TARGET", sqs.attrs["ERROR_CODE"])
				assert.Equal(t, "MaximumRetryAttempts", sqs.attrs["EXHAUSTED_RETRY_CONDITION"])
				assert.Equal(t, "1", sqs.attrs["RETRY_ATTEMPTS"])
				assert.NotEmpty(t, sqs.attrs["ERROR_MESSAGE"])
				assert.InDelta(t, 1, rec.names["RetryInvocationAttempts"], 0)
				assert.InDelta(t, 2, rec.names["InvocationAttempts"], 0)
			})
		})
	}
}
