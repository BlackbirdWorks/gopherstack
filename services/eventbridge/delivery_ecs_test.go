package eventbridge_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/require"
)

type auditECSTaskSink struct {
	invokes []string
	mu      sync.Mutex
}

func (s *auditECSTaskSink) RunTask(_ context.Context, clusterARN string, payload []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.invokes = append(s.invokes, clusterARN+":"+string(payload))

	return nil
}

func (s *auditECSTaskSink) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.invokes)
}

func TestDelivery_ECS_DeliversEvent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sink := &auditECSTaskSink{}
	clusterARN := "arn:aws:ecs:us-east-1:123456789012:cluster/my-cluster"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{ECS: sink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "ecs-rule",
		EventPattern: `{"source":["ecs-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "ecs-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: clusterARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "ecs-test", DetailType: "T", Detail: `{"task":"data"}`},
	})

	require.Eventually(t, func() bool {
		return sink.Count() > 0
	}, 2*time.Second, 10*time.Millisecond, "ECS should have been invoked")
}

func TestDelivery_ECS_NilHandlerSkipsGracefully(t *testing.T) {
	t.Parallel()
	b := newBackend()

	clusterARN := "arn:aws:ecs:us-east-1:123456789012:cluster/no-backend"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "ecs-nil-rule",
		EventPattern: `{"source":["nil-ecs"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "ecs-nil-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: clusterARN},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "nil-ecs", DetailType: "T", Detail: `{}`},
		})
	})
}

type auditFailingECSSink struct {
	delegate *auditECSTaskSink
	failARN  string
}

func (f *auditFailingECSSink) RunTask(ctx context.Context, clusterARN string, payload []byte) error {
	if clusterARN == f.failARN {
		return errSimulatedFailure
	}

	return f.delegate.RunTask(ctx, clusterARN, payload)
}

func TestDelivery_ECS_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newMockSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:ecs-dlq"
	clusterARN := "arn:aws:ecs:us-east-1:123456789012:cluster/failing-cluster"

	ecsDelegate := &auditECSTaskSink{}
	failingSink := &auditFailingECSSink{delegate: ecsDelegate, failARN: clusterARN}

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		ECS: failingSink,
		SQS: dlqSink,
	})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "ecs-dlq-rule",
		EventPattern: `{"source":["ecs-dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "ecs-dlq-rule", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              clusterARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
			RetryPolicy:      &eventbridge.RetryPolicy{MaximumRetryAttempts: 0},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "ecs-dlq-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should have received the failed ECS event")
}
