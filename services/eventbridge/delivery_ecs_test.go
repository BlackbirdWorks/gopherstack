package eventbridge_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
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

// auditECSParamsSink implements ECSTaskRunnerWithParams (in addition to the
// base RunTask, matching the optional-capability pattern) to record whatever
// EcsParameters delivery threads through, without depending on the payload
// carrying a "TaskDefinition" key.
type auditECSParamsSink struct {
	lastParams *eventbridge.EcsParameters
	lastArn    string
	mu         sync.Mutex
	calls      int
}

func (s *auditECSParamsSink) RunTask(_ context.Context, _ string, _ []byte) error {
	return nil
}

func (s *auditECSParamsSink) RunTaskWithParams(
	_ context.Context, clusterARN string, params *eventbridge.EcsParameters, _ []byte,
) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.calls++
	s.lastArn = clusterARN
	s.lastParams = params

	return nil
}

func (s *auditECSParamsSink) snapshot() (int, string, *eventbridge.EcsParameters) {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.calls, s.lastArn, s.lastParams
}

func TestDelivery_ECS_ParamsThreading(t *testing.T) {
	t.Parallel()

	tests := []struct {
		sink eventbridge.ECSTaskRunner
		name string
	}{
		{name: "params_capable_sink_receives_ecs_parameters", sink: &auditECSParamsSink{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			clusterARN := "arn:aws:ecs:us-east-1:123456789012:cluster/params-cluster"
			b.SetDeliveryTargets(&eventbridge.DeliveryTargets{ECS: tt.sink})

			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:         "ecs-params-rule",
				EventPattern: `{"source":["ecs-params-test"]}`,
			})
			require.NoError(t, err)

			_, err = b.PutTargets(context.Background(), "ecs-params-rule", "", []eventbridge.Target{
				{
					ID:  "t1",
					Arn: clusterARN,
					EcsParameters: &eventbridge.EcsParameters{
						TaskDefinitionArn: "arn:aws:ecs:us-east-1:123456789012:task-definition/my-task:3",
						LaunchType:        "FARGATE",
						TaskCount:         2,
						NetworkConfiguration: &eventbridge.NetworkConfiguration{
							AwsvpcConfiguration: &eventbridge.AwsVpcConfiguration{
								Subnets:        []string{"subnet-abc"},
								AssignPublicIP: "ENABLED",
							},
						},
					},
				},
			})
			require.NoError(t, err)

			b.PutEvents(context.Background(), []eventbridge.EventEntry{
				{Source: "ecs-params-test", DetailType: "T", Detail: `{}`},
			})

			sink, ok := tt.sink.(*auditECSParamsSink)
			require.True(t, ok)

			require.Eventually(t, func() bool {
				calls, _, _ := sink.snapshot()

				return calls > 0
			}, 2*time.Second, 10*time.Millisecond, "ECSTaskRunnerWithParams should have been invoked")

			_, arn, params := sink.snapshot()
			assert.Equal(t, clusterARN, arn)
			require.NotNil(t, params)
			assert.Equal(t, "arn:aws:ecs:us-east-1:123456789012:task-definition/my-task:3", params.TaskDefinitionArn)
			assert.Equal(t, "FARGATE", params.LaunchType)
			assert.EqualValues(t, 2, params.TaskCount)
			require.NotNil(t, params.NetworkConfiguration)
			require.NotNil(t, params.NetworkConfiguration.AwsvpcConfiguration)
			assert.Equal(t, []string{"subnet-abc"}, params.NetworkConfiguration.AwsvpcConfiguration.Subnets)
		})
	}
}

// TestDelivery_ECS_LegacySinkStillWorksWithoutParams locks in that an
// ECSTaskRunner implementation which does NOT implement ECSTaskRunnerWithParams
// (e.g. an older adapter) still receives delivery via the base RunTask method
// -- the optional-capability probe in deliverToECS must never regress a
// legacy sink into being skipped.
func TestDelivery_ECS_LegacySinkStillWorksWithoutParams(t *testing.T) {
	t.Parallel()

	b := newBackend()
	sink := &auditECSTaskSink{}
	clusterARN := "arn:aws:ecs:us-east-1:123456789012:cluster/legacy-cluster"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{ECS: sink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "ecs-legacy-rule",
		EventPattern: `{"source":["ecs-legacy-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "ecs-legacy-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: clusterARN,
			EcsParameters: &eventbridge.EcsParameters{
				TaskDefinitionArn: "arn:aws:ecs:us-east-1:123456789012:task-definition/legacy:1",
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "ecs-legacy-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return sink.Count() > 0
	}, 2*time.Second, 10*time.Millisecond, "legacy RunTask-only sink should still be invoked")
}
