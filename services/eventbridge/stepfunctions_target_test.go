package eventbridge_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

var errExecutionLimitReached = errors.New("execution limit reached")

// auditSFNExecutor records StartExecution calls for assertion.
type auditSFNExecutor struct {
	returnErr  error
	executions []sfnExecution
	mu         sync.Mutex
}

type sfnExecution struct {
	StateMachineARN string
	Name            string
	Input           string
}

func (s *auditSFNExecutor) StartExecution(stateMachineARN, name, input string) error {
	if s.returnErr != nil {
		return s.returnErr
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.executions = append(s.executions, sfnExecution{
		StateMachineARN: stateMachineARN,
		Name:            name,
		Input:           input,
	})

	return nil
}

func (s *auditSFNExecutor) Count() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.executions)
}

func (s *auditSFNExecutor) LastExecution() sfnExecution {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.executions) == 0 {
		return sfnExecution{}
	}

	return s.executions[len(s.executions)-1]
}

func TestAudit_Delivery_StepFunctions_DeliversEvent(t *testing.T) {
	t.Parallel()

	b := newBackend()
	sfn := &auditSFNExecutor{}
	smARN := "arn:aws:states:us-east-1:123456789012:stateMachine:my-sm"

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{StepFunctions: sfn})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "sfn-rule",
		EventPattern: `{"source":["sfn-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "sfn-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: smARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "sfn-test", DetailType: "Order", Detail: `{"id":42}`},
	})

	require.Eventually(t, func() bool {
		return sfn.Count() > 0
	}, 2*time.Second, 10*time.Millisecond, "Step Functions should have been invoked")

	exec := sfn.LastExecution()
	assert.Equal(t, smARN, exec.StateMachineARN)
	assert.NotEmpty(t, exec.Input)
}

func TestAudit_Delivery_StepFunctions_NilHandlerSkipsGracefully(t *testing.T) {
	t.Parallel()

	b := newBackend()
	smARN := "arn:aws:states:us-east-1:123456789012:stateMachine:no-backend"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "sfn-nil-rule",
		EventPattern: `{"source":["nil-sfn"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "sfn-nil-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: smARN},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "nil-sfn", DetailType: "T", Detail: `{}`},
		})
	})
}

func TestAudit_Delivery_StepFunctions_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()

	b := newBackend()

	dlqSink := newAuditSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:sfn-dlq"
	smARN := "arn:aws:states:us-east-1:123456789012:stateMachine:failing-sm"

	sfnSink := &auditSFNExecutor{returnErr: errExecutionLimitReached}

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		StepFunctions: sfnSink,
		SQS:           dlqSink,
	})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "sfn-fail-rule",
		EventPattern: `{"source":["sfn-fail"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "sfn-fail-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: smARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{
				Arn: dlqARN,
			},
			RetryPolicy: &eventbridge.RetryPolicy{MaximumRetryAttempts: 0},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "sfn-fail", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should receive failed SFN delivery")
}

func TestAudit_Delivery_IsStateMachineARN(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		arn  string
		want bool
	}{
		{"state_machine_arn", "arn:aws:states:us-east-1:123456789012:stateMachine:my-sm", true},
		{"ecs_arn", "arn:aws:ecs:us-east-1:123456789012:cluster/my-cluster", false},
		{"lambda_arn", "arn:aws:lambda:us-east-1:123456789012:function:my-fn", false},
		{"sqs_arn", "arn:aws:sqs:us-east-1:123456789012:my-queue", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := newBackend()
			sfn := &auditSFNExecutor{}
			b.SetDeliveryTargets(&eventbridge.DeliveryTargets{StepFunctions: sfn})

			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:         "arn-test-" + tt.name,
				EventPattern: `{"source":["arn-probe-` + tt.name + `"]}`,
			})
			require.NoError(t, err)

			_, err = b.PutTargets(context.Background(), "arn-test-"+tt.name, "", []eventbridge.Target{
				{ID: "t1", Arn: tt.arn},
			})
			require.NoError(t, err)

			b.PutEvents(context.Background(), []eventbridge.EventEntry{
				{Source: "arn-probe-" + tt.name, DetailType: "T", Detail: `{}`},
			})

			time.Sleep(50 * time.Millisecond)

			if tt.want {
				assert.Positive(t, sfn.Count(), "expected SFN invocation for ARN %s", tt.arn)
			} else {
				assert.Equal(t, 0, sfn.Count(), "expected no SFN invocation for non-SM ARN %s", tt.arn)
			}
		})
	}
}

// Ensure auditSFNExecutor satisfies the interface (compile-time check).
var _ eventbridge.StepFunctionsExecutor = (*auditSFNExecutor)(nil)

// Suppress unused warning for the context import.
var _ = context.Background
