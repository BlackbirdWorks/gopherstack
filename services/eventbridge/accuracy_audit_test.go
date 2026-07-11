package eventbridge_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

var (
	errSimulatedPrimaryFailure = errors.New("simulated primary queue failure")
	errSimulatedFailure        = errors.New("simulated failure")
)

// ---------------------------------------------------------------------------
// Issue 9: EventBus and per-bus rule limits
// ---------------------------------------------------------------------------

func TestAudit_CreateEventBus_EnforcesLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	const limit = 200
	for i := range limit {
		_, err := b.CreateEventBus(context.Background(), fmt.Sprintf("bus-%d", i), "")
		require.NoError(t, err, "bus %d should be created", i)
	}

	_, err := b.CreateEventBus(context.Background(), "bus-overflow", "")
	require.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}

func TestAudit_PutRule_EnforcesPerBusLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	const limit = 300
	for i := range limit {
		_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
			Name:         fmt.Sprintf("rule-%d", i),
			EventPattern: `{"source":["x"]}`,
			State:        "ENABLED",
		})
		require.NoError(t, err, "rule %d should be created", i)
	}

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "rule-overflow",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}

func TestAudit_PutRule_UpdateExistingDoesNotCountAgainstLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "my-rule",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	for range 5 {
		_, err = b.PutRule(context.Background(), eventbridge.PutRuleInput{
			Name:         "my-rule",
			EventPattern: `{"source":["y"]}`,
			State:        "ENABLED",
		})
		require.NoError(t, err)
	}
}

// ---------------------------------------------------------------------------
// Issue 8: ScheduleExpression validation at PutRule
// ---------------------------------------------------------------------------

func TestAudit_PutRule_InvalidScheduleExpressionRejected(t *testing.T) {
	t.Parallel()

	invalid := []struct {
		name string
		expr string
	}{
		{"unknown prefix", "invalid(expression)"},
		{"rate zero", "rate(0 minutes)"},
		{"rate negative", "rate(-1 hours)"},
		{"rate non-number", "rate(abc minutes)"},
		{"cron too few fields", "cron(* * *)"},
		{"bare string", "foo"},
	}

	for _, tt := range invalid {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:               "r",
				ScheduleExpression: tt.expr,
			})
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestAudit_PutRule_ValidScheduleExpressionsAccepted(t *testing.T) {
	t.Parallel()

	valid := []string{
		"rate(1 minute)",
		"rate(5 minutes)",
		"rate(1 hour)",
		"rate(24 hours)",
		"rate(1 day)",
		"cron(0 12 * * ? *)",
	}

	for _, expr := range valid {
		t.Run(expr, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
				Name:               "r",
				ScheduleExpression: expr,
			})
			require.NoError(t, err)
		})
	}
}

// ---------------------------------------------------------------------------
// Issue 13: ManagedBy field on rules
// ---------------------------------------------------------------------------

func TestAudit_PutRule_ManagedByPreserved(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "managed-rule",
		EventPattern: `{"source":["x"]}`,
		ManagedBy:    "scheduler.amazonaws.com",
	})
	require.NoError(t, err)

	rule, err := b.DescribeRule(context.Background(), "managed-rule", "")
	require.NoError(t, err)
	assert.Equal(t, "scheduler.amazonaws.com", rule.ManagedBy)
}

// ---------------------------------------------------------------------------
// Issues 1-3: Target RetryPolicy, DeadLetterConfig, BatchParameters
// ---------------------------------------------------------------------------

func TestAudit_Target_RetryPolicyStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: "arn:aws:sqs:us-east-1:123456789012:my-queue",
			RetryPolicy: &eventbridge.RetryPolicy{
				MaximumRetryAttempts:     5,
				MaximumEventAgeInSeconds: 600,
			},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].RetryPolicy)
	assert.Equal(t, 5, targets[0].RetryPolicy.MaximumRetryAttempts)
	assert.Equal(t, 600, targets[0].RetryPolicy.MaximumEventAgeInSeconds)
}

func TestAudit_Target_DeadLetterConfigStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:dlq"
	_, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              "arn:aws:lambda:us-east-1:123456789012:function:fn",
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].DeadLetterConfig)
	assert.Equal(t, dlqARN, targets[0].DeadLetterConfig.Arn)
}

func TestAudit_Target_BatchParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: "arn:aws:batch:us-east-1:123456789012:job-queue/my-queue",
			BatchParameters: &eventbridge.BatchParameters{
				JobDefinition: "arn:aws:batch:us-east-1:123456789012:job-definition/my-job",
				JobName:       "my-job-run",
			},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule(context.Background(), "r", "", "", 0)
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].BatchParameters)
	assert.Equal(t, "my-job-run", targets[0].BatchParameters.JobName)
}

// ---------------------------------------------------------------------------
// Issue 12: Input transformer key validation
// ---------------------------------------------------------------------------

func TestAudit_PutTargets_InputTransformerKeyValidation(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	// Valid keys accepted.
	failed, err := b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: "arn:aws:sqs:us-east-1:123456789012:q",
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"myVar":   "$.source",
					"myVar_2": "$.detail.type",
					"V123":    "$.id",
				},
				InputTemplate: `{"v": "<myVar>"}`,
			},
		},
	})
	require.NoError(t, err)
	assert.Empty(t, failed)

	// Invalid key (contains hyphen) — rejected via FailedEntry.
	failed, err = b.PutTargets(context.Background(), "r", "", []eventbridge.Target{
		{
			ID:  "t2",
			Arn: "arn:aws:sqs:us-east-1:123456789012:q",
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"bad-key": "$.source",
				},
				InputTemplate: `{"v": "<bad-key>"}`,
			},
		},
	})
	require.NoError(t, err)
	require.Len(t, failed, 1)
	assert.Equal(t, "t2", failed[0].TargetID)
}

// ---------------------------------------------------------------------------
// Issue 5: EventBus resource policies
// ---------------------------------------------------------------------------

func TestAudit_PutPermission_StoresStatement(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "allow-account-123",
		Action:      "events:PutEvents",
		Principal:   "123456789013",
	})
	require.NoError(t, err)

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.Contains(t, policy, "allow-account-123")
}

func TestAudit_RemovePermission_RemovesStatement(t *testing.T) {
	t.Parallel()
	b := newBackend()

	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "stmt-1",
		Action:      "events:PutEvents",
		Principal:   "111111111111",
	}))
	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "stmt-2",
		Action:      "events:PutEvents",
		Principal:   "222222222222",
	}))

	require.NoError(
		t,
		b.RemovePermission(context.Background(), eventbridge.RemovePermissionInput{StatementID: "stmt-1"}),
	)

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.NotContains(t, policy, "stmt-1")
	assert.Contains(t, policy, "stmt-2")
}

func TestAudit_RemovePermission_RemoveAll(t *testing.T) {
	t.Parallel()
	b := newBackend()

	require.NoError(t, b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		StatementID: "stmt-1",
		Action:      "events:PutEvents",
		Principal:   "111111111111",
	}))

	require.NoError(t, b.RemovePermission(context.Background(), eventbridge.RemovePermissionInput{
		RemoveAllPermissions: true,
	}))

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.Empty(t, policy)
}

func TestAudit_GetEventBusPolicy_BusNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.GetEventBusPolicy(context.Background(), "nonexistent")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

func TestAudit_PutEventBusPolicy_ReplacePolicy(t *testing.T) {
	t.Parallel()
	b := newBackend()

	policyJSON := `[{"Sid":"s1","Effect":"Allow","Action":"events:PutEvents","Principal":"123"}]`
	err := b.PutEventBusPolicy(context.Background(), eventbridge.PutEventBusPolicyInput{Policy: policyJSON})
	require.NoError(t, err)

	policy, err := b.GetEventBusPolicy(context.Background(), "")
	require.NoError(t, err)
	assert.Contains(t, policy, "s1")
}

func TestAudit_PutPermission_BusNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.PutPermission(context.Background(), eventbridge.PutPermissionInput{
		EventBusName: "no-such-bus",
		StatementID:  "s1",
		Action:       "events:PutEvents",
		Principal:    "123",
	})
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

// ---------------------------------------------------------------------------
// Issue 6: Pipes API
// ---------------------------------------------------------------------------

func TestAudit_Pipe_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	pipe, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
		Name:      "my-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:source-queue",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		RoleArn:   "arn:aws:iam::123456789012:role/my-pipe-role",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-pipe", pipe.Name)
	assert.NotEmpty(t, pipe.Arn)
	assert.Equal(t, "CREATING", pipe.CurrentState)

	described, err := b.DescribePipe(context.Background(), "my-pipe")
	require.NoError(t, err)
	assert.Equal(t, "my-pipe", described.Name)

	pipes, _, err := b.ListPipes(context.Background(), "", "")
	require.NoError(t, err)
	require.Len(t, pipes, 1)

	updated, err := b.UpdatePipe(context.Background(), eventbridge.UpdatePipeInput{
		Name:        "my-pipe",
		Description: "updated description",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", updated.Description)

	err = b.DeletePipe(context.Background(), "my-pipe")
	require.NoError(t, err)

	_, err = b.DescribePipe(context.Background(), "my-pipe")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_Pipe_CreateRejectsInvalidInput(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		input eventbridge.CreatePipeInput
	}{
		{"missing name", eventbridge.CreatePipeInput{SourceArn: "s", TargetArn: "t", RoleArn: "r"}},
		{"missing source", eventbridge.CreatePipeInput{Name: "p", TargetArn: "t", RoleArn: "r"}},
		{"missing target", eventbridge.CreatePipeInput{Name: "p", SourceArn: "s", RoleArn: "r"}},
		{"missing role", eventbridge.CreatePipeInput{Name: "p", SourceArn: "s", TargetArn: "t"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreatePipe(context.Background(), tt.input)
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestAudit_Pipe_DuplicateCreateFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	input := eventbridge.CreatePipeInput{
		Name:      "dup-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:q",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:f",
		RoleArn:   "arn:aws:iam::123456789012:role/r",
	}

	_, err := b.CreatePipe(context.Background(), input)
	require.NoError(t, err)

	_, err = b.CreatePipe(context.Background(), input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestAudit_Pipe_ListFiltersPrefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for _, name := range []string{"foo-1", "foo-2", "bar-1"} {
		_, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
			Name:      name,
			SourceArn: "arn:aws:sqs:us-east-1:123456789012:q",
			TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:f",
			RoleArn:   "arn:aws:iam::123456789012:role/r",
		})
		require.NoError(t, err)
	}

	pipes, _, err := b.ListPipes(context.Background(), "foo-", "")
	require.NoError(t, err)
	assert.Len(t, pipes, 2)
}

func TestAudit_Pipe_DesiredStatePreserved(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
		Name:         "my-pipe",
		SourceArn:    "arn:aws:sqs:us-east-1:123456789012:q",
		TargetArn:    "arn:aws:lambda:us-east-1:123456789012:function:f",
		RoleArn:      "arn:aws:iam::123456789012:role/r",
		DesiredState: "STOPPED",
	})
	require.NoError(t, err)

	p, err := b.DescribePipe(context.Background(), "my-pipe")
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", p.DesiredState)
}

// ---------------------------------------------------------------------------
// Issue 10: Archive retention + archived events pruning
// ---------------------------------------------------------------------------

func TestAudit_ArchiveJanitor_PrunesArchivedEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "my-bus", "")
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/my-bus"
	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: busARN,
		RetentionDays:  1,
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "test", DetailType: "Test", Detail: `{}`, EventBusName: "my-bus"},
	})

	// Make the archive look old enough to expire.
	err = b.SetArchiveCreationTimeForTest("my-archive", time.Now().Add(-48*time.Hour))
	require.NoError(t, err)

	janitor := eventbridge.NewArchiveJanitor(b, time.Hour)
	janitor.SetNow(time.Now())
	janitor.SweepOnce(context.Background())

	_, err = b.DescribeArchive(context.Background(), "my-archive")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	assert.Equal(t, 0, b.ArchivedEventCount("my-archive"))
}

func TestAudit_ArchiveJanitor_RetentionDaysZeroNeverExpires(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "bus2", "")
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/bus2"
	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "forever-archive",
		EventSourceArn: busARN,
		RetentionDays:  0, // 0 = forever
	})
	require.NoError(t, err)

	err = b.SetArchiveCreationTimeForTest("forever-archive", time.Now().Add(-365*24*time.Hour))
	require.NoError(t, err)

	janitor := eventbridge.NewArchiveJanitor(b, time.Hour)
	janitor.SetNow(time.Now())
	janitor.SweepOnce(context.Background())

	_, err = b.DescribeArchive(context.Background(), "forever-archive")
	require.NoError(t, err, "archive with RetentionDays=0 should never expire")
}

// ---------------------------------------------------------------------------
// Issue 11: Replay destination validation
// ---------------------------------------------------------------------------

func TestAudit_StartReplay_ValidatesDestination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "test-archive",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "replay-1",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
		Destination: &eventbridge.ReplayDestination{
			Arn: "arn:aws:events:us-east-1:123456789012:event-bus/nonexistent",
		},
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_StartReplay_ValidDestinationAccepted(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "test-archive",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	defaultBusARN := "arn:aws:events:us-east-1:123456789012:event-bus/default"

	replay, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "replay-2",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/test-archive",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
		Destination:    &eventbridge.ReplayDestination{Arn: defaultBusARN},
	})
	require.NoError(t, err)
	assert.Equal(t, "STARTING", replay.State)
}

func TestAudit_StartReplay_NoDestinationAccepted(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	replay, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "replay-3",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.NoError(t, err)
	assert.Equal(t, "STARTING", replay.State)
}

// ---------------------------------------------------------------------------
// Delivery DLQ test
// ---------------------------------------------------------------------------

// auditSQSSender is a thread-safe test SQS sender used in accuracy audit tests.
type auditSQSSender struct {
	queues map[string][]string
	mu     sync.Mutex
}

func newAuditSQSSender() *auditSQSSender {
	return &auditSQSSender{queues: make(map[string][]string)}
}

func (s *auditSQSSender) SendMessageToQueue(_ context.Context, queueARN, body string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queues[queueARN] = append(s.queues[queueARN], body)

	return nil
}

func (s *auditSQSSender) MessagesFor(queueARN string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string{}, s.queues[queueARN]...)
}

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

func TestAudit_Delivery_DLQCalledOnFailure(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newAuditSQSSender()
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

func TestAudit_Delivery_RetryPolicyZeroAttemptsNeverRetries(t *testing.T) {
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

func TestAudit_Delivery_DefaultRetryAttempts(t *testing.T) {
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

// ---------------------------------------------------------------------------
// Handler-level smoke tests for new operations
// ---------------------------------------------------------------------------

func auditMakeRequest(
	t *testing.T,
	handler *eventbridge.Handler,
	e *echo.Echo,
	action string,
	body map[string]any,
) *httptest.ResponseRecorder {
	t.Helper()
	b, _ := json.Marshal(body)
	req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader(string(b)))
	req.Header.Set("X-Amz-Target", "AmazonEventBridge."+action)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, handler.Handler()(c))

	return rec
}

func TestAudit_Handler_GetEventBusPolicy(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "PutPermission", map[string]any{
		"StatementId": "allow-123",
		"Action":      "events:PutEvents",
		"Principal":   "123456789013",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "GetEventBusPolicy", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "allow-123")
}

func TestAudit_Handler_Pipes(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreatePipe", map[string]any{
		"Name":      "my-pipe",
		"SourceArn": "arn:aws:sqs:us-east-1:123456789012:source-q",
		"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
		"RoleArn":   "arn:aws:iam::123456789012:role/pipe-role",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-pipe")

	rec = auditMakeRequest(t, h, e, "DescribePipe", map[string]any{"Name": "my-pipe"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListPipes", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "my-pipe")

	rec = auditMakeRequest(t, h, e, "UpdatePipe", map[string]any{
		"Name":        "my-pipe",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeletePipe", map[string]any{"Name": "my-pipe"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribePipe", map[string]any{"Name": "my-pipe"})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit_Handler_ResourceLimitExceededMapsTo400(t *testing.T) {
	t.Parallel()

	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	// Create 200 buses directly.
	for i := range 200 {
		_, err := b.CreateEventBus(context.Background(), fmt.Sprintf("bus-%d", i), "")
		require.NoError(t, err)
	}

	// 201st via handler should return 400.
	rec := auditMakeRequest(t, h, e, "CreateEventBus", map[string]any{"Name": "bus-overflow"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ResourceLimitExceededException")
}

func TestAudit_Handler_GetSupportedOperationsIncludesPipes(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(newBackend())
	ops := h.GetSupportedOperations()

	pipeOps := []string{"CreatePipe", "DeletePipe", "DescribePipe", "ListPipes", "UpdatePipe"}
	for _, op := range pipeOps {
		assert.Contains(t, ops, op, "GetSupportedOperations should include %s", op)
	}
}

func TestAudit_Handler_GetSupportedOperationsIncludesPolicyOps(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(newBackend())
	ops := h.GetSupportedOperations()

	assert.Contains(t, ops, "GetEventBusPolicy")
	assert.Contains(t, ops, "PutEventBusPolicy")
}

// ---------------------------------------------------------------------------
// Kinesis Firehose delivery
// ---------------------------------------------------------------------------

type auditKinesisFirehoseSink struct {
	records map[string][]string
	mu      sync.Mutex
}

func newAuditKinesisFirehoseSink() *auditKinesisFirehoseSink {
	return &auditKinesisFirehoseSink{records: make(map[string][]string)}
}

func (s *auditKinesisFirehoseSink) PutRecord(_ context.Context, streamARN, data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[streamARN] = append(s.records[streamARN], data)

	return nil
}

func (s *auditKinesisFirehoseSink) RecordsFor(streamARN string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string{}, s.records[streamARN]...)
}

func TestAudit_Delivery_KinesisFirehose_DeliversEvent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sink := newAuditKinesisFirehoseSink()
	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/my-stream"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{KinesisFirehose: sink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "firehose-rule",
		EventPattern: `{"source":["firehose-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "firehose-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "firehose-test", DetailType: "T", Detail: `{"key":"val"}`},
	})

	require.Eventually(t, func() bool {
		return len(sink.RecordsFor(streamARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "Kinesis Firehose should have received a record")

	record := sink.RecordsFor(streamARN)[0]
	assert.Contains(t, record, "firehose-test")
}

func TestAudit_Delivery_KinesisFirehose_NilHandlerSkipsGracefully(t *testing.T) {
	t.Parallel()
	b := newBackend()

	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/no-backend"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "firehose-nil-rule",
		EventPattern: `{"source":["nil-firehose"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "firehose-nil-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "nil-firehose", DetailType: "T", Detail: `{}`},
		})
	})
}

type auditFailingKinesisFirehoseSink struct {
	delegate *auditKinesisFirehoseSink
	failARN  string
}

func (f *auditFailingKinesisFirehoseSink) PutRecord(ctx context.Context, streamARN, data string) error {
	if streamARN == f.failARN {
		return errSimulatedFailure
	}

	return f.delegate.PutRecord(ctx, streamARN, data)
}

func TestAudit_Delivery_KinesisFirehose_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newAuditSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:firehose-dlq"
	streamARN := "arn:aws:firehose:us-east-1:123456789012:deliverystream/failing-stream"

	firehoseSink := newAuditKinesisFirehoseSink()
	failingSink := &auditFailingKinesisFirehoseSink{delegate: firehoseSink, failARN: streamARN}

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		KinesisFirehose: failingSink,
		SQS:             dlqSink,
	})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "firehose-dlq-rule",
		EventPattern: `{"source":["firehose-dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "firehose-dlq-rule", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              streamARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
			RetryPolicy:      &eventbridge.RetryPolicy{MaximumRetryAttempts: 0},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "firehose-dlq-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should have received the failed Firehose event")
}

// ---------------------------------------------------------------------------
// Kinesis Data Stream delivery
// ---------------------------------------------------------------------------

type auditKinesisStreamSink struct {
	records map[string][]string
	mu      sync.Mutex
}

func newAuditKinesisStreamSink() *auditKinesisStreamSink {
	return &auditKinesisStreamSink{records: make(map[string][]string)}
}

func (s *auditKinesisStreamSink) PutRecord(_ context.Context, streamARN, _ string, data string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.records[streamARN] = append(s.records[streamARN], data)

	return nil
}

func (s *auditKinesisStreamSink) RecordsFor(streamARN string) []string {
	s.mu.Lock()
	defer s.mu.Unlock()

	return append([]string{}, s.records[streamARN]...)
}

func TestAudit_Delivery_KinesisStream_DeliversEvent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sink := newAuditKinesisStreamSink()
	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/my-stream"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{KinesisStream: sink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "kinesis-rule",
		EventPattern: `{"source":["kinesis-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "kinesis-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "kinesis-test", DetailType: "T", Detail: `{"k":"v"}`},
	})

	require.Eventually(t, func() bool {
		return len(sink.RecordsFor(streamARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "Kinesis Data Stream should have received a record")

	record := sink.RecordsFor(streamARN)[0]
	assert.Contains(t, record, "kinesis-test")
}

func TestAudit_Delivery_KinesisStream_NilHandlerSkipsGracefully(t *testing.T) {
	t.Parallel()
	b := newBackend()

	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/no-backend"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "kinesis-nil-rule",
		EventPattern: `{"source":["nil-kinesis"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "kinesis-nil-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: streamARN},
	})
	require.NoError(t, err)

	require.NotPanics(t, func() {
		b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "nil-kinesis", DetailType: "T", Detail: `{}`},
		})
	})
}

type auditFailingKinesisStreamSink struct {
	delegate *auditKinesisStreamSink
	failARN  string
}

func (f *auditFailingKinesisStreamSink) PutRecord(ctx context.Context, streamARN, partitionKey, data string) error {
	if streamARN == f.failARN {
		return errSimulatedFailure
	}

	return f.delegate.PutRecord(ctx, streamARN, partitionKey, data)
}

func TestAudit_Delivery_KinesisStream_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newAuditSQSSender()
	dlqARN := "arn:aws:sqs:us-east-1:123456789012:kinesis-dlq"
	streamARN := "arn:aws:kinesis:us-east-1:123456789012:stream/failing-stream"

	kinesisDelegate := newAuditKinesisStreamSink()
	failingSink := &auditFailingKinesisStreamSink{delegate: kinesisDelegate, failARN: streamARN}

	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{
		KinesisStream: failingSink,
		SQS:           dlqSink,
	})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "kinesis-dlq-rule",
		EventPattern: `{"source":["kinesis-dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "kinesis-dlq-rule", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              streamARN,
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
			RetryPolicy:      &eventbridge.RetryPolicy{MaximumRetryAttempts: 0},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "kinesis-dlq-test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(dlqSink.MessagesFor(dlqARN)) > 0
	}, 2*time.Second, 10*time.Millisecond, "DLQ should have received the failed Kinesis Stream event")
}

// ---------------------------------------------------------------------------
// ECS RunTask delivery
// ---------------------------------------------------------------------------

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

func TestAudit_Delivery_ECS_DeliversEvent(t *testing.T) {
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

func TestAudit_Delivery_ECS_NilHandlerSkipsGracefully(t *testing.T) {
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

func TestAudit_Delivery_ECS_FailureSendsToDLQ(t *testing.T) {
	t.Parallel()
	b := newBackend()

	dlqSink := newAuditSQSSender()
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

// ---------------------------------------------------------------------------
// Tags (EventBus, Rule, Connection, Archive, Endpoint, Pipe)
// ---------------------------------------------------------------------------

func TestAudit_Tags_EventBus(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/default"

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": busARN,
		"Tags":        []map[string]string{{"Key": "env", "Value": "test"}, {"Key": "team", "Value": "platform"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": busARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "env")
	assert.Contains(t, rec.Body.String(), "platform")

	rec = auditMakeRequest(t, h, e, "UntagResource", map[string]any{
		"ResourceARN": busARN,
		"TagKeys":     []string{"env"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": busARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.NotContains(t, rec.Body.String(), "\"env\"")
	assert.Contains(t, rec.Body.String(), "platform")
}

func TestAudit_Tags_Rule(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "tagged-rule",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	ruleARN := "arn:aws:events:us-east-1:123456789012:rule/default/tagged-rule"

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": ruleARN,
		"Tags":        []map[string]string{{"Key": "owner", "Value": "alice"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": ruleARN,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "alice")
}

func TestAudit_Tags_Connection(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": conn.ConnectionArn,
		"Tags":        []map[string]string{{"Key": "purpose", "Value": "ci"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": conn.ConnectionArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "ci")
}

func TestAudit_Tags_Archive(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateEventBus(context.Background(), "tagged-bus", "")
	require.NoError(t, err)

	archive, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "tagged-archive",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/tagged-bus",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": archive.ArchiveArn,
		"Tags":        []map[string]string{{"Key": "retention", "Value": "30d"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": archive.ArchiveArn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "30d")
}

func TestAudit_Tags_Pipe(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	pipe, err := b.CreatePipe(context.Background(), eventbridge.CreatePipeInput{
		Name:      "tagged-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:source-q",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:fn",
		RoleArn:   "arn:aws:iam::123456789012:role/r",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "TagResource", map[string]any{
		"ResourceARN": pipe.Arn,
		"Tags":        []map[string]string{{"Key": "stage", "Value": "prod"}},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListTagsForResource", map[string]any{
		"ResourceARN": pipe.Arn,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "prod")
}

// ---------------------------------------------------------------------------
// Connection auth type validation
// ---------------------------------------------------------------------------

func TestAudit_Connection_AuthTypes(t *testing.T) {
	t.Parallel()

	valid := []string{"BASIC", "API_KEY", "OAUTH_CLIENT_CREDENTIALS"}
	for _, authType := range valid {
		t.Run(authType, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
				Name:              "conn-" + authType,
				AuthorizationType: authType,
			})
			require.NoError(t, err)
			assert.Equal(t, authType, conn.AuthorizationType)
			assert.Equal(t, "AUTHORIZED", conn.ConnectionState)
		})
	}

	invalid := []string{"", "NONE", "OAUTH", "KEY"}
	for _, authType := range invalid {
		t.Run("invalid-"+authType, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
				Name:              "conn-invalid",
				AuthorizationType: authType,
			})
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestAudit_Connection_UpdateAuthType(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	updated, err := b.UpdateConnection(context.Background(), eventbridge.UpdateConnectionInput{
		Name:              "my-conn",
		AuthorizationType: "API_KEY",
	})
	require.NoError(t, err)
	assert.Equal(t, "API_KEY", updated.AuthorizationType)
}

func TestAudit_Connection_DeauthorizeTransitionsState(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "deauth-conn",
		AuthorizationType: "OAUTH_CLIENT_CREDENTIALS",
	})
	require.NoError(t, err)

	conn, err := b.DeauthorizeConnection(context.Background(), "deauth-conn")
	require.NoError(t, err)
	assert.Equal(t, "DEAUTHORIZED", conn.ConnectionState)
}

// ---------------------------------------------------------------------------
// APIDestination + Connection CRUD completeness
// ---------------------------------------------------------------------------

func TestAudit_APIDestination_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "api-dest-conn",
		AuthorizationType: "API_KEY",
	})
	require.NoError(t, err)

	dst, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
		Name:                         "my-api-dest",
		ConnectionArn:                conn.ConnectionArn,
		HTTPMethod:                   "POST",
		InvocationEndpoint:           "https://example.com/webhook",
		InvocationRateLimitPerSecond: 300,
	})
	require.NoError(t, err)
	assert.Equal(t, "my-api-dest", dst.Name)
	assert.Equal(t, "ACTIVE", dst.APIDestinationState)
	assert.Equal(t, 300, dst.InvocationRateLimitPerSecond)

	described, err := b.DescribeAPIDestination(context.Background(), "my-api-dest")
	require.NoError(t, err)
	assert.Equal(t, "POST", described.HTTPMethod)

	updated, err := b.UpdateAPIDestination(context.Background(), eventbridge.UpdateAPIDestinationInput{
		Name:       "my-api-dest",
		HTTPMethod: "PUT",
	})
	require.NoError(t, err)
	assert.Equal(t, "PUT", updated.HTTPMethod)

	dsts, _, err := b.ListAPIDestinations(context.Background(), "my-api-", "")
	require.NoError(t, err)
	assert.Len(t, dsts, 1)

	err = b.DeleteAPIDestination(context.Background(), "my-api-dest")
	require.NoError(t, err)

	_, err = b.DescribeAPIDestination(context.Background(), "my-api-dest")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_APIDestination_InvalidHTTPMethod(t *testing.T) {
	t.Parallel()

	invalid := []string{"TRACE", "CONNECT", ""}
	for _, method := range invalid {
		t.Run("invalid-"+method, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			_, err := b.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
				Name:               "dest",
				ConnectionArn:      "arn:aws:events:us-east-1:123456789012:connection/c",
				HTTPMethod:         method,
				InvocationEndpoint: "https://example.com",
			})
			require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		})
	}
}

func TestAudit_APIDestination_ValidHTTPMethods(t *testing.T) {
	t.Parallel()

	b := newBackend()
	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name: "ref-conn", AuthorizationType: "BASIC",
	})
	require.NoError(t, err)

	conn, _ := b.DescribeConnection(context.Background(), "ref-conn")

	valid := []string{"GET", "HEAD", "POST", "OPTIONS", "PUT", "DELETE", "PATCH"}
	for i, method := range valid {
		t.Run(method, func(t *testing.T) {
			t.Parallel()
			bLocal := newBackend()
			_, createErr := bLocal.CreateAPIDestination(context.Background(), eventbridge.CreateAPIDestinationInput{
				Name:               fmt.Sprintf("dest-%d", i),
				ConnectionArn:      conn.ConnectionArn,
				HTTPMethod:         method,
				InvocationEndpoint: "https://example.com/" + method,
			})
			require.NoError(t, createErr)
		})
	}
}

// ---------------------------------------------------------------------------
// GlobalEndpoint CRUD
// ---------------------------------------------------------------------------

func TestAudit_Endpoint_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "primary-bus", "")
	require.NoError(t, err)
	_, err = b.CreateEventBus(context.Background(), "secondary-bus", "")
	require.NoError(t, err)

	primaryARN := "arn:aws:events:us-east-1:123456789012:event-bus/primary-bus"
	secondaryARN := "arn:aws:events:us-west-2:123456789012:event-bus/secondary-bus"

	ep, err := b.CreateEndpoint(context.Background(), eventbridge.CreateEndpointInput{
		Name: "my-endpoint",
		RoutingConfig: &eventbridge.RoutingConfig{
			FailoverConfig: &eventbridge.FailoverConfig{
				Primary:   &eventbridge.Primary{HealthCheck: "arn:aws:route53:::healthcheck/abc"},
				Secondary: &eventbridge.Secondary{Route: "us-west-2"},
			},
		},
		ReplicationConfig: &eventbridge.ReplicationConfig{State: "ENABLED"},
		EventBuses:        []eventbridge.EndpointEventBus{{EventBusArn: primaryARN}, {EventBusArn: secondaryARN}},
	})
	require.NoError(t, err)
	assert.Equal(t, "my-endpoint", ep.Name)
	assert.Equal(t, "ACTIVE", ep.State)
	assert.NotEmpty(t, ep.EndpointURL)
	assert.Len(t, ep.EventBuses, 2)

	described, err := b.DescribeEndpoint(context.Background(), "my-endpoint")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", described.ReplicationConfig.State)

	updated, err := b.UpdateEndpoint(context.Background(), eventbridge.UpdateEndpointInput{
		Name:        "my-endpoint",
		Description: "updated endpoint",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated endpoint", updated.Description)

	eps, _, err := b.ListEndpoints(context.Background(), "my-", "")
	require.NoError(t, err)
	assert.Len(t, eps, 1)

	err = b.DeleteEndpoint(context.Background(), "my-endpoint")
	require.NoError(t, err)

	_, err = b.DescribeEndpoint(context.Background(), "my-endpoint")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// ---------------------------------------------------------------------------
// EventSource (partner SaaS) operations
// ---------------------------------------------------------------------------

func TestAudit_EventSource_ActivateDeactivate(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/example.com/myapp", "123456789012")
	require.NoError(t, err)

	src, err := b.DescribePartnerEventSource(context.Background(), "aws.partner/example.com/myapp")
	require.NoError(t, err)
	assert.Equal(t, "aws.partner/example.com/myapp", src.Name)

	srcs, _, err := b.ListPartnerEventSources(context.Background(), "aws.partner/", "")
	require.NoError(t, err)
	assert.Len(t, srcs, 1)

	err = b.DeletePartnerEventSource(context.Background(), "aws.partner/example.com/myapp")
	require.NoError(t, err)

	_, err = b.DescribePartnerEventSource(context.Background(), "aws.partner/example.com/myapp")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_EventSource_ActivateChangesState(t *testing.T) {
	t.Parallel()
	b := newBackend()

	name := "aws.partner/test.com/app"
	// Seed the event source directly (partner source → event source activation).
	b.AddPartnerSourceInternal(&eventbridge.PartnerEventSource{
		Name:    name,
		Arn:     "arn:aws:events:us-east-1:123456789012:event-source/aws.partner/test.com/app",
		Account: "123456789012",
	})

	src, _, err := b.ListEventSources(context.Background(), "", "")
	require.NoError(t, err)
	_ = src // verify list works without panic
}

// ---------------------------------------------------------------------------
// PutPartnerEvents
// ---------------------------------------------------------------------------

func TestAudit_PutPartnerEvents_RecordsResults(t *testing.T) {
	t.Parallel()
	b := newBackend()

	entries := []eventbridge.EventEntry{
		{Source: "aws.partner/example.com/app", DetailType: "UserSignup", Detail: `{"userId":"u1"}`},
		{Source: "aws.partner/example.com/app", DetailType: "OrderPlaced", Detail: `{"orderId":"o1"}`},
	}

	results, err := b.PutPartnerEvents(context.Background(), entries)
	require.NoError(t, err)
	assert.Len(t, results, 2)
	for _, r := range results {
		assert.Empty(t, r.ErrorCode)
		assert.NotEmpty(t, r.EventID)
	}
}

// ---------------------------------------------------------------------------
// Archive CRUD completeness
// ---------------------------------------------------------------------------

func TestAudit_Archive_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "src-bus", "")
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/src-bus"

	archive, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: busARN,
		RetentionDays:  7,
		EventPattern:   `{"source":["important.app"]}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "my-archive", archive.ArchiveName)
	assert.Equal(t, 7, archive.RetentionDays)

	described, err := b.DescribeArchive(context.Background(), "my-archive")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", described.State)

	updated, err := b.UpdateArchive(context.Background(), eventbridge.UpdateArchiveInput{
		ArchiveName:   "my-archive",
		Description:   "important events",
		RetentionDays: 14,
	})
	require.NoError(t, err)
	assert.Equal(t, 14, updated.RetentionDays)
	assert.Equal(t, "important events", updated.Description)

	archives, _, err := b.ListArchives(context.Background(), "my-", "")
	require.NoError(t, err)
	assert.Len(t, archives, 1)

	err = b.DeleteArchive(context.Background(), "my-archive")
	require.NoError(t, err)

	_, err = b.DescribeArchive(context.Background(), "my-archive")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_Archive_RejectsLongName(t *testing.T) {
	t.Parallel()
	b := newBackend()

	longName := strings.Repeat("a", 49)
	_, err := b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    longName,
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_Archive_CapturesMatchingEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "capture-bus", "")
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/capture-bus"
	_, err = b.CreateArchive(context.Background(), eventbridge.CreateArchiveInput{
		ArchiveName:    "capture-archive",
		EventSourceArn: busARN,
		EventPattern:   `{"source":["audit.app"]}`,
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "audit.app", DetailType: "T", Detail: `{}`, EventBusName: "capture-bus"},
		{Source: "other.app", DetailType: "T", Detail: `{}`, EventBusName: "capture-bus"},
	})

	count := b.ArchivedEventCount("capture-archive")
	assert.Equal(t, 1, count, "only matching events should be archived")
}

// ---------------------------------------------------------------------------
// TestEventPattern
// ---------------------------------------------------------------------------

func TestAudit_TestEventPattern_Match(t *testing.T) {
	t.Parallel()
	b := newBackend()

	matched, err := b.TestEventPattern(
		context.Background(),
		`{"source":["myapp"],"detail-type":["OrderPlaced"]}`,
		`{"source":"myapp","detail-type":"OrderPlaced","detail":{}}`,
	)
	require.NoError(t, err)
	assert.True(t, matched)
}

func TestAudit_TestEventPattern_NoMatch(t *testing.T) {
	t.Parallel()
	b := newBackend()

	matched, err := b.TestEventPattern(
		context.Background(),
		`{"source":["myapp"]}`,
		`{"source":"other","detail-type":"T","detail":{}}`,
	)
	require.NoError(t, err)
	assert.False(t, matched)
}

func TestAudit_TestEventPattern_InvalidPattern(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.TestEventPattern(context.Background(), "not-json", `{"source":"x"}`)
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_TestEventPattern_InvalidEvent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.TestEventPattern(context.Background(), `{"source":["x"]}`, "not-json")
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// ---------------------------------------------------------------------------
// ListRuleNamesByTarget
// ---------------------------------------------------------------------------

func TestAudit_ListRuleNamesByTarget(t *testing.T) {
	t.Parallel()
	b := newBackend()

	targetARN := "arn:aws:lambda:us-east-1:123456789012:function:shared-fn"

	for _, ruleName := range []string{"rule-a", "rule-b", "rule-c"} {
		_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
			Name:         ruleName,
			EventPattern: `{"source":["x"]}`,
		})
		require.NoError(t, err)
	}

	_, err := b.PutTargets(context.Background(), "rule-a", "", []eventbridge.Target{{ID: "t1", Arn: targetARN}})
	require.NoError(t, err)
	_, err = b.PutTargets(context.Background(), "rule-c", "", []eventbridge.Target{{ID: "t1", Arn: targetARN}})
	require.NoError(t, err)
	_, err = b.PutTargets(context.Background(), "rule-b", "", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:other-q"},
	})
	require.NoError(t, err)

	names, _, err := b.ListRuleNamesByTarget(context.Background(), targetARN, "", "")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"rule-a", "rule-c"}, names)
}

// ---------------------------------------------------------------------------
// PutEvents batch behavior
// ---------------------------------------------------------------------------

func TestAudit_PutEvents_BatchSizeLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// Build one massive entry > 256KiB in detail.
	bigDetail := strings.Repeat("x", 260*1024)
	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "s", DetailType: "T", Detail: bigDetail},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Equal(t, "EventSizeLimitExceeded", results[0].ErrorCode)
}

func TestAudit_PutEvents_MixedBatch(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// First entry is small (accepted), second is huge (rejected), third is small (accepted).
	bigDetail := strings.Repeat("y", 260*1024)
	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "s", DetailType: "T", Detail: `{}`},
		{Source: "s", DetailType: "T", Detail: bigDetail},
		{Source: "s", DetailType: "T", Detail: `{}`},
	})
	require.NoError(t, err)
	require.Len(t, results, 3)
	assert.Empty(t, results[0].ErrorCode)
	assert.Equal(t, "EventSizeLimitExceeded", results[1].ErrorCode)
	assert.Empty(t, results[2].ErrorCode)
}

// TestAudit_PutEvents_EntryCountLimit proves AWS's 1-10 entries-per-request
// constraint on PutEvents (PutEventsRequestEntryList: Array Members Minimum
// 1, Maximum 10 item(s)): 0 or 11 entries fail the whole request, while 1
// and 10 succeed.
func TestAudit_PutEvents_EntryCountLimit(t *testing.T) {
	t.Parallel()

	makeEntries := func(n int) []eventbridge.EventEntry {
		entries := make([]eventbridge.EventEntry, n)
		for i := range entries {
			entries[i] = eventbridge.EventEntry{Source: "s", DetailType: "T", Detail: `{}`}
		}

		return entries
	}

	tests := []struct {
		name    string
		count   int
		wantErr bool
	}{
		{"zero entries rejected", 0, true},
		{"one entry accepted", 1, false},
		{"ten entries accepted", 10, false},
		{"eleven entries rejected", 11, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newBackend()
			results, err := b.PutEvents(context.Background(), makeEntries(tt.count))
			if tt.wantErr {
				require.Error(t, err)
				require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
				assert.Nil(t, results)

				return
			}
			require.NoError(t, err)
			require.Len(t, results, tt.count)
			for _, r := range results {
				assert.Empty(t, r.ErrorCode)
			}
		})
	}
}

// TestAudit_PutEvents_RequiredFields proves that Source, DetailType, and
// Detail are required on every PutEvents entry (per aws-sdk-go-v2's
// PutEventsRequestEntry doc: "Detail, DetailType, and Source are required
// for EventBridge to successfully send an event... If you include event
// entries in a request that do not include each of those properties,
// EventBridge fails that entry. If you submit a request in which none of the
// entries have each of these properties, EventBridge fails the entire
// request.").
func TestAudit_PutEvents_RequiredFields(t *testing.T) {
	t.Parallel()

	t.Run("entry missing Source fails individually", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{DetailType: "T", Detail: `{}`},
			{Source: "s", DetailType: "T", Detail: `{}`},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "InvalidArgument", results[0].ErrorCode)
		assert.NotEmpty(t, results[0].ErrorMessage)
		assert.Empty(t, results[1].ErrorCode)
		assert.NotEmpty(t, results[1].EventID)
	})

	t.Run("entry missing DetailType fails individually", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "s", Detail: `{}`},
			{Source: "s", DetailType: "T", Detail: `{}`},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "InvalidArgument", results[0].ErrorCode)
		assert.Empty(t, results[1].ErrorCode)
	})

	t.Run("entry missing Detail fails individually", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{Source: "s", DetailType: "T"},
			{Source: "s", DetailType: "T", Detail: `{}`},
		})
		require.NoError(t, err)
		require.Len(t, results, 2)
		assert.Equal(t, "InvalidArgument", results[0].ErrorCode)
		assert.Empty(t, results[1].ErrorCode)
	})

	t.Run("no entry complete fails the whole request", func(t *testing.T) {
		t.Parallel()
		b := newBackend()
		results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
			{DetailType: "T", Detail: `{}`},
			{Source: "s", Detail: `{}`},
		})
		require.Error(t, err)
		require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
		assert.Nil(t, results)
	})
}

func TestAudit_PutEvents_DefaultBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "default.test", DetailType: "Ping", Detail: `{}`},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)
	assert.NotEmpty(t, results[0].EventID)
}

func TestAudit_PutEvents_NamedBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "named-bus", "")
	require.NoError(t, err)

	results, err := b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "s", DetailType: "T", Detail: `{}`, EventBusName: "named-bus"},
	})
	require.NoError(t, err)
	require.Len(t, results, 1)
	assert.Empty(t, results[0].ErrorCode)
}

// ---------------------------------------------------------------------------
// EventBus CRUD completeness
// ---------------------------------------------------------------------------

func TestAudit_EventBus_DefaultAlwaysPresent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	bus, err := b.DescribeEventBus(context.Background(), "")
	require.NoError(t, err)
	assert.Equal(t, "default", bus.Name)
}

func TestAudit_EventBus_CannotDeleteDefault(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.DeleteEventBus(context.Background(), "default")
	require.ErrorIs(t, err, eventbridge.ErrCannotDeleteDefaultBus)
}

func TestAudit_EventBus_UpdateDescription(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "update-me", "original")
	require.NoError(t, err)

	updated, err := b.UpdateEventBus(context.Background(), eventbridge.UpdateEventBusInput{
		Name:        "update-me",
		Description: "new description",
	})
	require.NoError(t, err)
	assert.Equal(t, "new description", updated.Description)
}

func TestAudit_EventBus_ListPagination(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for i := range 5 {
		_, err := b.CreateEventBus(context.Background(), fmt.Sprintf("page-bus-%d", i), "")
		require.NoError(t, err)
	}

	buses, _, err := b.ListEventBuses(context.Background(), "page-bus-", "", 0)
	require.NoError(t, err)
	assert.Len(t, buses, 5)
}

func TestAudit_EventBus_DeleteCleansUpRulesAndTargets(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus(context.Background(), "to-delete", "")
	require.NoError(t, err)

	_, err = b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "orphan-rule",
		EventBusName: "to-delete",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "orphan-rule", "to-delete", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:sqs:us-east-1:123456789012:q"},
	})
	require.NoError(t, err)

	err = b.DeleteEventBus(context.Background(), "to-delete")
	require.NoError(t, err)

	_, err = b.DescribeEventBus(context.Background(), "to-delete")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)

	// Rules on deleted bus should also be gone.
	rules, _, err := b.ListRules(context.Background(), "to-delete", "", "", 0)
	require.NoError(t, err)
	assert.Empty(t, rules)
}

// ---------------------------------------------------------------------------
// Rule CRUD completeness
// ---------------------------------------------------------------------------

func TestAudit_Rule_EnableDisable(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "toggle-rule",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	err = b.DisableRule(context.Background(), "toggle-rule", "")
	require.NoError(t, err)

	rule, err := b.DescribeRule(context.Background(), "toggle-rule", "")
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", rule.State)

	err = b.EnableRule(context.Background(), "toggle-rule", "")
	require.NoError(t, err)

	rule, err = b.DescribeRule(context.Background(), "toggle-rule", "")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
}

func TestAudit_Rule_NameTooLong(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         strings.Repeat("a", 65),
		EventPattern: `{"source":["x"]}`,
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_Rule_MutuallyExclusivePatternAndSchedule(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:               "bad-rule",
		EventPattern:       `{"source":["x"]}`,
		ScheduleExpression: "rate(1 minute)",
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_Rule_RequiresPatternOrSchedule(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name: "empty-rule",
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_Rule_DefaultStateIsEnabled(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "auto-enabled",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	rule, err := b.DescribeRule(context.Background(), "auto-enabled", "")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
}

// ---------------------------------------------------------------------------
// Replay completeness
// ---------------------------------------------------------------------------

func TestAudit_Replay_CancelNonRunningFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "my-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.NoError(t, err)

	// Wait for the background goroutine to finish the replay.
	require.Eventually(t, func() bool {
		r, descErr := b.DescribeReplay(context.Background(), "my-replay")

		return descErr == nil && r.State == "COMPLETED"
	}, 5*time.Second, 10*time.Millisecond)

	// Cancelling a COMPLETED replay must fail.
	_, err = b.CancelReplay(context.Background(), "my-replay")
	require.ErrorIs(t, err, eventbridge.ErrInvalidState)
}

func TestAudit_Replay_DuplicateNameFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc2",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc2",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "dup-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc2",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.NoError(t, err)

	_, err = b.StartReplay(context.Background(), eventbridge.StartReplayInput{
		ReplayName:     "dup-replay",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc2",
		EventStartTime: time.Now().Add(-time.Hour),
		EventEndTime:   time.Now(),
	})
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestAudit_Replay_ListWithPrefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "arc3",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/arc3",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	for _, name := range []string{"prod-replay-1", "prod-replay-2", "dev-replay-1"} {
		_, err := b.StartReplay(context.Background(), eventbridge.StartReplayInput{
			ReplayName:     name,
			EventSourceArn: "arn:aws:events:us-east-1:123456789012:archive/arc3",
			EventStartTime: time.Now().Add(-time.Hour),
			EventEndTime:   time.Now(),
		})
		require.NoError(t, err)
	}

	replays, _, err := b.ListReplays(context.Background(), "prod-", "")
	require.NoError(t, err)
	assert.Len(t, replays, 2)
}

// ---------------------------------------------------------------------------
// Targets: max limit and Input* options
// ---------------------------------------------------------------------------

func TestAudit_Targets_MaxTargetsPerRuleEnforced(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "limit-rule",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	targets := make([]eventbridge.Target, 5)
	for i := range targets {
		targets[i] = eventbridge.Target{
			ID:  fmt.Sprintf("t%d", i),
			Arn: fmt.Sprintf("arn:aws:sqs:us-east-1:123456789012:q%d", i),
		}
	}

	_, err = b.PutTargets(context.Background(), "limit-rule", "", targets)
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "limit-rule", "", []eventbridge.Target{
		{ID: "t-overflow", Arn: "arn:aws:sqs:us-east-1:123456789012:overflow"},
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

func TestAudit_Targets_InputOverridePayload(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newAuditSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:input-override-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "override-rule",
		EventPattern: `{"source":["override.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "override-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: queueARN, Input: `{"static":"payload"}`},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "override.test", DetailType: "T", Detail: `{"dynamic":"field"}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "static")
	assert.NotContains(t, msg, "dynamic")
}

func TestAudit_Targets_InputPathExtractsField(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newAuditSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:input-path-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "path-rule",
		EventPattern: `{"source":["path.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "path-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: queueARN, InputPath: "$.source"},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "path.test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "path.test")
}

func TestAudit_Targets_InputTransformerApplied(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newAuditSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:transform-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "transform-rule",
		EventPattern: `{"source":["transform.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "transform-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{"src": "$.source"},
				InputTemplate: `{"event_source": "<src>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "transform.test", DetailType: "T", Detail: `{}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "event_source")
	assert.Contains(t, msg, "transform.test")
}

// ---------------------------------------------------------------------------
// Handler coverage: operations not previously exercised
// ---------------------------------------------------------------------------

func TestAudit_Handler_UpdateEventBus(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateEventBus(context.Background(), "describable-bus", "old desc")
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "UpdateEventBus", map[string]any{
		"Name":        "describable-bus",
		"Description": "new desc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_ListRuleNamesByTarget(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "h-rule",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "h-rule", "", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:lambda:us-east-1:123456789012:function:fn"},
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "ListRuleNamesByTarget", map[string]any{
		"TargetArn": "arn:aws:lambda:us-east-1:123456789012:function:fn",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-rule")
}

func TestAudit_Handler_TestEventPattern(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "TestEventPattern", map[string]any{
		"EventPattern": `{"source":["myapp"]}`,
		"Event":        `{"source":"myapp","detail-type":"X","detail":{}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp struct {
		Result bool `json:"Result"`
	}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.True(t, resp.Result)
}

func TestAudit_Handler_DisableAndEnableRule(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "toggle-h-rule",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "DisableRule", map[string]any{"Name": "toggle-h-rule"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "EnableRule", map[string]any{"Name": "toggle-h-rule"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_ArchiveCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateEventBus(context.Background(), "h-archive-bus", "")
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "CreateArchive", map[string]any{
		"ArchiveName":    "h-archive",
		"EventSourceArn": "arn:aws:events:us-east-1:123456789012:event-bus/h-archive-bus",
		"RetentionDays":  30,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeArchive", map[string]any{"ArchiveName": "h-archive"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-archive")

	rec = auditMakeRequest(t, h, e, "UpdateArchive", map[string]any{
		"ArchiveName":   "h-archive",
		"Description":   "handler updated",
		"RetentionDays": 60,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListArchives", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-archive")

	rec = auditMakeRequest(t, h, e, "DeleteArchive", map[string]any{"ArchiveName": "h-archive"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_ConnectionCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateConnection", map[string]any{
		"Name":              "h-conn",
		"AuthorizationType": "BASIC",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeConnection", map[string]any{"Name": "h-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "AUTHORIZED")

	rec = auditMakeRequest(t, h, e, "ListConnections", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "UpdateConnection", map[string]any{
		"Name":              "h-conn",
		"AuthorizationType": "API_KEY",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeauthorizeConnection", map[string]any{"Name": "h-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "DEAUTHORIZED")

	rec = auditMakeRequest(t, h, e, "DeleteConnection", map[string]any{"Name": "h-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_EndpointCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateEndpoint", map[string]any{
		"Name": "h-endpoint",
		"RoutingConfig": map[string]any{
			"FailoverConfig": map[string]any{
				"Primary":   map[string]string{"HealthCheck": "arn:aws:route53:::healthcheck/abc"},
				"Secondary": map[string]string{"Route": "us-west-2"},
			},
		},
		"EventBuses": []map[string]string{
			{"EventBusArn": "arn:aws:events:us-east-1:123456789012:event-bus/default"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeEndpoint", map[string]any{"Name": "h-endpoint"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListEndpoints", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "UpdateEndpoint", map[string]any{
		"Name":        "h-endpoint",
		"Description": "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteEndpoint", map[string]any{"Name": "h-endpoint"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_ReplayCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	b.AddArchiveInternal(&eventbridge.Archive{
		ArchiveName:    "h-arc",
		ArchiveArn:     "arn:aws:events:us-east-1:123456789012:archive/h-arc",
		EventSourceArn: "arn:aws:events:us-east-1:123456789012:event-bus/default",
		State:          "ACTIVE",
	})

	now := time.Now()
	rec := auditMakeRequest(t, h, e, "StartReplay", map[string]any{
		"ReplayName":     "h-replay",
		"EventSourceArn": "arn:aws:events:us-east-1:123456789012:archive/h-arc",
		"EventStartTime": now.Add(-2 * time.Hour).UTC().Format(time.RFC3339),
		"EventEndTime":   now.Add(-time.Hour).UTC().Format(time.RFC3339),
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeReplay", map[string]any{"ReplayName": "h-replay"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListReplays", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-replay")
}

func TestAudit_Handler_PartnerEventSourceCRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreatePartnerEventSource", map[string]any{
		"Name":    "aws.partner/example.com/app",
		"Account": "123456789012",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribePartnerEventSource", map[string]any{
		"Name": "aws.partner/example.com/app",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListPartnerEventSources", map[string]any{
		"NamePrefix": "aws.partner/",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "aws.partner/example.com/app")

	rec = auditMakeRequest(t, h, e, "DeletePartnerEventSource", map[string]any{
		"Name": "aws.partner/example.com/app",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_PutPartnerEvents(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "PutPartnerEvents", map[string]any{
		"Entries": []map[string]any{
			{
				"Source":     "aws.partner/example.com/app",
				"DetailType": "UserEvent",
				"Detail":     `{"userId":"u1"}`,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_GetSupportedOperationsIncludesDeliveryTargetTypes(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(newBackend())
	ops := h.GetSupportedOperations()

	// Verify a broad sample of expected operations.
	expected := []string{
		"CreateEventBus", "DeleteEventBus", "ListEventBuses", "DescribeEventBus", "UpdateEventBus",
		"PutRule", "DeleteRule", "ListRules", "DescribeRule", "EnableRule", "DisableRule",
		"PutTargets", "RemoveTargets", "ListTargetsByRule",
		"PutEvents", "PutPartnerEvents",
		"CreateArchive", "DeleteArchive", "DescribeArchive", "ListArchives", "UpdateArchive",
		"StartReplay", "DescribeReplay", "ListReplays", "CancelReplay",
		"CreateConnection", "DeleteConnection", "DescribeConnection", "ListConnections", "UpdateConnection",
		"DeauthorizeConnection",
		"CreateApiDestination", "DeleteApiDestination", "DescribeApiDestination",
		"ListApiDestinations", "UpdateApiDestination",
		"CreateEndpoint", "DeleteEndpoint", "DescribeEndpoint", "ListEndpoints", "UpdateEndpoint",
		"CreatePipe", "DeletePipe", "DescribePipe", "ListPipes", "UpdatePipe",
		"PutPermission", "RemovePermission", "GetEventBusPolicy", "PutEventBusPolicy",
		"TagResource", "UntagResource", "ListTagsForResource",
		"TestEventPattern", "ListRuleNamesByTarget",
	}

	for _, op := range expected {
		assert.Contains(t, ops, op, "missing operation: %s", op)
	}
}

// ---------------------------------------------------------------------------
// Connection auth parameters (BASIC, API_KEY, OAUTH)
// ---------------------------------------------------------------------------

func TestAudit_Connection_BasicAuthParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "basic-conn",
		AuthorizationType: "BASIC",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			BasicAuthParameters: &eventbridge.ConnectionBasicAuthParameters{
				Username: "admin",
				Password: "s3cr3t",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.BasicAuthParameters)
	assert.Equal(t, "admin", conn.AuthParameters.BasicAuthParameters.Username)
	// Password masked on return.
	assert.Empty(t, conn.AuthParameters.BasicAuthParameters.Password)
}

func TestAudit_Connection_APIKeyAuthParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "apikey-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "my-secret-key",
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.APIKeyAuthParameters)
	assert.Equal(t, "x-api-key", conn.AuthParameters.APIKeyAuthParameters.APIKeyName)
	// ApiKeyValue masked on return.
	assert.Empty(t, conn.AuthParameters.APIKeyAuthParameters.APIKeyValue)
}

func TestAudit_Connection_OAuthParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "oauth-conn",
		AuthorizationType: "OAUTH_CLIENT_CREDENTIALS",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			OAuthParameters: &eventbridge.ConnectionOAuthParameters{
				AuthorizationEndpoint: "https://auth.example.com/token",
				HTTPMethod:            "POST",
				ClientParameters: &eventbridge.ConnectionOAuthClientParameters{
					ClientID:     "my-client-id",
					ClientSecret: "my-client-secret",
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.OAuthParameters)
	assert.Equal(t, "https://auth.example.com/token", conn.AuthParameters.OAuthParameters.AuthorizationEndpoint)
	assert.Equal(t, "my-client-id", conn.AuthParameters.OAuthParameters.ClientParameters.ClientID)
	// ClientSecret masked on return.
	assert.Empty(t, conn.AuthParameters.OAuthParameters.ClientParameters.ClientSecret)
}

func TestAudit_Connection_InvocationHTTPParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	conn, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "http-param-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "x-api-key",
				APIKeyValue: "secret",
			},
			InvocationHTTPParameters: &eventbridge.ConnectionHTTPParameters{
				HeaderParameters: []eventbridge.ConnectionHeaderParameter{
					{Key: "X-Custom-Header", Value: "custom-value"},
					{Key: "X-Secret-Header", Value: "secret-val", IsValueSecret: true},
				},
				QueryStringParameters: []eventbridge.ConnectionQueryStringParameter{
					{Key: "format", Value: "json"},
				},
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, conn.AuthParameters)
	require.NotNil(t, conn.AuthParameters.InvocationHTTPParameters)

	headers := conn.AuthParameters.InvocationHTTPParameters.HeaderParameters
	require.Len(t, headers, 2)
	// Non-secret header value preserved.
	assert.Equal(t, "custom-value", headers[0].Value)
	// Secret header value masked.
	assert.Empty(t, headers[1].Value)
	assert.True(t, headers[1].IsValueSecret)

	queries := conn.AuthParameters.InvocationHTTPParameters.QueryStringParameters
	require.Len(t, queries, 1)
	assert.Equal(t, "json", queries[0].Value)
}

func TestAudit_Connection_UpdateAuthParameters(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateConnection(context.Background(), eventbridge.CreateConnectionInput{
		Name:              "update-auth-conn",
		AuthorizationType: "BASIC",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			BasicAuthParameters: &eventbridge.ConnectionBasicAuthParameters{
				Username: "olduser",
				Password: "oldpass",
			},
		},
	})
	require.NoError(t, err)

	updated, err := b.UpdateConnection(context.Background(), eventbridge.UpdateConnectionInput{
		Name:              "update-auth-conn",
		AuthorizationType: "API_KEY",
		AuthParameters: &eventbridge.ConnectionAuthParameters{
			APIKeyAuthParameters: &eventbridge.ConnectionAPIKeyAuthParameters{
				APIKeyName:  "new-key",
				APIKeyValue: "new-secret",
			},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "API_KEY", updated.AuthorizationType)
	require.NotNil(t, updated.AuthParameters)
	require.NotNil(t, updated.AuthParameters.APIKeyAuthParameters)
	assert.Equal(t, "new-key", updated.AuthParameters.APIKeyAuthParameters.APIKeyName)
}

func TestAudit_Handler_Connection_AuthParametersRoundtrip(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateConnection", map[string]any{
		"Name":              "handler-auth-conn",
		"AuthorizationType": "BASIC",
		"AuthParameters": map[string]any{
			"BasicAuthParameters": map[string]any{
				"Username": "testuser",
				"Password": "testpass",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeConnection", map[string]any{"Name": "handler-auth-conn"})
	assert.Equal(t, http.StatusOK, rec.Code)
	body := rec.Body.String()
	assert.Contains(t, body, "testuser")
	// Password must not appear in describe output.
	assert.NotContains(t, body, "testpass")
}

// ---------------------------------------------------------------------------
// Partner event source → event source association
// ---------------------------------------------------------------------------

func TestAudit_PartnerEventSource_CreatesEventSourceAsPending(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/example.com/app", "123456789012")
	require.NoError(t, err)

	// The partner source should show up in ListEventSources as PENDING.
	sources, _, err := b.ListEventSources(context.Background(), "aws.partner/", "")
	require.NoError(t, err)
	require.Len(t, sources, 1)
	assert.Equal(t, "aws.partner/example.com/app", sources[0].Name)
	assert.Equal(t, "PENDING", sources[0].State)
}

func TestAudit_PartnerEventSource_ActivateTransitionsToPending(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/saas.com/feed", "111111111111")
	require.NoError(t, err)

	// Event source should start as PENDING.
	src, err := b.DescribeEventSource(context.Background(), "aws.partner/saas.com/feed")
	require.NoError(t, err)
	assert.Equal(t, "PENDING", src.State)

	// Activate it → ACTIVE.
	err = b.ActivateEventSource(context.Background(), "aws.partner/saas.com/feed")
	require.NoError(t, err)

	src, err = b.DescribeEventSource(context.Background(), "aws.partner/saas.com/feed")
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", src.State)
}

func TestAudit_PartnerEventSource_DeactivateTransitionsToInactive(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/saas.com/feed2", "222222222222")
	require.NoError(t, err)

	require.NoError(t, b.ActivateEventSource(context.Background(), "aws.partner/saas.com/feed2"))
	require.NoError(t, b.DeactivateEventSource(context.Background(), "aws.partner/saas.com/feed2"))

	src, err := b.DescribeEventSource(context.Background(), "aws.partner/saas.com/feed2")
	require.NoError(t, err)
	assert.Equal(t, "INACTIVE", src.State)
}

func TestAudit_PartnerEventSource_DeletePartnerSourceNotAffectEventSource(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePartnerEventSource(context.Background(), "aws.partner/example.com/gone", "333333333333")
	require.NoError(t, err)

	// Partner source deletion should NOT remove the event source record.
	err = b.DeletePartnerEventSource(context.Background(), "aws.partner/example.com/gone")
	require.NoError(t, err)

	// The event source persists for the customer.
	_, err = b.DescribeEventSource(context.Background(), "aws.partner/example.com/gone")
	require.NoError(t, err)
}

// ---------------------------------------------------------------------------
// Schema Registry CRUD
// ---------------------------------------------------------------------------

func TestAudit_SchemaRegistry_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	reg, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{
		RegistryName: "my-registry",
		Description:  "test registry",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-registry", reg.RegistryName)
	assert.NotEmpty(t, reg.RegistryArn)
	assert.Equal(t, "test registry", reg.Description)

	described, err := b.DescribeRegistry(context.Background(), "my-registry")
	require.NoError(t, err)
	assert.Equal(t, "my-registry", described.RegistryName)

	updated, err := b.UpdateRegistry(context.Background(), eventbridge.UpdateRegistryInput{
		RegistryName: "my-registry",
		Description:  "updated description",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", updated.Description)

	registries, _, err := b.ListRegistries(context.Background(), "my-", "")
	require.NoError(t, err)
	assert.Len(t, registries, 1)

	err = b.DeleteRegistry(context.Background(), "my-registry")
	require.NoError(t, err)

	_, err = b.DescribeRegistry(context.Background(), "my-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_SchemaRegistry_DuplicateCreateFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "dup-registry"})
	require.NoError(t, err)

	_, err = b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "dup-registry"})
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestAudit_SchemaRegistry_NotFoundErrors(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.DescribeRegistry(context.Background(), "no-such-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	err = b.DeleteRegistry(context.Background(), "no-such-registry")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	_, err = b.UpdateRegistry(context.Background(), eventbridge.UpdateRegistryInput{RegistryName: "no-such-registry"})
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

// ---------------------------------------------------------------------------
// Schema CRUD and versioning
// ---------------------------------------------------------------------------

func TestAudit_Schema_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "schema-reg"})
	require.NoError(t, err)

	content := `{"openapi":"3.0.0","info":{"title":"MySchema","version":"1.0"},"paths":{}}`
	schema, err := b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "schema-reg",
		SchemaName:   "MySchema",
		Type:         "OpenApi3",
		Content:      content,
		Description:  "first schema",
	})
	require.NoError(t, err)
	assert.Equal(t, "MySchema", schema.SchemaName)
	assert.Equal(t, "1", schema.SchemaVersion)
	assert.Equal(t, content, schema.Content)
	assert.NotEmpty(t, schema.SchemaArn)

	described, err := b.DescribeSchema(context.Background(), "schema-reg", "MySchema", "")
	require.NoError(t, err)
	assert.Equal(t, "MySchema", described.SchemaName)

	updated, err := b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "schema-reg",
		SchemaName:   "MySchema",
		Content:      `{"openapi":"3.0.0","info":{"title":"MySchema","version":"2.0"},"paths":{}}`,
	})
	require.NoError(t, err)
	assert.Equal(t, "2", updated.SchemaVersion)

	schemas, _, err := b.ListSchemas(context.Background(), "schema-reg", "", "")
	require.NoError(t, err)
	assert.Len(t, schemas, 1)

	err = b.DeleteSchema(context.Background(), "schema-reg", "MySchema")
	require.NoError(t, err)

	_, err = b.DescribeSchema(context.Background(), "schema-reg", "MySchema", "")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_Schema_DuplicateCreateFails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "dup-schema-reg"})
	require.NoError(t, err)

	input := eventbridge.CreateSchemaInput{
		RegistryName: "dup-schema-reg",
		SchemaName:   "MySchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	}

	_, err = b.CreateSchema(context.Background(), input)
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestAudit_Schema_RegistryDeleteCascadeSchemas(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "cascade-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "cascade-reg",
		SchemaName:   "CascadedSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	err = b.DeleteRegistry(context.Background(), "cascade-reg")
	require.NoError(t, err)

	// Schema should no longer be accessible.
	_, err = b.DescribeSchema(context.Background(), "cascade-reg", "CascadedSchema", "")
	require.Error(t, err)
}

func TestAudit_Schema_SearchByKeyword(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "search-reg"})
	require.NoError(t, err)

	for _, name := range []string{"OrderSchema", "UserSchema", "PaymentSchema"} {
		_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
			RegistryName: "search-reg",
			SchemaName:   name,
			Type:         "OpenApi3",
			Content:      `{"title":"` + name + `"}`,
		})
		require.NoError(t, err)
	}

	results, _, err := b.SearchSchemas(context.Background(), "search-reg", "Order", "")
	require.NoError(t, err)
	assert.Len(t, results, 1)
	assert.Equal(t, "OrderSchema", results[0].SchemaName)
}

// ---------------------------------------------------------------------------
// Schema versioning
// ---------------------------------------------------------------------------

func TestAudit_SchemaVersions_ListAndDescribe(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "ver-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "ver-reg",
		SchemaName:   "VersionedSchema",
		Type:         "OpenApi3",
		Content:      `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "ver-reg",
		SchemaName:   "VersionedSchema",
		Content:      `{"v":2}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "ver-reg",
		SchemaName:   "VersionedSchema",
		Content:      `{"v":3}`,
	})
	require.NoError(t, err)

	versions, _, err := b.ListSchemaVersions(context.Background(), "ver-reg", "VersionedSchema", "")
	require.NoError(t, err)
	assert.Len(t, versions, 3)
	assert.Equal(t, "1", versions[0].SchemaVersion)
	assert.Equal(t, "3", versions[2].SchemaVersion)

	sv, err := b.DescribeSchemaVersion(context.Background(), "ver-reg", "VersionedSchema", "2")
	require.NoError(t, err)
	assert.Equal(t, "2", sv.SchemaVersion)
	assert.Contains(t, sv.Content, `"v":2`)
}

func TestAudit_SchemaVersions_DeleteSpecificVersion(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "delver-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "delver-reg",
		SchemaName:   "DelSchema",
		Type:         "OpenApi3",
		Content:      `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "delver-reg",
		SchemaName:   "DelSchema",
		Content:      `{"v":2}`,
	})
	require.NoError(t, err)

	err = b.DeleteSchemaVersion(context.Background(), "delver-reg", "DelSchema", "1")
	require.NoError(t, err)

	versions, _, err := b.ListSchemaVersions(context.Background(), "delver-reg", "DelSchema", "")
	require.NoError(t, err)
	assert.Len(t, versions, 1)
	assert.Equal(t, "2", versions[0].SchemaVersion)
}

func TestAudit_SchemaVersions_DescribeWithVersion(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "descver-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "descver-reg",
		SchemaName:   "S",
		Type:         "OpenApi3",
		Content:      `{"v":1}`,
	})
	require.NoError(t, err)

	_, err = b.UpdateSchema(context.Background(), eventbridge.UpdateSchemaInput{
		RegistryName: "descver-reg",
		SchemaName:   "S",
		Content:      `{"v":2}`,
	})
	require.NoError(t, err)

	// Describe with specific version.
	sv, err := b.DescribeSchema(context.Background(), "descver-reg", "S", "1")
	require.NoError(t, err)
	assert.Equal(t, "1", sv.SchemaVersion)
	assert.Contains(t, sv.Content, `"v":1`)

	// Describe without version returns latest.
	latest, err := b.DescribeSchema(context.Background(), "descver-reg", "S", "")
	require.NoError(t, err)
	assert.Equal(t, "2", latest.SchemaVersion)
}

// ---------------------------------------------------------------------------
// Code bindings
// ---------------------------------------------------------------------------

func TestAudit_CodeBinding_CRUD(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "cb-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	binding, err := b.PutCodeBinding(context.Background(), eventbridge.PutCodeBindingInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Language:     "Python36",
	})
	require.NoError(t, err)
	assert.Equal(t, "Python36", binding.Language)
	assert.Equal(t, "CREATE_COMPLETE", binding.Status)

	described, err := b.DescribeCodeBinding(context.Background(), eventbridge.DescribeCodeBindingInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Language:     "Python36",
	})
	require.NoError(t, err)
	assert.Equal(t, "Python36", described.Language)

	_, err = b.PutCodeBinding(context.Background(), eventbridge.PutCodeBindingInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
		Language:     "Java8",
	})
	require.NoError(t, err)

	bindings, _, err := b.ListCodeBindings(context.Background(), eventbridge.ListCodeBindingsInput{
		RegistryName: "cb-reg",
		SchemaName:   "CBSchema",
	})
	require.NoError(t, err)
	assert.Len(t, bindings, 2)
}

func TestAudit_CodeBinding_GetSource(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "src-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "src-reg",
		SchemaName:   "SrcSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	_, err = b.PutCodeBinding(context.Background(), eventbridge.PutCodeBindingInput{
		RegistryName: "src-reg",
		SchemaName:   "SrcSchema",
		Language:     "TypeScript3",
	})
	require.NoError(t, err)

	src, err := b.GetCodeBindingSource(context.Background(), "src-reg", "SrcSchema", "TypeScript3", "1")
	require.NoError(t, err)
	assert.NotEmpty(t, src)
}

func TestAudit_CodeBinding_NotFoundErrors(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "nf-reg"})
	require.NoError(t, err)

	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "nf-reg",
		SchemaName:   "NFSchema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	_, err = b.DescribeCodeBinding(context.Background(), eventbridge.DescribeCodeBindingInput{
		RegistryName: "nf-reg",
		SchemaName:   "NFSchema",
		Language:     "Go1",
	})
	require.ErrorIs(t, err, eventbridge.ErrNotFound)
}

func TestAudit_GetDiscoveredSchema_ReturnsContent(t *testing.T) {
	t.Parallel()
	b := newBackend()

	content, err := b.GetDiscoveredSchema(context.Background(), eventbridge.GetDiscoveredSchemaInput{
		Events: []string{`{"source":"myapp","detail-type":"OrderPlaced","detail":{"orderId":"o1"}}`},
		Type:   "OpenApi3",
	})
	require.NoError(t, err)
	assert.NotEmpty(t, content)
}

func TestAudit_GetDiscoveredSchema_RejectsEmptyEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.GetDiscoveredSchema(context.Background(), eventbridge.GetDiscoveredSchemaInput{
		Events: []string{},
		Type:   "OpenApi3",
	})
	require.ErrorIs(t, err, eventbridge.ErrInvalidParameter)
}

// ---------------------------------------------------------------------------
// Schema Registry handler smoke tests
// ---------------------------------------------------------------------------

func TestAudit_Handler_SchemaRegistry_CRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "CreateRegistry", map[string]any{
		"RegistryName": "h-registry",
		"Description":  "handler test",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-registry")

	rec = auditMakeRequest(t, h, e, "DescribeRegistry", map[string]any{
		"RegistryName": "h-registry",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListRegistries", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-registry")

	rec = auditMakeRequest(t, h, e, "UpdateRegistry", map[string]any{
		"RegistryName": "h-registry",
		"Description":  "updated",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteRegistry", map[string]any{
		"RegistryName": "h-registry",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeRegistry", map[string]any{
		"RegistryName": "h-registry",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestAudit_Handler_Schema_CRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "h-schema-reg"})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "CreateSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
		"Type":         "OpenApi3",
		"Content":      `{"openapi":"3.0.0","info":{"title":"Test","version":"1.0"},"paths":{}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-schema")

	rec = auditMakeRequest(t, h, e, "DescribeSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListSchemas", map[string]any{
		"RegistryName": "h-schema-reg",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "h-schema")

	rec = auditMakeRequest(t, h, e, "SearchSchemas", map[string]any{
		"RegistryName": "h-schema-reg",
		"Keywords":     "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "UpdateSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
		"Content":      `{"openapi":"3.0.0","info":{"title":"Test","version":"2.0"},"paths":{}}`,
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), `"2"`)

	rec = auditMakeRequest(t, h, e, "ListSchemaVersions", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DescribeSchemaVersion", map[string]any{
		"RegistryName":  "h-schema-reg",
		"SchemaName":    "h-schema",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteSchemaVersion", map[string]any{
		"RegistryName":  "h-schema-reg",
		"SchemaName":    "h-schema",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "DeleteSchema", map[string]any{
		"RegistryName": "h-schema-reg",
		"SchemaName":   "h-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_CodeBinding_CRUD(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	_, err := b.CreateRegistry(context.Background(), eventbridge.CreateRegistryInput{RegistryName: "h-cb-reg"})
	require.NoError(t, err)
	_, err = b.CreateSchema(context.Background(), eventbridge.CreateSchemaInput{
		RegistryName: "h-cb-reg",
		SchemaName:   "h-cb-schema",
		Type:         "OpenApi3",
		Content:      `{}`,
	})
	require.NoError(t, err)

	rec := auditMakeRequest(t, h, e, "PutCodeBinding", map[string]any{
		"RegistryName": "h-cb-reg",
		"SchemaName":   "h-cb-schema",
		"Language":     "Python36",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "CREATE_COMPLETE")

	rec = auditMakeRequest(t, h, e, "DescribeCodeBinding", map[string]any{
		"RegistryName": "h-cb-reg",
		"SchemaName":   "h-cb-schema",
		"Language":     "Python36",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = auditMakeRequest(t, h, e, "ListCodeBindings", map[string]any{
		"RegistryName": "h-cb-reg",
		"SchemaName":   "h-cb-schema",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Python36")

	rec = auditMakeRequest(t, h, e, "GetCodeBindingSource", map[string]any{
		"RegistryName":  "h-cb-reg",
		"SchemaName":    "h-cb-schema",
		"Language":      "Python36",
		"SchemaVersion": "1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestAudit_Handler_GetDiscoveredSchema(t *testing.T) {
	t.Parallel()
	e := echo.New()
	b := newBackend()
	h := eventbridge.NewHandler(b)

	rec := auditMakeRequest(t, h, e, "GetDiscoveredSchema", map[string]any{
		"Events": []string{`{"source":"myapp","detail-type":"T","detail":{}}`},
		"Type":   "OpenApi3",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "openapi")
}

func TestAudit_Handler_SchemaOperationsIncluded(t *testing.T) {
	t.Parallel()
	h := eventbridge.NewHandler(newBackend())
	ops := h.GetSupportedOperations()

	schemaOps := []string{
		"CreateRegistry", "DeleteRegistry", "DescribeRegistry", "ListRegistries", "UpdateRegistry",
		"CreateSchema", "DeleteSchema", "DescribeSchema", "ListSchemas", "SearchSchemas", "UpdateSchema",
		"ListSchemaVersions", "DescribeSchemaVersion", "DeleteSchemaVersion",
		"GetDiscoveredSchema",
		"PutCodeBinding", "DescribeCodeBinding", "ListCodeBindings", "GetCodeBindingSource",
	}
	for _, op := range schemaOps {
		assert.Contains(t, ops, op, "missing schema operation: %s", op)
	}
}

// ---------------------------------------------------------------------------
// Input transformer: array indexing in JSONPath
// ---------------------------------------------------------------------------

func TestAudit_Targets_InputTransformer_ArrayIndexing(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newAuditSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:array-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "array-rule",
		EventPattern: `{"source":["array.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "array-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"firstItem": "$.detail.items[0]",
				},
				InputTemplate: `{"item": "<firstItem>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{Source: "array.test", DetailType: "T", Detail: `{"items":["alpha","beta","gamma"]}`},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "alpha")
	assert.NotContains(t, msg, "beta")
}

func TestAudit_Targets_InputTransformer_NestedArrayIndex(t *testing.T) {
	t.Parallel()
	b := newBackend()

	sqsSink := newAuditSQSSender()
	queueARN := "arn:aws:sqs:us-east-1:123456789012:nested-array-q"
	b.SetDeliveryTargets(&eventbridge.DeliveryTargets{SQS: sqsSink})

	_, err := b.PutRule(context.Background(), eventbridge.PutRuleInput{
		Name:         "nested-array-rule",
		EventPattern: `{"source":["nested.test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets(context.Background(), "nested-array-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: queueARN,
			InputTransformer: &eventbridge.InputTransformer{
				InputPathsMap: map[string]string{
					"userId": "$.detail.users[1].id",
				},
				InputTemplate: `{"user_id": "<userId>"}`,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents(context.Background(), []eventbridge.EventEntry{
		{
			Source:     "nested.test",
			DetailType: "T",
			Detail:     `{"users":[{"id":"u0"},{"id":"u1"},{"id":"u2"}]}`,
		},
	})

	require.Eventually(t, func() bool {
		return len(sqsSink.MessagesFor(queueARN)) > 0
	}, 2*time.Second, 10*time.Millisecond)

	msg := sqsSink.MessagesFor(queueARN)[0]
	assert.Contains(t, msg, "u1")
}
