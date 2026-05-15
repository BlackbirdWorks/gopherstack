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
		_, err := b.CreateEventBus(fmt.Sprintf("bus-%d", i), "")
		require.NoError(t, err, "bus %d should be created", i)
	}

	_, err := b.CreateEventBus("bus-overflow", "")
	require.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}

func TestAudit_PutRule_EnforcesPerBusLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	const limit = 300
	for i := range limit {
		_, err := b.PutRule(eventbridge.PutRuleInput{
			Name:         fmt.Sprintf("rule-%d", i),
			EventPattern: `{"source":["x"]}`,
			State:        "ENABLED",
		})
		require.NoError(t, err, "rule %d should be created", i)
	}

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "rule-overflow",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.ErrorIs(t, err, eventbridge.ErrResourceLimitExceeded)
}

func TestAudit_PutRule_UpdateExistingDoesNotCountAgainstLimit(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "my-rule",
		EventPattern: `{"source":["x"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	for range 5 {
		_, err = b.PutRule(eventbridge.PutRuleInput{
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
			_, err := b.PutRule(eventbridge.PutRuleInput{
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
			_, err := b.PutRule(eventbridge.PutRuleInput{
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

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "managed-rule",
		EventPattern: `{"source":["x"]}`,
		ManagedBy:    "scheduler.amazonaws.com",
	})
	require.NoError(t, err)

	rule, err := b.DescribeRule("managed-rule", "")
	require.NoError(t, err)
	assert.Equal(t, "scheduler.amazonaws.com", rule.ManagedBy)
}

// ---------------------------------------------------------------------------
// Issues 1-3: Target RetryPolicy, DeadLetterConfig, BatchParameters
// ---------------------------------------------------------------------------

func TestAudit_Target_RetryPolicyStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets("r", "", []eventbridge.Target{
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

	targets, _, err := b.ListTargetsByRule("r", "", "")
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].RetryPolicy)
	assert.Equal(t, 5, targets[0].RetryPolicy.MaximumRetryAttempts)
	assert.Equal(t, 600, targets[0].RetryPolicy.MaximumEventAgeInSeconds)
}

func TestAudit_Target_DeadLetterConfigStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	dlqARN := "arn:aws:sqs:us-east-1:123456789012:dlq"
	_, err = b.PutTargets("r", "", []eventbridge.Target{
		{
			ID:               "t1",
			Arn:              "arn:aws:lambda:us-east-1:123456789012:function:fn",
			DeadLetterConfig: &eventbridge.DeadLetterConfig{Arn: dlqARN},
		},
	})
	require.NoError(t, err)

	targets, _, err := b.ListTargetsByRule("r", "", "")
	require.NoError(t, err)
	require.Len(t, targets, 1)
	require.NotNil(t, targets[0].DeadLetterConfig)
	assert.Equal(t, dlqARN, targets[0].DeadLetterConfig.Arn)
}

func TestAudit_Target_BatchParametersStored(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets("r", "", []eventbridge.Target{
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

	targets, _, err := b.ListTargetsByRule("r", "", "")
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

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "r",
		EventPattern: `{"source":["x"]}`,
	})
	require.NoError(t, err)

	// Valid keys accepted.
	failed, err := b.PutTargets("r", "", []eventbridge.Target{
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
	failed, err = b.PutTargets("r", "", []eventbridge.Target{
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

	err := b.PutPermission(eventbridge.PutPermissionInput{
		StatementID: "allow-account-123",
		Action:      "events:PutEvents",
		Principal:   "123456789013",
	})
	require.NoError(t, err)

	policy, err := b.GetEventBusPolicy("")
	require.NoError(t, err)
	assert.Contains(t, policy, "allow-account-123")
}

func TestAudit_RemovePermission_RemovesStatement(t *testing.T) {
	t.Parallel()
	b := newBackend()

	require.NoError(t, b.PutPermission(eventbridge.PutPermissionInput{
		StatementID: "stmt-1",
		Action:      "events:PutEvents",
		Principal:   "111111111111",
	}))
	require.NoError(t, b.PutPermission(eventbridge.PutPermissionInput{
		StatementID: "stmt-2",
		Action:      "events:PutEvents",
		Principal:   "222222222222",
	}))

	require.NoError(t, b.RemovePermission(eventbridge.RemovePermissionInput{StatementID: "stmt-1"}))

	policy, err := b.GetEventBusPolicy("")
	require.NoError(t, err)
	assert.NotContains(t, policy, "stmt-1")
	assert.Contains(t, policy, "stmt-2")
}

func TestAudit_RemovePermission_RemoveAll(t *testing.T) {
	t.Parallel()
	b := newBackend()

	require.NoError(t, b.PutPermission(eventbridge.PutPermissionInput{
		StatementID: "stmt-1",
		Action:      "events:PutEvents",
		Principal:   "111111111111",
	}))

	require.NoError(t, b.RemovePermission(eventbridge.RemovePermissionInput{
		RemoveAllPermissions: true,
	}))

	policy, err := b.GetEventBusPolicy("")
	require.NoError(t, err)
	assert.Empty(t, policy)
}

func TestAudit_GetEventBusPolicy_BusNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.GetEventBusPolicy("nonexistent")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

func TestAudit_PutEventBusPolicy_ReplacePolicy(t *testing.T) {
	t.Parallel()
	b := newBackend()

	policyJSON := `[{"Sid":"s1","Effect":"Allow","Action":"events:PutEvents","Principal":"123"}]`
	err := b.PutEventBusPolicy(eventbridge.PutEventBusPolicyInput{Policy: policyJSON})
	require.NoError(t, err)

	policy, err := b.GetEventBusPolicy("")
	require.NoError(t, err)
	assert.Contains(t, policy, "s1")
}

func TestAudit_PutPermission_BusNotFound(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.PutPermission(eventbridge.PutPermissionInput{
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

	pipe, err := b.CreatePipe(eventbridge.CreatePipeInput{
		Name:      "my-pipe",
		SourceArn: "arn:aws:sqs:us-east-1:123456789012:source-queue",
		TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:my-fn",
		RoleArn:   "arn:aws:iam::123456789012:role/my-pipe-role",
	})
	require.NoError(t, err)
	assert.Equal(t, "my-pipe", pipe.Name)
	assert.NotEmpty(t, pipe.Arn)
	assert.Equal(t, "CREATING", pipe.CurrentState)

	described, err := b.DescribePipe("my-pipe")
	require.NoError(t, err)
	assert.Equal(t, "my-pipe", described.Name)

	pipes, _, err := b.ListPipes("", "")
	require.NoError(t, err)
	require.Len(t, pipes, 1)

	updated, err := b.UpdatePipe(eventbridge.UpdatePipeInput{
		Name:        "my-pipe",
		Description: "updated description",
	})
	require.NoError(t, err)
	assert.Equal(t, "updated description", updated.Description)

	err = b.DeletePipe("my-pipe")
	require.NoError(t, err)

	_, err = b.DescribePipe("my-pipe")
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
			_, err := b.CreatePipe(tt.input)
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

	_, err := b.CreatePipe(input)
	require.NoError(t, err)

	_, err = b.CreatePipe(input)
	require.ErrorIs(t, err, eventbridge.ErrAlreadyExists)
}

func TestAudit_Pipe_ListFiltersPrefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	for _, name := range []string{"foo-1", "foo-2", "bar-1"} {
		_, err := b.CreatePipe(eventbridge.CreatePipeInput{
			Name:      name,
			SourceArn: "arn:aws:sqs:us-east-1:123456789012:q",
			TargetArn: "arn:aws:lambda:us-east-1:123456789012:function:f",
			RoleArn:   "arn:aws:iam::123456789012:role/r",
		})
		require.NoError(t, err)
	}

	pipes, _, err := b.ListPipes("foo-", "")
	require.NoError(t, err)
	assert.Len(t, pipes, 2)
}

func TestAudit_Pipe_DesiredStatePreserved(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreatePipe(eventbridge.CreatePipeInput{
		Name:         "my-pipe",
		SourceArn:    "arn:aws:sqs:us-east-1:123456789012:q",
		TargetArn:    "arn:aws:lambda:us-east-1:123456789012:function:f",
		RoleArn:      "arn:aws:iam::123456789012:role/r",
		DesiredState: "STOPPED",
	})
	require.NoError(t, err)

	p, err := b.DescribePipe("my-pipe")
	require.NoError(t, err)
	assert.Equal(t, "STOPPED", p.DesiredState)
}

// ---------------------------------------------------------------------------
// Issue 10: Archive retention + archived events pruning
// ---------------------------------------------------------------------------

func TestAudit_ArchiveJanitor_PrunesArchivedEvents(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus("my-bus", "")
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/my-bus"
	_, err = b.CreateArchive(eventbridge.CreateArchiveInput{
		ArchiveName:    "my-archive",
		EventSourceArn: busARN,
		RetentionDays:  1,
	})
	require.NoError(t, err)

	b.PutEvents([]eventbridge.EventEntry{
		{Source: "test", DetailType: "Test", Detail: `{}`, EventBusName: "my-bus"},
	})

	// Make the archive look old enough to expire.
	err = b.SetArchiveCreationTimeForTest("my-archive", time.Now().Add(-48*time.Hour))
	require.NoError(t, err)

	janitor := eventbridge.NewArchiveJanitor(b, time.Hour)
	janitor.SetNow(time.Now())
	janitor.SweepOnce(context.Background())

	_, err = b.DescribeArchive("my-archive")
	require.ErrorIs(t, err, eventbridge.ErrNotFound)

	assert.Equal(t, 0, b.ArchivedEventCount("my-archive"))
}

func TestAudit_ArchiveJanitor_RetentionDaysZeroNeverExpires(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus("bus2", "")
	require.NoError(t, err)

	busARN := "arn:aws:events:us-east-1:123456789012:event-bus/bus2"
	_, err = b.CreateArchive(eventbridge.CreateArchiveInput{
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

	_, err = b.DescribeArchive("forever-archive")
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

	_, err := b.StartReplay(eventbridge.StartReplayInput{
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

	replay, err := b.StartReplay(eventbridge.StartReplayInput{
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

	replay, err := b.StartReplay(eventbridge.StartReplayInput{
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

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "rule",
		EventPattern: `{"source":["dlq-test"]}`,
	})
	require.NoError(t, err)

	_, err = b.PutTargets("rule", "", []eventbridge.Target{
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

	b.PutEvents([]eventbridge.EventEntry{
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

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "retry-rule",
		EventPattern: `{"source":["retry-test"]}`,
	})
	require.NoError(t, err)

	targetARN := "arn:aws:sqs:us-east-1:123456789012:target-q"
	_, err = b.PutTargets("retry-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: targetARN,
			RetryPolicy: &eventbridge.RetryPolicy{
				MaximumRetryAttempts: 0,
			},
		},
	})
	require.NoError(t, err)

	b.PutEvents([]eventbridge.EventEntry{
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

	_, err := b.PutRule(eventbridge.PutRuleInput{
		Name:         "default-retry-rule",
		EventPattern: `{"source":["default-retry"]}`,
	})
	require.NoError(t, err)

	targetARN := "arn:aws:sqs:us-east-1:123456789012:target-q2"
	_, err = b.PutTargets("default-retry-rule", "", []eventbridge.Target{
		{
			ID:  "t1",
			Arn: targetARN,
			// No RetryPolicy set → use defaults (2 retries = 3 total attempts).
		},
	})
	require.NoError(t, err)

	b.PutEvents([]eventbridge.EventEntry{
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
		_, err := b.CreateEventBus(fmt.Sprintf("bus-%d", i), "")
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
