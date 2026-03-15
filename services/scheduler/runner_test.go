package scheduler_test

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/scheduler"
)

// --- mock target implementations ---

type mockLambdaInvoker struct {
	err   error
	calls []string
	mu    sync.Mutex
}

func (m *mockLambdaInvoker) InvokeFunction(_ context.Context, name, _ string, _ []byte) ([]byte, int, error) {
	m.mu.Lock()
	m.calls = append(m.calls, name)
	m.mu.Unlock()

	return nil, 200, m.err
}

func (m *mockLambdaInvoker) Called() []string {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]string, len(m.calls))
	copy(cp, m.calls)

	return cp
}

type mockSQSSender struct {
	err  error
	arns []string
	mu   sync.Mutex
}

func (m *mockSQSSender) SendMessageToQueue(_ context.Context, queueARN, _ string) error {
	m.mu.Lock()
	m.arns = append(m.arns, queueARN)
	m.mu.Unlock()

	return m.err
}

type mockSNSPublisher struct {
	err    error
	topics []string
	mu     sync.Mutex
}

func (m *mockSNSPublisher) PublishToTopic(_ context.Context, topicARN, _ string) error {
	m.mu.Lock()
	m.topics = append(m.topics, topicARN)
	m.mu.Unlock()

	return m.err
}

type mockSFNStarter struct {
	err  error
	arns []string
	mu   sync.Mutex
}

func (m *mockSFNStarter) StartExecution(arn, _, _ string) error {
	m.mu.Lock()
	m.arns = append(m.arns, arn)
	m.mu.Unlock()

	return m.err
}

// --- helper ---

func newTestBackendWithSchedule(t *testing.T, name, expr, targetARN, state string) *scheduler.InMemoryBackend {
	t.Helper()

	backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := backend.CreateSchedule(
		name,
		expr,
		scheduler.Target{ARN: targetARN, RoleARN: "arn:aws:iam::000000000000:role/r"},
		state,
		scheduler.FlexibleTimeWindow{Mode: "OFF"},
	)
	require.NoError(t, err)

	return backend
}

// --- tests ---

// TestScheduler_ParseRateExpression tests all supported rate expression formats.
func TestScheduler_ParseRateExpression(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		expr    string
		wantDur time.Duration
		wantErr bool
	}{
		{
			name:    "rate minutes",
			expr:    "rate(5 minutes)",
			wantDur: 5 * time.Minute,
		},
		{
			name:    "rate minute singular",
			expr:    "rate(1 minute)",
			wantDur: 1 * time.Minute,
		},
		{
			name:    "rate hours",
			expr:    "rate(2 hours)",
			wantDur: 2 * time.Hour,
		},
		{
			name:    "rate hour singular",
			expr:    "rate(1 hour)",
			wantDur: 1 * time.Hour,
		},
		{
			name:    "rate days",
			expr:    "rate(3 days)",
			wantDur: 3 * 24 * time.Hour,
		},
		{
			name:    "rate day singular",
			expr:    "rate(1 day)",
			wantDur: 24 * time.Hour,
		},
		{
			name:    "rate seconds (local testing)",
			expr:    "rate(30 seconds)",
			wantDur: 30 * time.Second,
		},
		{
			name:    "invalid - too many fields",
			expr:    "rate(5 minutes extra)",
			wantErr: true,
		},
		{
			name:    "invalid - unknown unit",
			expr:    "rate(5 weeks)",
			wantErr: true,
		},
		{
			name:    "invalid - non-numeric value",
			expr:    "rate(abc minutes)",
			wantErr: true,
		},
		{
			name:    "invalid - zero value",
			expr:    "rate(0 minutes)",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			got, err := scheduler.ParseRateExpression(tt.expr)

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.wantDur, got)
		})
	}
}

// TestScheduler_Runner_RateFiresOnFirstPoll tests that a rate-based schedule fires on the first poll.
func TestScheduler_Runner_RateFiresOnFirstPoll(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	backend := newTestBackendWithSchedule(t, "my-schedule", "rate(1 minute)", lambdaARN, "ENABLED")

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	// Manually call checkAndFireSchedules with a fixed time (simulates first poll - no lastFiredAt)
	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	calls := invoker.Called()
	require.Len(t, calls, 1)
	assert.Equal(t, "my-fn", calls[0])
}

// TestScheduler_Runner_RateRespectsCooldown tests that a rate schedule does not fire twice within the interval.
func TestScheduler_Runner_RateRespectsCooldown(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	backend := newTestBackendWithSchedule(t, "rate-sched", "rate(5 minutes)", lambdaARN, "ENABLED")

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	now := time.Now()

	// First poll - should fire
	scheduler.CheckAndFireSchedules(t.Context(), runner, now)
	require.Len(t, invoker.Called(), 1, "should fire on first poll")

	// Immediately after - should NOT fire again (interval not elapsed)
	scheduler.CheckAndFireSchedules(t.Context(), runner, now.Add(1*time.Second))
	assert.Len(t, invoker.Called(), 1, "should not fire again within interval")

	// After interval elapsed - should fire again
	scheduler.CheckAndFireSchedules(t.Context(), runner, now.Add(6*time.Minute))
	assert.Len(t, invoker.Called(), 2, "should fire again after interval")
}

// TestScheduler_Runner_DisabledScheduleSkipped tests that disabled schedules are not fired.
func TestScheduler_Runner_DisabledScheduleSkipped(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:my-fn"
	backend := newTestBackendWithSchedule(t, "disabled-sched", "rate(1 minute)", lambdaARN, "DISABLED")

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	assert.Empty(t, invoker.Called(), "disabled schedule should not fire")
}

// TestScheduler_Runner_SQSTarget tests that SQS targets receive a message when a schedule fires.
func TestScheduler_Runner_SQSTarget(t *testing.T) {
	t.Parallel()

	queueARN := "arn:aws:sqs:us-east-1:000000000000:my-queue"
	backend := newTestBackendWithSchedule(t, "sqs-sched", "rate(1 minute)", queueARN, "ENABLED")

	sender := &mockSQSSender{}
	runner := scheduler.NewRunner(backend)
	runner.SetSQSSender(sender)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	sender.mu.Lock()
	arns := sender.arns
	sender.mu.Unlock()

	require.Len(t, arns, 1)
	assert.Equal(t, queueARN, arns[0])
}

// TestScheduler_Runner_SNSTarget tests that SNS targets receive a notification when a schedule fires.
func TestScheduler_Runner_SNSTarget(t *testing.T) {
	t.Parallel()

	topicARN := "arn:aws:sns:us-east-1:000000000000:my-topic"
	backend := newTestBackendWithSchedule(t, "sns-sched", "rate(1 minute)", topicARN, "ENABLED")

	pub := &mockSNSPublisher{}
	runner := scheduler.NewRunner(backend)
	runner.SetSNSPublisher(pub)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	pub.mu.Lock()
	topics := pub.topics
	pub.mu.Unlock()

	require.Len(t, topics, 1)
	assert.Equal(t, topicARN, topics[0])
}

// TestScheduler_Runner_SFNTarget tests that StepFunctions targets receive a start execution when a schedule fires.
func TestScheduler_Runner_SFNTarget(t *testing.T) {
	t.Parallel()

	sfnARN := "arn:aws:states:us-east-1:000000000000:stateMachine:my-sm"
	backend := newTestBackendWithSchedule(t, "sfn-sched", "rate(1 minute)", sfnARN, "ENABLED")

	starter := &mockSFNStarter{}
	runner := scheduler.NewRunner(backend)
	runner.SetStepFunctionsStarter(starter)

	scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

	starter.mu.Lock()
	arns := starter.arns
	starter.mu.Unlock()

	require.Len(t, arns, 1)
	assert.Equal(t, sfnARN, arns[0])
}

// TestScheduler_Runner_CronMatches tests basic cron expression matching.
func TestScheduler_Runner_CronMatches(t *testing.T) {
	t.Parallel()

	// Create a schedule for exactly 12:00 UTC on any day
	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:noon-fn"
	backend := newTestBackendWithSchedule(t, "noon-sched", "cron(0 12 * * ? *)", lambdaARN, "ENABLED")

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	// Time that matches: Monday 2024-01-15 12:00:00 UTC
	matchTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
	scheduler.CheckAndFireSchedules(t.Context(), runner, matchTime)
	require.Len(t, invoker.Called(), 1, "should fire at 12:00")

	// Time that does NOT match: 12:01
	noMatchTime := time.Date(2024, 1, 15, 12, 1, 0, 0, time.UTC)
	scheduler.CheckAndFireSchedules(t.Context(), runner, noMatchTime)
	assert.Len(t, invoker.Called(), 1, "should not fire at 12:01")
}

// TestScheduler_Runner_StartAndShutdown tests that the runner goroutine starts and stops cleanly.
func TestScheduler_Runner_StartAndShutdown(t *testing.T) {
	t.Parallel()

	backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
	runner := scheduler.NewRunner(backend)

	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()

	runner.Start(ctx)
	<-ctx.Done()
	// No panic - goroutine should have stopped cleanly
}
