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
		"",
		expr,
		"",
		"",
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

// TestScheduler_Runner_TargetInput tests that a schedule with Target.Input sends the custom
// payload to every supported target type instead of the default scheduler event.
func TestScheduler_Runner_TargetInput(t *testing.T) {
	t.Parallel()

	const customInput = `{"key":"custom-value"}`
	const role = "arn:aws:iam::000000000000:role/r"

	tests := []struct {
		setup     func(runner *scheduler.Runner) func() string
		name      string
		targetARN string
		wantBody  string
	}{
		{
			name:      "Lambda target",
			targetARN: "arn:aws:lambda:us-east-1:000000000000:function:my-fn",
			setup: func(r *scheduler.Runner) func() string {
				inv := &mockLambdaInvokerWithPayload{}
				r.SetLambdaInvoker(inv)

				return func() string {
					calls := inv.Called()
					if len(calls) == 0 {
						return ""
					}

					return string(calls[0].payload)
				}
			},
		},
		{
			name:      "SQS target",
			targetARN: "arn:aws:sqs:us-east-1:000000000000:my-queue",
			setup: func(r *scheduler.Runner) func() string {
				m := &mockSQSSenderWithPayload{}
				r.SetSQSSender(m)

				return func() string { return m.Last() }
			},
		},
		{
			name:      "SNS target",
			targetARN: "arn:aws:sns:us-east-1:000000000000:my-topic",
			setup: func(r *scheduler.Runner) func() string {
				m := &mockSNSPublisherWithPayload{}
				r.SetSNSPublisher(m)

				return func() string { return m.Last() }
			},
		},
		{
			name:      "SFN target",
			targetARN: "arn:aws:states:us-east-1:000000000000:stateMachine:my-sm",
			setup: func(r *scheduler.Runner) func() string {
				m := &mockSFNStarterWithPayload{}
				r.SetStepFunctionsStarter(m)

				return func() string { return m.Last() }
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateSchedule(
				"custom-input-sched",
				"",
				"rate(1 second)",
				"",
				"",
				scheduler.Target{ARN: tt.targetARN, RoleARN: role, Input: customInput},
				"ENABLED",
				scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			runner := scheduler.NewRunner(backend)
			getPayload := tt.setup(runner)

			scheduler.CheckAndFireSchedules(t.Context(), runner, time.Now())

			assert.JSONEq(t, customInput, getPayload(), "custom Input should be sent verbatim to %s", tt.name)
		})
	}
}

// TestScheduler_Runner_CronRangeAndStep tests range and step cron expressions.
func TestScheduler_Runner_CronRangeAndStep(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:range-fn"

	tests := []struct {
		matchAt   time.Time
		noMatchAt time.Time
		name      string
		cronExpr  string
	}{
		{
			name:      "range in minutes",
			cronExpr:  "cron(0-5 * * * ? *)",
			matchAt:   time.Date(2024, 1, 15, 10, 3, 0, 0, time.UTC),
			noMatchAt: time.Date(2024, 1, 15, 10, 7, 0, 0, time.UTC),
		},
		{
			name:      "step in minutes",
			cronExpr:  "cron(*/15 * * * ? *)",
			matchAt:   time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC),
			noMatchAt: time.Date(2024, 1, 15, 10, 31, 0, 0, time.UTC),
		},
		{
			name:      "month name alias",
			cronExpr:  "cron(0 0 1 JAN ? *)",
			matchAt:   time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC),
			noMatchAt: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC),
		},
		{
			name:      "day-of-week name alias MON",
			cronExpr:  "cron(0 9 ? * MON *)",
			matchAt:   time.Date(2024, 1, 15, 9, 0, 0, 0, time.UTC), // 2024-01-15 is a Monday
			noMatchAt: time.Date(2024, 1, 16, 9, 0, 0, 0, time.UTC), // Tuesday
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateSchedule(
				tt.name,
				"",
				tt.cronExpr,
				"",
				"",
				scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::000000000000:role/r"},
				"ENABLED",
				scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			invoker := &mockLambdaInvoker{}
			runner := scheduler.NewRunner(backend)
			runner.SetLambdaInvoker(invoker)

			scheduler.CheckAndFireSchedules(t.Context(), runner, tt.matchAt)
			require.Len(t, invoker.Called(), 1, "should fire at match time")

			invoker2 := &mockLambdaInvoker{}
			runner2 := scheduler.NewRunner(backend)
			runner2.SetLambdaInvoker(invoker2)
			scheduler.CheckAndFireSchedules(t.Context(), runner2, tt.noMatchAt)
			assert.Empty(t, invoker2.Called(), "should not fire at no-match time")
		})
	}
}

// TestScheduler_Runner_LastFiredAtCleanup tests that lastFiredAt entries are swept when
// a schedule is deleted.
func TestScheduler_Runner_LastFiredAtCleanup(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:cleanup-fn"
	backend := newTestBackendWithSchedule(t, "sweep-sched", "rate(1 second)", lambdaARN, "ENABLED")

	invoker := &mockLambdaInvoker{}
	runner := scheduler.NewRunner(backend)
	runner.SetLambdaInvoker(invoker)

	now := time.Now()
	scheduler.CheckAndFireSchedules(t.Context(), runner, now)
	require.Len(t, invoker.Called(), 1, "schedule should have fired")

	assert.Equal(t, 1, scheduler.LastFiredAtLen(runner), "lastFiredAt should have one entry")

	// Delete the schedule.
	require.NoError(t, backend.DeleteSchedule("sweep-sched", ""))

	// Fire again: the stale entry should be swept.
	scheduler.CheckAndFireSchedules(t.Context(), runner, now.Add(2*time.Second))
	assert.Equal(t, 0, scheduler.LastFiredAtLen(runner), "lastFiredAt should be empty after schedule deletion")
}

// mockLambdaInvokerWithPayload records the payload sent to Lambda.
type mockLambdaInvokerWithPayload struct {
	calls []struct {
		name    string
		payload []byte
	}
	mu sync.Mutex
}

func (m *mockLambdaInvokerWithPayload) InvokeFunction(
	_ context.Context,
	name, _ string,
	payload []byte,
) ([]byte, int, error) {
	m.mu.Lock()
	m.calls = append(m.calls, struct {
		name    string
		payload []byte
	}{name: name, payload: payload})
	m.mu.Unlock()

	return nil, 200, nil
}

func (m *mockLambdaInvokerWithPayload) Called() []struct {
	name    string
	payload []byte
} {
	m.mu.Lock()
	defer m.mu.Unlock()

	cp := make([]struct {
		name    string
		payload []byte
	}, len(m.calls))
	copy(cp, m.calls)

	return cp
}

// mockSQSSenderWithPayload records the last message body sent to SQS.
type mockSQSSenderWithPayload struct {
	last string
	mu   sync.Mutex
}

func (m *mockSQSSenderWithPayload) SendMessageToQueue(_ context.Context, _, body string) error {
	m.mu.Lock()
	m.last = body
	m.mu.Unlock()

	return nil
}

func (m *mockSQSSenderWithPayload) Last() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.last
}

// mockSNSPublisherWithPayload records the last message published to SNS.
type mockSNSPublisherWithPayload struct {
	last string
	mu   sync.Mutex
}

func (m *mockSNSPublisherWithPayload) PublishToTopic(_ context.Context, _, msg string) error {
	m.mu.Lock()
	m.last = msg
	m.mu.Unlock()

	return nil
}

func (m *mockSNSPublisherWithPayload) Last() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.last
}

// mockSFNStarterWithPayload records the last input passed to StepFunctions.
type mockSFNStarterWithPayload struct {
	last string
	mu   sync.Mutex
}

func (m *mockSFNStarterWithPayload) StartExecution(_, _, input string) error {
	m.mu.Lock()
	m.last = input
	m.mu.Unlock()

	return nil
}

func (m *mockSFNStarterWithPayload) Last() string {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.last
}

// TestScheduler_Runner_CronCacheEviction tests that stale cronCache entries are swept when
// a schedule is deleted or its expression changes.
func TestScheduler_Runner_CronCacheEviction(t *testing.T) {
	t.Parallel()

	const lambdaARN = "arn:aws:lambda:us-east-1:000000000000:function:evict-fn"
	const role = "arn:aws:iam::000000000000:role/r"

	tests := []struct {
		setup    func(t *testing.T, backend *scheduler.InMemoryBackend)
		name     string
		wantSize int
	}{
		{
			name: "delete schedule removes cache entry",
			setup: func(t *testing.T, b *scheduler.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.DeleteSchedule("evict-sched", ""))
			},
			wantSize: 0,
		},
		{
			name: "update schedule to different expression removes old entry",
			setup: func(t *testing.T, b *scheduler.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.DeleteSchedule("evict-sched", ""))

				_, err := b.CreateSchedule(
					"evict-sched", "", "cron(0 6 * * ? *)", "", "",
					scheduler.Target{ARN: lambdaARN, RoleARN: role},
					"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
				)
				require.NoError(t, err)
			},
			// After update: old expression is swept, new expression is cached during the same poll.
			wantSize: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateSchedule(
				"evict-sched", "", "cron(0 12 * * ? *)", "", "",
				scheduler.Target{ARN: lambdaARN, RoleARN: role},
				"ENABLED", scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			runner := scheduler.NewRunner(backend)
			runner.SetLambdaInvoker(&mockLambdaInvoker{})

			// Fire at match time so the cron expression gets cached.
			matchTime := time.Date(2024, 1, 15, 12, 0, 0, 0, time.UTC)
			scheduler.CheckAndFireSchedules(t.Context(), runner, matchTime)
			require.Equal(t, 1, scheduler.CronCacheLen(runner), "expression should be cached after first fire")

			// Mutate the backend (delete or change the schedule).
			tt.setup(t, backend)

			// Next poll sweeps stale cache entries.
			scheduler.CheckAndFireSchedules(t.Context(), runner, matchTime.Add(time.Hour))
			assert.Equal(t, tt.wantSize, scheduler.CronCacheLen(runner), "stale cache entries should be evicted")
		})
	}
}

// TestScheduler_Runner_CronMonthAliases tests all 12 month name aliases in cron expressions.
func TestScheduler_Runner_CronMonthAliases(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:month-fn"

	tests := []struct {
		matchAt  time.Time
		name     string
		cronExpr string
	}{
		{name: "FEB", cronExpr: "cron(0 0 1 FEB ? *)", matchAt: time.Date(2024, 2, 1, 0, 0, 0, 0, time.UTC)},
		{name: "MAR", cronExpr: "cron(0 0 1 MAR ? *)", matchAt: time.Date(2024, 3, 1, 0, 0, 0, 0, time.UTC)},
		{name: "APR", cronExpr: "cron(0 0 1 APR ? *)", matchAt: time.Date(2024, 4, 1, 0, 0, 0, 0, time.UTC)},
		{name: "MAY", cronExpr: "cron(0 0 1 MAY ? *)", matchAt: time.Date(2024, 5, 1, 0, 0, 0, 0, time.UTC)},
		{name: "JUN", cronExpr: "cron(0 0 1 JUN ? *)", matchAt: time.Date(2024, 6, 1, 0, 0, 0, 0, time.UTC)},
		{name: "JUL", cronExpr: "cron(0 0 1 JUL ? *)", matchAt: time.Date(2024, 7, 1, 0, 0, 0, 0, time.UTC)},
		{name: "AUG", cronExpr: "cron(0 0 1 AUG ? *)", matchAt: time.Date(2024, 8, 1, 0, 0, 0, 0, time.UTC)},
		{name: "SEP", cronExpr: "cron(0 0 1 SEP ? *)", matchAt: time.Date(2024, 9, 1, 0, 0, 0, 0, time.UTC)},
		{name: "OCT", cronExpr: "cron(0 0 1 OCT ? *)", matchAt: time.Date(2024, 10, 1, 0, 0, 0, 0, time.UTC)},
		{name: "NOV", cronExpr: "cron(0 0 1 NOV ? *)", matchAt: time.Date(2024, 11, 1, 0, 0, 0, 0, time.UTC)},
		{name: "DEC", cronExpr: "cron(0 0 1 DEC ? *)", matchAt: time.Date(2024, 12, 1, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateSchedule(
				tt.name+"-sched",
				"",
				tt.cronExpr,
				"",
				"",
				scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::000000000000:role/r"},
				"ENABLED",
				scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			invoker := &mockLambdaInvoker{}
			runner := scheduler.NewRunner(backend)
			runner.SetLambdaInvoker(invoker)

			scheduler.CheckAndFireSchedules(t.Context(), runner, tt.matchAt)
			assert.NotEmpty(t, invoker.Called(), "should fire at match time for "+tt.name)
		})
	}
}

// TestScheduler_Runner_CronDOWAliases tests all 7 day-of-week name aliases in cron expressions.
func TestScheduler_Runner_CronDOWAliases(t *testing.T) {
	t.Parallel()

	lambdaARN := "arn:aws:lambda:us-east-1:000000000000:function:dow-fn"

	tests := []struct {
		matchAt  time.Time
		name     string
		cronExpr string
	}{
		// 2024-01-07 is a Sunday.
		{name: "SUN", cronExpr: "cron(0 0 ? * SUN *)", matchAt: time.Date(2024, 1, 7, 0, 0, 0, 0, time.UTC)},
		// 2024-01-09 is a Tuesday.
		{name: "TUE", cronExpr: "cron(0 0 ? * TUE *)", matchAt: time.Date(2024, 1, 9, 0, 0, 0, 0, time.UTC)},
		// 2024-01-10 is a Wednesday.
		{name: "WED", cronExpr: "cron(0 0 ? * WED *)", matchAt: time.Date(2024, 1, 10, 0, 0, 0, 0, time.UTC)},
		// 2024-01-11 is a Thursday.
		{name: "THU", cronExpr: "cron(0 0 ? * THU *)", matchAt: time.Date(2024, 1, 11, 0, 0, 0, 0, time.UTC)},
		// 2024-01-12 is a Friday.
		{name: "FRI", cronExpr: "cron(0 0 ? * FRI *)", matchAt: time.Date(2024, 1, 12, 0, 0, 0, 0, time.UTC)},
		// 2024-01-13 is a Saturday.
		{name: "SAT", cronExpr: "cron(0 0 ? * SAT *)", matchAt: time.Date(2024, 1, 13, 0, 0, 0, 0, time.UTC)},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := scheduler.NewInMemoryBackend("000000000000", "us-east-1")
			_, err := backend.CreateSchedule(
				tt.name+"-sched",
				"",
				tt.cronExpr,
				"",
				"",
				scheduler.Target{ARN: lambdaARN, RoleARN: "arn:aws:iam::000000000000:role/r"},
				"ENABLED",
				scheduler.FlexibleTimeWindow{Mode: "OFF"},
			)
			require.NoError(t, err)

			invoker := &mockLambdaInvoker{}
			runner := scheduler.NewRunner(backend)
			runner.SetLambdaInvoker(invoker)

			scheduler.CheckAndFireSchedules(t.Context(), runner, tt.matchAt)
			assert.NotEmpty(t, invoker.Called(), "should fire at match time for "+tt.name)
		})
	}
}
