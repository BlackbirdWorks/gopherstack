package eventbridge_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAndDescribeEventBus(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	bus, err := b.CreateEventBus("my-bus", "a test bus")
	require.NoError(t, err)
	assert.Equal(t, "my-bus", bus.Name)
	assert.Contains(t, bus.Arn, "my-bus")

	got, err := b.DescribeEventBus("my-bus")
	require.NoError(t, err)
	assert.Equal(t, "my-bus", got.Name)
	assert.Equal(t, "a test bus", got.Description)
}

func TestCreateEventBusAlreadyExists(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.CreateEventBus("dup-bus", "")
	require.NoError(t, err)

	_, err = b.CreateEventBus("dup-bus", "")
	require.ErrorIs(t, err, eventbridge.ErrEventBusAlreadyExists)
}

func TestDeleteEventBus(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.CreateEventBus("to-delete", "")
	require.NoError(t, err)

	err = b.DeleteEventBus("to-delete")
	require.NoError(t, err)

	_, err = b.DescribeEventBus("to-delete")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

func TestDeleteDefaultEventBusFails(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	err := b.DeleteEventBus("default")
	require.ErrorIs(t, err, eventbridge.ErrCannotDeleteDefaultBus)
}

func TestListEventBuses(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		prefix        string
		wantFirstName string
		setupBuses    []string
		wantCount     int
	}{
		{
			name:       "All",
			setupBuses: []string{"alpha", "beta"},
			prefix:     "",
			// default + alpha + beta
			wantCount: 3,
		},
		{
			name:          "Prefix",
			setupBuses:    []string{"prod-bus", "dev-bus"},
			prefix:        "prod",
			wantCount:     1,
			wantFirstName: "prod-bus",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			for _, name := range tt.setupBuses {
				_, _ = b.CreateEventBus(name, "")
			}

			buses, next, err := b.ListEventBuses(tt.prefix, "")
			require.NoError(t, err)
			assert.Empty(t, next)
			assert.Len(t, buses, tt.wantCount)
			if tt.wantFirstName != "" {
				assert.Equal(t, tt.wantFirstName, buses[0].Name)
			}
		})
	}
}

func TestDescribeEventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		busName  string
		wantName string
	}{
		{name: "Default", busName: "default", wantName: "default"},
		// empty name should resolve to default
		{name: "EmptyName", busName: "", wantName: "default"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			bus, err := b.DescribeEventBus(tt.busName)
			require.NoError(t, err)
			assert.Equal(t, tt.wantName, bus.Name)
		})
	}
}

func TestPutAndListRules(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	input := eventbridge.PutRuleInput{
		Name:         "my-rule",
		EventBusName: "default",
		EventPattern: `{"source":["my.app"]}`,
		State:        "ENABLED",
	}
	rule, err := b.PutRule(input)
	require.NoError(t, err)
	assert.Equal(t, "my-rule", rule.Name)
	assert.Equal(t, "ENABLED", rule.State)
	assert.Contains(t, rule.Arn, "my-rule")

	rules, next, err := b.ListRules("default", "", "")
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, rules, 1)
}

func TestDescribeRule(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r1", Description: "desc"})
	require.NoError(t, err)

	rule, err := b.DescribeRule("r1", "")
	require.NoError(t, err)
	assert.Equal(t, "r1", rule.Name)
	assert.Equal(t, "desc", rule.Description)
}

func TestDeleteRule(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "del-rule"})
	require.NoError(t, err)

	err = b.DeleteRule("del-rule", "")
	require.NoError(t, err)

	_, err = b.DescribeRule("del-rule", "")
	require.ErrorIs(t, err, eventbridge.ErrRuleNotFound)
}

func TestEnableDisableRule(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "toggle-rule", State: "ENABLED"})
	require.NoError(t, err)

	err = b.DisableRule("toggle-rule", "")
	require.NoError(t, err)

	rule, err := b.DescribeRule("toggle-rule", "")
	require.NoError(t, err)
	assert.Equal(t, "DISABLED", rule.State)

	err = b.EnableRule("toggle-rule", "")
	require.NoError(t, err)

	rule, err = b.DescribeRule("toggle-rule", "")
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
}

func TestPutAndListTargets(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "rule-with-targets"})
	require.NoError(t, err)

	targets := []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:lambda:us-east-1:123456789012:function:my-func"},
		{ID: "t2", Arn: "arn:aws:sqs:us-east-1:123456789012:my-queue"},
	}

	failed, err := b.PutTargets("rule-with-targets", "", targets)
	require.NoError(t, err)
	assert.Empty(t, failed)

	got, next, err := b.ListTargetsByRule("rule-with-targets", "", "")
	require.NoError(t, err)
	assert.Empty(t, next)
	assert.Len(t, got, 2)
}

func TestRemoveTargets(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "rule-remove"})
	require.NoError(t, err)

	_, err = b.PutTargets("rule-remove", "", []eventbridge.Target{
		{ID: "t1", Arn: "arn:aws:lambda:us-east-1:123456789012:function:fn"},
	})
	require.NoError(t, err)

	failed, err := b.RemoveTargets("rule-remove", "", []string{"t1"})
	require.NoError(t, err)
	assert.Empty(t, failed)

	got, _, err := b.ListTargetsByRule("rule-remove", "", "")
	require.NoError(t, err)
	assert.Empty(t, got)
}

func TestPutEvents(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	now := time.Now()
	entries := []eventbridge.EventEntry{
		{Source: "my.app", DetailType: "UserCreated", Detail: `{"userId":"123"}`, Time: &now},
		{Source: "my.app", DetailType: "UserDeleted", Detail: `{"userId":"456"}`},
	}

	results := b.PutEvents(entries)
	assert.Len(t, results, 2)
	for _, r := range results {
		assert.NotEmpty(t, r.EventID)
		assert.Empty(t, r.ErrorCode)
	}

	log := b.GetEventLog()
	assert.Len(t, log, 2)
}

func TestEventLogMaxSize(t *testing.T) {
	t.Parallel()
	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Put 1100 events; log should cap at 1000.
	batch := make([]eventbridge.EventEntry, 1100)
	for i := range batch {
		batch[i] = eventbridge.EventEntry{Source: "s", DetailType: "t", Detail: "{}"}
	}
	b.PutEvents(batch)

	log := b.GetEventLog()
	assert.Len(t, log, 1000)
}

func TestPutRule(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		input     eventbridge.PutRuleInput
		name      string
		wantState string
	}{
		{
			name:      "DefaultState",
			input:     eventbridge.PutRuleInput{Name: "no-state-rule"},
			wantState: "ENABLED",
		},
		{
			name:    "UnknownBus",
			input:   eventbridge.PutRuleInput{Name: "r", EventBusName: "nonexistent"},
			wantErr: eventbridge.ErrEventBusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

			rule, err := b.PutRule(tt.input)
			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.wantState, rule.State)
		})
	}
}

func TestScheduler_LastFiredCleanupOnDeleteRule(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	defer b.Close()

	rule, err := b.PutRule(eventbridge.PutRuleInput{
		Name:               "sched-rule",
		ScheduleExpression: "rate(1 minute)",
		State:              "ENABLED",
		EventBusName:       "default",
	})
	require.NoError(t, err)

	// Seed lastFired as the scheduler would after init.
	lastFired := map[string]time.Time{
		rule.Arn: time.Now().Add(-2 * time.Minute),
	}

	// Delete the rule then run a scheduler tick.
	err = b.DeleteRule("sched-rule", "default")
	require.NoError(t, err)

	sched := eventbridge.NewScheduler(b, 0)
	sched.ProcessTickForTest(t.Context(), time.Now(), lastFired)

	// The stale entry for the deleted rule must have been purged.
	_, still := lastFired[rule.Arn]
	assert.False(t, still, "lastFired should not contain entries for deleted rules")
}

func TestBackend_Close(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
	// Close should return without blocking even when no goroutines are active.
	b.Close()
}

func TestBackend_ResetRestoresDefaultEventBus(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	// Create a user-defined event bus and a rule.
	_, err := b.CreateEventBus("user-bus", "")
	require.NoError(t, err)

	_, err = b.PutRule(eventbridge.PutRuleInput{
		Name:         "user-rule",
		EventPattern: `{"source":["test"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err)

	// Reset clears user data.
	b.Reset()

	// User bus must be gone.
	_, err = b.DescribeEventBus("user-bus")
	require.Error(t, err)

	// Default event bus must still exist so PutRule works.
	_, err = b.PutRule(eventbridge.PutRuleInput{
		Name:         "post-reset-rule",
		EventBusName: "default",
		EventPattern: `{"source":["test"]}`,
		State:        "ENABLED",
	})
	require.NoError(t, err, "default event bus must be available after Reset")

	// Default bus must appear in ListEventBuses.
	buses, _, err := b.ListEventBuses("", "")
	require.NoError(t, err)
	assert.Len(t, buses, 1, "only the default bus should exist after Reset")
	assert.Equal(t, "default", buses[0].Name)
}
