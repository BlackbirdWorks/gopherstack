package eventbridge_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newBackend() *eventbridge.InMemoryBackend {
	return eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
}

func TestCreateAndDescribeEventBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	bus, err := b.CreateEventBus("my-bus", "a test bus")
	require.NoError(t, err)
	assert.Equal(t, "my-bus", bus.Name)
	assert.Contains(t, bus.Arn, "my-bus")

	got, err := b.DescribeEventBus("my-bus")
	require.NoError(t, err)
	assert.Equal(t, "my-bus", got.Name)
	assert.Equal(t, "a test bus", got.Description)
}

func TestCreateEventBus_AlreadyExists(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus("dup-bus", "")
	require.NoError(t, err)

	_, err = b.CreateEventBus("dup-bus", "")
	require.ErrorIs(t, err, eventbridge.ErrEventBusAlreadyExists)
}

func TestDeleteEventBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.CreateEventBus("to-delete", "")
	require.NoError(t, err)

	err = b.DeleteEventBus("to-delete")
	require.NoError(t, err)

	_, err = b.DescribeEventBus("to-delete")
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}

func TestDeleteDefaultEventBus_Fails(t *testing.T) {
	t.Parallel()
	b := newBackend()

	err := b.DeleteEventBus("default")
	require.ErrorIs(t, err, eventbridge.ErrCannotDeleteDefaultBus)
}

func TestListEventBuses(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateEventBus("alpha", "")
	_, _ = b.CreateEventBus("beta", "")

	buses, next, err := b.ListEventBuses("", "")
	require.NoError(t, err)
	assert.Empty(t, next)
	// default + alpha + beta
	assert.Len(t, buses, 3)
}

func TestListEventBuses_Prefix(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, _ = b.CreateEventBus("prod-bus", "")
	_, _ = b.CreateEventBus("dev-bus", "")

	buses, _, err := b.ListEventBuses("prod", "")
	require.NoError(t, err)
	assert.Len(t, buses, 1)
	assert.Equal(t, "prod-bus", buses[0].Name)
}

func TestDescribeDefaultEventBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	bus, err := b.DescribeEventBus("default")
	require.NoError(t, err)
	assert.Equal(t, "default", bus.Name)
}

func TestDescribeEventBus_EmptyName(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// empty name should resolve to default
	bus, err := b.DescribeEventBus("")
	require.NoError(t, err)
	assert.Equal(t, "default", bus.Name)
}

func TestPutAndListRules(t *testing.T) {
	t.Parallel()
	b := newBackend()

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
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r1", Description: "desc"})
	require.NoError(t, err)

	rule, err := b.DescribeRule("r1", "")
	require.NoError(t, err)
	assert.Equal(t, "r1", rule.Name)
	assert.Equal(t, "desc", rule.Description)
}

func TestDeleteRule(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "del-rule"})
	require.NoError(t, err)

	err = b.DeleteRule("del-rule", "")
	require.NoError(t, err)

	_, err = b.DescribeRule("del-rule", "")
	require.ErrorIs(t, err, eventbridge.ErrRuleNotFound)
}

func TestEnableDisableRule(t *testing.T) {
	t.Parallel()
	b := newBackend()

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
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "rule-with-targets"})
	require.NoError(t, err)

	targets := []eventbridge.Target{
		{Id: "t1", Arn: "arn:aws:lambda:us-east-1:123456789012:function:my-func"},
		{Id: "t2", Arn: "arn:aws:sqs:us-east-1:123456789012:my-queue"},
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
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "rule-remove"})
	require.NoError(t, err)

	_, err = b.PutTargets("rule-remove", "", []eventbridge.Target{
		{Id: "t1", Arn: "arn:aws:lambda:us-east-1:123456789012:function:fn"},
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
	b := newBackend()

	now := time.Now()
	entries := []eventbridge.EventEntry{
		{Source: "my.app", DetailType: "UserCreated", Detail: `{"userId":"123"}`, Time: &now},
		{Source: "my.app", DetailType: "UserDeleted", Detail: `{"userId":"456"}`},
	}

	results := b.PutEvents(entries)
	assert.Len(t, results, 2)
	for _, r := range results {
		assert.NotEmpty(t, r.EventId)
		assert.Empty(t, r.ErrorCode)
	}

	log := b.GetEventLog()
	assert.Len(t, log, 2)
}

func TestEventLog_MaxSize(t *testing.T) {
	t.Parallel()
	b := newBackend()

	// Put 1100 events; log should cap at 1000.
	batch := make([]eventbridge.EventEntry, 1100)
	for i := range batch {
		batch[i] = eventbridge.EventEntry{Source: "s", DetailType: "t", Detail: "{}"}
	}
	b.PutEvents(batch)

	log := b.GetEventLog()
	assert.Len(t, log, 1000)
}

func TestPutRule_DefaultState(t *testing.T) {
	t.Parallel()
	b := newBackend()

	rule, err := b.PutRule(eventbridge.PutRuleInput{Name: "no-state-rule"})
	require.NoError(t, err)
	assert.Equal(t, "ENABLED", rule.State)
}

func TestPutRule_UnknownBus(t *testing.T) {
	t.Parallel()
	b := newBackend()

	_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r", EventBusName: "nonexistent"})
	require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
}
