package eventbridge_test

import (
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/eventbridge"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBridgeBackend(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		run  func(t *testing.T, b *eventbridge.InMemoryBackend)
	}{
		{name: "CreateAndDescribeEventBus", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			bus, err := b.CreateEventBus("my-bus", "a test bus")
			require.NoError(t, err)
			assert.Equal(t, "my-bus", bus.Name)
			assert.Contains(t, bus.Arn, "my-bus")

			got, err := b.DescribeEventBus("my-bus")
			require.NoError(t, err)
			assert.Equal(t, "my-bus", got.Name)
			assert.Equal(t, "a test bus", got.Description)
		}},
		{name: "CreateEventBus/AlreadyExists", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, err := b.CreateEventBus("dup-bus", "")
			require.NoError(t, err)

			_, err = b.CreateEventBus("dup-bus", "")
			require.ErrorIs(t, err, eventbridge.ErrEventBusAlreadyExists)
		}},
		{name: "DeleteEventBus", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, err := b.CreateEventBus("to-delete", "")
			require.NoError(t, err)

			err = b.DeleteEventBus("to-delete")
			require.NoError(t, err)

			_, err = b.DescribeEventBus("to-delete")
			require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
		}},
		{name: "DeleteDefaultEventBus/Fails", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			err := b.DeleteEventBus("default")
			require.ErrorIs(t, err, eventbridge.ErrCannotDeleteDefaultBus)
		}},
		{name: "ListEventBuses", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, _ = b.CreateEventBus("alpha", "")
			_, _ = b.CreateEventBus("beta", "")

			buses, next, err := b.ListEventBuses("", "")
			require.NoError(t, err)
			assert.Empty(t, next)
			// default + alpha + beta
			assert.Len(t, buses, 3)
		}},
		{name: "ListEventBuses/Prefix", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, _ = b.CreateEventBus("prod-bus", "")
			_, _ = b.CreateEventBus("dev-bus", "")

			buses, _, err := b.ListEventBuses("prod", "")
			require.NoError(t, err)
			assert.Len(t, buses, 1)
			assert.Equal(t, "prod-bus", buses[0].Name)
		}},
		{name: "DescribeDefaultEventBus", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			bus, err := b.DescribeEventBus("default")
			require.NoError(t, err)
			assert.Equal(t, "default", bus.Name)
		}},
		{name: "DescribeEventBus/EmptyName", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			// empty name should resolve to default
			bus, err := b.DescribeEventBus("")
			require.NoError(t, err)
			assert.Equal(t, "default", bus.Name)
		}},
		{name: "PutAndListRules", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
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
		}},
		{name: "DescribeRule", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r1", Description: "desc"})
			require.NoError(t, err)

			rule, err := b.DescribeRule("r1", "")
			require.NoError(t, err)
			assert.Equal(t, "r1", rule.Name)
			assert.Equal(t, "desc", rule.Description)
		}},
		{name: "DeleteRule", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, err := b.PutRule(eventbridge.PutRuleInput{Name: "del-rule"})
			require.NoError(t, err)

			err = b.DeleteRule("del-rule", "")
			require.NoError(t, err)

			_, err = b.DescribeRule("del-rule", "")
			require.ErrorIs(t, err, eventbridge.ErrRuleNotFound)
		}},
		{name: "EnableDisableRule", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
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
		}},
		{name: "PutAndListTargets", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
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
		}},
		{name: "RemoveTargets", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
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
		}},
		{name: "PutEvents", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
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
		}},
		{name: "EventLog/MaxSize", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			// Put 1100 events; log should cap at 1000.
			batch := make([]eventbridge.EventEntry, 1100)
			for i := range batch {
				batch[i] = eventbridge.EventEntry{Source: "s", DetailType: "t", Detail: "{}"}
			}
			b.PutEvents(batch)

			log := b.GetEventLog()
			assert.Len(t, log, 1000)
		}},
		{name: "PutRule/DefaultState", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			rule, err := b.PutRule(eventbridge.PutRuleInput{Name: "no-state-rule"})
			require.NoError(t, err)
			assert.Equal(t, "ENABLED", rule.State)
		}},
		{name: "PutRule/UnknownBus", run: func(t *testing.T, b *eventbridge.InMemoryBackend) {
			_, err := b.PutRule(eventbridge.PutRuleInput{Name: "r", EventBusName: "nonexistent"})
			require.ErrorIs(t, err, eventbridge.ErrEventBusNotFound)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := eventbridge.NewInMemoryBackendWithConfig("123456789012", "us-east-1")
			tt.run(t, b)
		})
	}
}
