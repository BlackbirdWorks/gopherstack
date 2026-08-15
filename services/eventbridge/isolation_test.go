package eventbridge //nolint:testpackage // existing issue.

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEventBridgeRegionIsolation(t *testing.T) { //nolint:paralleltest // existing issue.
	backend := NewInMemoryBackendWithConfig("123456789012", "us-east-1")

	ctxEast := context.WithValue(context.Background(), regionContextKey{}, "us-east-1")
	ctxWest := context.WithValue(context.Background(), regionContextKey{}, "us-west-2")

	// 1. Create bus in us-east-1
	_, err := backend.CreateEventBus(ctxEast, CreateEventBusParams{Name: "bus-east"})
	require.NoError(t, err)

	// 2. Create bus with SAME NAME in us-west-2
	_, err = backend.CreateEventBus(ctxWest, CreateEventBusParams{Name: "bus-east"})
	require.NoError(t, err)

	// 3. Verify us-east-1 only sees its bus
	eastBuses, _, err := backend.ListEventBuses(ctxEast, "", "", 0)
	require.NoError(t, err)
	assert.True(t, containsBus(eastBuses, "bus-east"))

	// 4. Verify us-west-2 only sees its bus
	westBuses, _, err := backend.ListEventBuses(ctxWest, "", "", 0)
	require.NoError(t, err)
	assert.True(t, containsBus(westBuses, "bus-east"))

	// 5. Create rule in us-east-1
	_, err = backend.PutRule(ctxEast, PutRuleInput{
		Name:               "rule1",
		EventBusName:       "bus-east",
		ScheduleExpression: "rate(1 minute)",
	})
	require.NoError(t, err)

	// 6. Verify rule1 NOT in us-west-2
	westRules, _, err := backend.ListRules(ctxWest, "bus-east", "", "", 0)
	require.NoError(t, err)
	assert.False(t, containsRule(westRules, "rule1"))

	// 7. Verify rule1 IS in us-east-1
	eastRules, _, err := backend.ListRules(ctxEast, "bus-east", "", "", 0)
	require.NoError(t, err)
	assert.True(t, containsRule(eastRules, "rule1"))
}

func containsBus(buses []EventBus, name string) bool {
	for _, b := range buses {
		if b.Name == name {
			return true
		}
	}

	return false
}

func containsRule(rules []Rule, name string) bool {
	for _, r := range rules {
		if r.Name == name {
			return true
		}
	}

	return false
}
