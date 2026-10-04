package eventbridge_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/eventbridge"
)

// regionCtx returns a context carrying the given AWS region, using the same
// key type the backend uses so the region is correctly extracted.
func regionCtx(region string) context.Context {
	return context.WithValue(context.Background(), eventbridge.RegionContextKeyForTest{}, region)
}

func TestRegionIsolation_EventBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		createRegion string
		listRegion   string
		busName      string
		wantVisible  bool
	}{
		{
			name:         "bus created in us-east-1 is visible from us-east-1",
			createRegion: "us-east-1",
			listRegion:   "us-east-1",
			busName:      "my-bus",
			wantVisible:  true,
		},
		{
			name:         "bus created in us-east-1 is NOT visible from eu-west-1",
			createRegion: "us-east-1",
			listRegion:   "eu-west-1",
			busName:      "my-bus",
			wantVisible:  false,
		},
		{
			name:         "bus created in eu-west-1 is visible from eu-west-1",
			createRegion: "eu-west-1",
			listRegion:   "eu-west-1",
			busName:      "eu-bus",
			wantVisible:  true,
		},
		{
			name:         "bus created in eu-west-1 is NOT visible from us-east-1",
			createRegion: "eu-west-1",
			listRegion:   "us-east-1",
			busName:      "eu-bus",
			wantVisible:  false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()

			// Create bus in the create region.
			_, err := b.CreateEventBus(regionCtx(tc.createRegion), eventbridge.CreateEventBusParams{Name: tc.busName})
			if err != nil {
				t.Fatalf("CreateEventBus: %v", err)
			}

			// List buses from the list region.
			buses, _, err := b.ListEventBuses(regionCtx(tc.listRegion), "", "", 0)
			if err != nil {
				t.Fatalf("ListEventBuses: %v", err)
			}

			found := false
			for _, bus := range buses {
				if bus.Name == tc.busName {
					found = true

					break
				}
			}

			if found != tc.wantVisible {
				if tc.wantVisible {
					t.Errorf("expected bus %q to be visible from region %q, but it was not", tc.busName, tc.listRegion)
				} else {
					t.Errorf("expected bus %q to NOT be visible from region %q, but it was", tc.busName, tc.listRegion)
				}
			}

			// Also verify DescribeEventBus behaviour.
			_, descErr := b.DescribeEventBus(regionCtx(tc.listRegion), tc.busName)
			if tc.wantVisible && descErr != nil {
				t.Errorf("DescribeEventBus: expected success from %q, got %v", tc.listRegion, descErr)
			}
			if !tc.wantVisible && descErr == nil {
				t.Errorf(
					"DescribeEventBus: expected error from %q (bus is in different region), got nil",
					tc.listRegion,
				)
			}
		})
	}
}

func TestRegionIsolation_Rules(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		ruleRegion  string
		listRegion  string
		ruleName    string
		wantVisible bool
	}{
		{
			name:        "rule in us-east-1 visible from us-east-1",
			ruleRegion:  "us-east-1",
			listRegion:  "us-east-1",
			ruleName:    "my-rule",
			wantVisible: true,
		},
		{
			name:        "rule in us-east-1 NOT visible from eu-west-1",
			ruleRegion:  "us-east-1",
			listRegion:  "eu-west-1",
			ruleName:    "my-rule",
			wantVisible: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()

			// Create a rule on the default bus in the rule region.
			_, err := b.PutRule(regionCtx(tc.ruleRegion), eventbridge.PutRuleInput{
				Name:         tc.ruleName,
				EventPattern: `{"source":["test"]}`,
			})
			if err != nil {
				t.Fatalf("PutRule: %v", err)
			}

			// List rules from the list region.
			rules, _, err := b.ListRules(regionCtx(tc.listRegion), "default", "", "", 0)
			if err != nil {
				t.Fatalf("ListRules: %v", err)
			}

			found := false
			for _, rule := range rules {
				if rule.Name == tc.ruleName {
					found = true

					break
				}
			}

			if found != tc.wantVisible {
				if tc.wantVisible {
					t.Errorf(
						"expected rule %q to be visible from region %q, but it was not",
						tc.ruleName,
						tc.listRegion,
					)
				} else {
					t.Errorf(
						"expected rule %q to NOT be visible from region %q, but it was",
						tc.ruleName,
						tc.listRegion,
					)
				}
			}
		})
	}
}

func TestRegionIsolation_DefaultBus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		region string
	}{
		{name: "home region", region: "us-east-1"},
		{name: "sibling region", region: "us-west-2"},
		{name: "eu region", region: "eu-west-1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := eventbridge.NewInMemoryBackend()
			ctx := regionCtx(tc.region)

			buses, _, err := b.ListEventBuses(ctx, "", "", 0)
			require.NoError(t, err)
			require.Len(t, buses, 1)
			assert.Equal(t, "default", buses[0].Name)
			assert.Contains(t, buses[0].Arn, ":"+tc.region+":")

			bus, err := b.DescribeEventBus(ctx, "")
			require.NoError(t, err)
			assert.Equal(t, buses[0].Arn, bus.Arn)

			require.ErrorIs(t, b.DeleteEventBus(ctx, "default"), eventbridge.ErrCannotDeleteDefaultBus)
		})
	}
}

func TestDefaultBus_RulesAndRestoreInSiblingRegion(t *testing.T) {
	t.Parallel()

	const euRegion = "eu-west-1"

	ctx := regionCtx(euRegion)
	b := eventbridge.NewInMemoryBackend()

	_, err := b.PutRule(ctx, eventbridge.PutRuleInput{Name: "r", EventPattern: `{"source":["x"]}`})
	require.NoError(t, err)

	_, err = b.CreateEventBus(ctx, eventbridge.CreateEventBusParams{Name: "custom"})
	require.NoError(t, err)

	var snap map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(b.Snapshot(ctx), &snap))

	var tables map[string]json.RawMessage

	require.NoError(t, json.Unmarshal(snap["tables"], &tables))

	var buses []map[string]any

	require.NoError(t, json.Unmarshal(tables["buses/"+euRegion], &buses))

	legacy := make([]map[string]any, 0, len(buses))

	for _, bus := range buses {
		if bus["Name"] != "default" {
			legacy = append(legacy, bus)
		}
	}

	require.Len(t, legacy, 1, "legacy snapshot keeps only the custom bus")

	tables["buses/"+euRegion], err = json.Marshal(legacy)
	require.NoError(t, err)

	snap["tables"], err = json.Marshal(tables)
	require.NoError(t, err)

	data, err := json.Marshal(snap)
	require.NoError(t, err)

	restored := eventbridge.NewInMemoryBackend()
	require.NoError(t, restored.Restore(ctx, data))

	got, _, err := restored.ListEventBuses(ctx, "", "", 0)
	require.NoError(t, err)

	names := make([]string, 0, len(got))
	for _, bus := range got {
		names = append(names, bus.Name)
	}

	assert.ElementsMatch(t, []string{"default", "custom"}, names)

	rules, _, err := restored.ListRules(ctx, "default", "", "", 0)
	require.NoError(t, err)
	require.Len(t, rules, 1)
}
