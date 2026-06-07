package eventbridge_test

import (
	"context"
	"testing"

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
			_, err := b.CreateEventBus(regionCtx(tc.createRegion), tc.busName, "")
			if err != nil {
				t.Fatalf("CreateEventBus: %v", err)
			}

			// List buses from the list region.
			buses, _, err := b.ListEventBuses(regionCtx(tc.listRegion), "", "")
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
				t.Errorf("DescribeEventBus: expected error from %q (bus is in different region), got nil", tc.listRegion)
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
			rules, _, err := b.ListRules(regionCtx(tc.listRegion), "default", "", "")
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
					t.Errorf("expected rule %q to be visible from region %q, but it was not", tc.ruleName, tc.listRegion)
				} else {
					t.Errorf("expected rule %q to NOT be visible from region %q, but it was", tc.ruleName, tc.listRegion)
				}
			}
		})
	}
}

func TestRegionIsolation_DefaultBus(t *testing.T) {
	t.Parallel()

	b := eventbridge.NewInMemoryBackend()

	// The default bus is created with the backend's default region (config.DefaultRegion).
	// Requesting from a different region should not see it.
	buses, _, err := b.ListEventBuses(regionCtx("us-west-2"), "", "")
	if err != nil {
		t.Fatalf("ListEventBuses: %v", err)
	}

	for _, bus := range buses {
		if bus.Name == "default" {
			t.Errorf("default bus should not be visible from us-west-2 (backend region is %q)", "us-east-1")
		}
	}

	// The backend's own region should see the default bus.
	defaultBuses, _, err := b.ListEventBuses(context.Background(), "", "")
	if err != nil {
		t.Fatalf("ListEventBuses default region: %v", err)
	}

	foundDefault := false
	for _, bus := range defaultBuses {
		if bus.Name == "default" {
			foundDefault = true
			break
		}
	}
	if !foundDefault {
		t.Error("default bus should be visible from the backend's own region")
	}
}
