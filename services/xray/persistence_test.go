package xray_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/xray"
)

func TestXRay_PersistenceSnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *xray.InMemoryBackend)
		verify func(t *testing.T, b *xray.InMemoryBackend)
		name   string
	}{
		{
			name:  "empty",
			setup: func(_ *xray.InMemoryBackend) {},
			verify: func(t *testing.T, b *xray.InMemoryBackend) {
				t.Helper()

				assert.Empty(t, b.GetGroups())
			},
		},
		{
			name: "group_and_rule_preserved",
			setup: func(b *xray.InMemoryBackend) {
				_, _ = b.CreateGroup("my-group", `service("my-svc")`)
				rule := xray.SamplingRule{RuleName: "my-rule", FixedRate: 0.05, ReservoirSize: 50, Priority: 10}
				_, _ = b.CreateSamplingRule(rule)
			},
			verify: func(t *testing.T, b *xray.InMemoryBackend) {
				t.Helper()

				groups := b.GetGroups()
				require.Len(t, groups, 1)
				assert.Equal(t, "my-group", groups[0].GroupName)
				// GetSamplingRules returns the user-created rule + the built-in Default rule.
				rules := b.GetSamplingRules()
				require.GreaterOrEqual(t, len(rules), 2, "expected at least 2 rules (my-rule + Default)")
				ruleNames := make(map[string]bool, len(rules))
				for _, r := range rules {
					ruleNames[r.RuleName] = true
				}
				assert.True(t, ruleNames["my-rule"], "my-rule should be present")
				assert.True(t, ruleNames["Default"], "Default rule should always be present")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := xray.NewInMemoryBackend()
			tt.setup(b)

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := xray.NewInMemoryBackend()
			err := b2.Restore(t.Context(), snap)
			require.NoError(t, err)

			tt.verify(t, b2)
		})
	}
}
