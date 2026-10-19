package pipes_test

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/pipes"
)

// Snapshots written before the wire-key case fix carry the old PascalCase keys.
func TestWireKeyCase_OldSnapshotKeysStillDecode(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		raw  string
		want pipes.ECSTaskTargetParameters
	}{
		{
			name: "old_pascal_case",
			raw: `{"NetworkConfiguration":{"AwsvpcConfiguration":{"Subnets":["s-1"]}},
				"CapacityProviderStrategy":[{"CapacityProvider":"FARGATE","Weight":2,"Base":1}],
				"PlacementConstraints":[{"Type":"memberOf","Expression":"x"}],
				"PlacementStrategy":[{"Type":"spread","Field":"host"}]}`,
		},
		{
			name: "new_wire_case",
			raw: `{"NetworkConfiguration":{"awsvpcConfiguration":{"Subnets":["s-1"]}},
				"CapacityProviderStrategy":[{"capacityProvider":"FARGATE","weight":2,"base":1}],
				"PlacementConstraints":[{"type":"memberOf","expression":"x"}],
				"PlacementStrategy":[{"type":"spread","field":"host"}]}`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var got pipes.ECSTaskTargetParameters

			require.NoError(t, json.Unmarshal([]byte(tt.raw), &got))
			assert.Equal(t, []string{"s-1"}, got.NetworkConfiguration.AwsvpcConfiguration.Subnets)
			assert.Equal(t, "FARGATE", got.CapacityProviderStrategy[0].CapacityProvider)
			assert.Equal(t, 2, got.CapacityProviderStrategy[0].Weight)
			assert.Equal(t, 1, got.CapacityProviderStrategy[0].Base)
			assert.Equal(t, "x", got.PlacementConstraints[0].Expression)
			assert.Equal(t, "host", got.PlacementStrategy[0].Field)
		})
	}
}
