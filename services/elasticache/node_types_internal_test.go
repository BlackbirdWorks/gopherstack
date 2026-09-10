package elasticache

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAllowedNodeTypeModifications(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		current     string
		wantScaleUp bool
		wantScaleDn bool
		wantOK      bool
	}{
		{
			name:        "smallest_type_only_scales_up",
			current:     "cache.t3.micro",
			wantScaleUp: true,
			wantOK:      true,
		},
		{
			name:        "largest_type_only_scales_down",
			current:     "cache.r7g.4xlarge",
			wantScaleDn: true,
			wantOK:      true,
		},
		{
			name:        "mid_size_scales_both_ways",
			current:     "cache.m6g.xlarge",
			wantScaleUp: true,
			wantScaleDn: true,
			wantOK:      true,
		},
		{
			name:    "unrecognized_type",
			current: "cache.unknown.mega",
			wantOK:  false,
		},
		{
			name:    "empty_type",
			current: "",
			wantOK:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			scaleUp, scaleDown, ok := allowedNodeTypeModifications(tt.current)

			require.Equal(t, tt.wantOK, ok)
			if tt.wantScaleUp {
				assert.NotEmpty(t, scaleUp)
			} else {
				assert.Empty(t, scaleUp)
			}
			if tt.wantScaleDn {
				assert.NotEmpty(t, scaleDown)
			} else {
				assert.Empty(t, scaleDown)
			}
			assert.NotContains(t, scaleUp, tt.current)
			assert.NotContains(t, scaleDown, tt.current)
		})
	}
}

func TestAllowedNodeTypeModifications_SortedAscendingByMemory(t *testing.T) {
	t.Parallel()

	scaleUp, scaleDown, ok := allowedNodeTypeModifications("cache.m6g.xlarge")
	require.True(t, ok)
	require.NotEmpty(t, scaleUp)
	require.NotEmpty(t, scaleDown)

	for i := 1; i < len(scaleUp); i++ {
		assert.LessOrEqual(t, nodeTypeMemoryGiB[scaleUp[i-1]], nodeTypeMemoryGiB[scaleUp[i]])
	}
	for i := 1; i < len(scaleDown); i++ {
		assert.LessOrEqual(t, nodeTypeMemoryGiB[scaleDown[i-1]], nodeTypeMemoryGiB[scaleDown[i]])
	}
}
