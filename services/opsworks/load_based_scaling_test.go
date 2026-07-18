package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestLoadBasedAutoScaling verifies SetLoadBasedAutoScaling and
// DescribeLoadBasedAutoScaling.
func TestLoadBasedAutoScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "SetLoadBasedAutoScaling and DescribeLoadBasedAutoScaling",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)

				rec := doTarget(t, h, "SetLoadBasedAutoScaling", map[string]any{
					"LayerId": layerID,
					"Enable":  true,
					"UpScaling": map[string]any{
						"CpuThreshold":  80.0,
						"InstanceCount": 2,
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeLoadBasedAutoScaling", map[string]any{
					"LayerIds": []string{layerID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				configs := parseJSON(t, rec.Body.Bytes())["LoadBasedAutoScalingConfigurations"].([]any)
				require.Len(t, configs, 1)
				c := configs[0].(map[string]any)
				assert.Equal(t, layerID, c["LayerId"])
				assert.Equal(t, true, c["Enable"])
			},
		},
		{
			name: "SetLoadBasedAutoScaling on nonexistent layer returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "SetLoadBasedAutoScaling", map[string]any{
					"LayerId": "nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			tt.check(t, h)
		})
	}
}
