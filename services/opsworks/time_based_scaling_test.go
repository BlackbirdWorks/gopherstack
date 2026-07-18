package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestTimeBasedAutoScaling verifies SetTimeBasedAutoScaling and
// DescribeTimeBasedAutoScaling.
func TestTimeBasedAutoScaling(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "SetTimeBasedAutoScaling and DescribeTimeBasedAutoScaling",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				layerID := createTestLayer(t, h, stackID)
				instanceID := createTestInstance(t, h, stackID, layerID)

				rec := doTarget(t, h, "SetTimeBasedAutoScaling", map[string]any{
					"InstanceId": instanceID,
					"AutoScalingSchedule": map[string]any{
						"Monday": map[string]string{"1": "on"},
					},
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeTimeBasedAutoScaling", map[string]any{
					"InstanceIds": []string{instanceID},
				})
				require.Equal(t, http.StatusOK, rec.Code)
				configs := parseJSON(t, rec.Body.Bytes())["TimeBasedAutoScalingConfigurations"].([]any)
				require.Len(t, configs, 1)
				assert.Equal(t, instanceID, configs[0].(map[string]any)["InstanceId"])
			},
		},
		{
			name: "SetTimeBasedAutoScaling on nonexistent instance returns 404",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				rec := doTarget(t, h, "SetTimeBasedAutoScaling", map[string]any{
					"InstanceId": "nonexistent",
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
