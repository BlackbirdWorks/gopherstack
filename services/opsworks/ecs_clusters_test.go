package opsworks_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opsworks"
)

// TestEcsClusters verifies ECS cluster registration.
func TestEcsClusters(t *testing.T) {
	t.Parallel()

	const testClusterArn = "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster"

	tests := []struct {
		check func(t *testing.T, h *opsworks.Handler)
		name  string
	}{
		{
			name: "RegisterEcsCluster and DescribeEcsClusters",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				rec := doTarget(t, h, "RegisterEcsCluster", map[string]any{
					"EcsClusterArn": testClusterArn,
					"StackId":       stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec.Body.Bytes())
				assert.Equal(t, testClusterArn, resp["EcsClusterArn"])

				rec = doTarget(t, h, "DescribeEcsClusters", map[string]any{
					"StackId": stackID,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				clusters := parseJSON(t, rec.Body.Bytes())["EcsClusters"].([]any)
				require.Len(t, clusters, 1)
				c := clusters[0].(map[string]any)
				assert.Equal(t, testClusterArn, c["EcsClusterArn"])
				assert.Equal(t, "my-cluster", c["EcsClusterName"])
			},
		},
		{
			name: "DeregisterEcsCluster removes cluster",
			check: func(t *testing.T, h *opsworks.Handler) {
				t.Helper()
				stackID := createTestStack(t, h)
				doTarget(t, h, "RegisterEcsCluster", map[string]any{
					"EcsClusterArn": testClusterArn,
					"StackId":       stackID,
				})
				rec := doTarget(t, h, "DeregisterEcsCluster", map[string]any{
					"EcsClusterArn": testClusterArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)

				rec = doTarget(t, h, "DescribeEcsClusters", map[string]any{
					"EcsClusterArns": []string{testClusterArn},
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
