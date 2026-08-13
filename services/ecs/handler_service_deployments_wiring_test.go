package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestECS_ServiceDeployments_RealDeploymentsAreVisible proves
// CreateService/UpdateService record a real ServiceDeployment for every
// deployment they create on a service, so ListServiceDeployments and
// DescribeServiceDeployments -- which filter/read the same backing map -- see
// live data (parity-principles.md rule 4: a "real-looking" op filtering a
// never-populated map is a disguised stub).
func TestECS_ServiceDeployments_RealDeploymentsAreVisible(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		updateTaskDef     bool
		wantDeploymentMin int
	}{
		{
			name:              "create only, one PRIMARY deployment",
			updateTaskDef:     false,
			wantDeploymentMin: 1,
		},
		{
			name:              "create then update rotates in a second deployment",
			updateTaskDef:     true,
			wantDeploymentMin: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "wiring-cluster"})
			doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family": "wiring-app",
				"containerDefinitions": []map[string]any{
					{"name": "app", "image": "nginx:1"},
				},
			})

			doECSRequest(t, h, "CreateService", map[string]any{
				"cluster":        "wiring-cluster",
				"serviceName":    "wiring-svc",
				"taskDefinition": "wiring-app",
				"desiredCount":   0,
			})

			if tt.updateTaskDef {
				doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family": "wiring-app",
					"containerDefinitions": []map[string]any{
						{"name": "app", "image": "nginx:2"},
					},
				})
				doECSRequest(t, h, "UpdateService", map[string]any{
					"cluster":        "wiring-cluster",
					"service":        "wiring-svc",
					"taskDefinition": "wiring-app",
				})
			}

			rec := doECSRequest(t, h, "ListServiceDeployments", map[string]any{
				"cluster": "wiring-cluster",
				"service": "wiring-svc",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

			briefs, ok := listResp["serviceDeployments"].([]any)
			require.True(t, ok)
			require.GreaterOrEqualf(t, len(briefs), tt.wantDeploymentMin,
				"ListServiceDeployments returned %d deployments, want at least %d", len(briefs), tt.wantDeploymentMin)

			firstArn, ok := briefs[0].(map[string]any)["serviceDeploymentArn"].(string)
			require.True(t, ok)

			rec = doECSRequest(t, h, "DescribeServiceDeployments", map[string]any{
				"serviceDeploymentArns": []string{firstArn},
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var describeResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &describeResp))

			deployments, ok := describeResp["serviceDeployments"].([]any)
			require.True(t, ok)
			require.Len(t, deployments, 1)

			failures, ok := describeResp["failures"].([]any)
			require.True(t, ok)
			assert.Empty(t, failures)

			sd := deployments[0].(map[string]any)
			assert.Equal(t, firstArn, sd["serviceDeploymentArn"])
			assert.NotEmpty(t, sd["status"])
		})
	}
}

// TestECS_ServiceDeployments_DeletedOnServiceDelete proves DeleteService now
// cleans up its ServiceDeployment records instead of leaking one entry per
// service name ever created and deleted.
func TestECS_ServiceDeployments_DeletedOnServiceDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cleanup-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "cleanup-app",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "nginx:1"},
		},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "cleanup-cluster",
		"serviceName":    "cleanup-svc",
		"taskDefinition": "cleanup-app",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "DeleteService", map[string]any{
		"cluster": "cleanup-cluster",
		"service": "cleanup-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doECSRequest(t, h, "ListServiceDeployments", map[string]any{
		"cluster": "cleanup-cluster",
		"service": "cleanup-svc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))

	briefs, ok := listResp["serviceDeployments"].([]any)
	require.True(t, ok)
	assert.Empty(t, briefs, "ServiceDeployment entries must be removed when their service is deleted")
}
