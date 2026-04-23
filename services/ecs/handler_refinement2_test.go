package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// TestECS_UpdateCluster verifies UpdateCluster updates capacity providers and settings.
func TestECS_UpdateCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name: "update capacity providers",
			input: map[string]any{
				"cluster":           "test-cluster",
				"capacityProviders": []string{"FARGATE", "FARGATE_SPOT"},
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "cluster not found",
			input:    map[string]any{"cluster": "missing-cluster"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCode == http.StatusOK {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "test-cluster"})
			}

			rec := doECSRequest(t, h, "UpdateCluster", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cluster, ok := resp["cluster"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "test-cluster", cluster["clusterName"])
		})
	}
}

// TestECS_UpdateCapacityProvider verifies UpdateCapacityProvider.
func TestECS_UpdateCapacityProvider(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "update existing",
			input:    map[string]any{"name": "my-cp", "status": "ACTIVE"},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			input:    map[string]any{"name": "missing"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.wantCode == http.StatusOK {
				doECSRequest(t, h, "CreateCapacityProvider", map[string]any{"name": "my-cp"})
			}

			rec := doECSRequest(t, h, "UpdateCapacityProvider", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestECS_ListTaskDefinitionFamilies verifies ListTaskDefinitionFamilies.
func TestECS_ListTaskDefinitionFamilies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		input    map[string]any
		families []string
		wantLen  int
	}{
		{
			name:    "empty",
			input:   map[string]any{},
			wantLen: 0,
		},
		{
			name:     "all families",
			families: []string{"web", "worker"},
			input:    map[string]any{},
			wantLen:  2,
		},
		{
			name:     "prefix filter",
			families: []string{"web", "worker", "backend"},
			input:    map[string]any{"familyPrefix": "w"},
			wantLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			for _, family := range tt.families {
				doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
					"family":               family,
					"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
				})
			}

			rec := doECSRequest(t, h, "ListTaskDefinitionFamilies", tt.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			families, ok := resp["families"].([]any)
			require.True(t, ok)
			assert.Len(t, families, tt.wantLen)
		})
	}
}

// TestECS_StartTask verifies StartTask places tasks on specific container instances.
func TestECS_StartTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "missing task definition",
			input: map[string]any{
				"taskDefinition":     "missing",
				"containerInstances": []string{"ci-arn-1"},
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "empty container instances",
			input: map[string]any{
				"taskDefinition":     "missing",
				"containerInstances": []string{},
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "StartTask", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestECS_StartTask_WithSetup verifies StartTask when cluster and task def exist.
func TestECS_StartTask_WithSetup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cluster, task def, container instance
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "start-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "startapp",
		"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
	})

	ci, err := h.Backend.RegisterContainerInstance("start-cluster", "i-abc12345")
	require.NoError(t, err)

	rec := doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "start-cluster",
		"taskDefinition":     "startapp",
		"containerInstances": []string{ci.ContainerInstanceArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	taskList, ok := resp["tasks"].([]any)
	require.True(t, ok)
	assert.Len(t, taskList, 1)
}

// TestECS_Tagging verifies TagResource, UntagResource, ListTagsForResource.
func TestECS_Tagging(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		resourceArn string
		tags        []map[string]any
		removeKeys  []string
		wantLen     int
	}{
		{
			name:        "tag and list",
			resourceArn: "arn:aws:ecs:us-east-1:000000000000:cluster/my-cluster",
			tags: []map[string]any{
				{"key": "env", "value": "prod"},
				{"key": "team", "value": "platform"},
			},
			wantLen: 2,
		},
		{
			name:        "tag then untag one",
			resourceArn: "arn:aws:ecs:us-east-1:000000000000:cluster/other",
			tags: []map[string]any{
				{"key": "env", "value": "prod"},
				{"key": "team", "value": "platform"},
			},
			removeKeys: []string{"env"},
			wantLen:    1,
		},
		{
			name:        "empty tags",
			resourceArn: "arn:aws:ecs:us-east-1:000000000000:cluster/empty",
			tags:        []map[string]any{},
			wantLen:     0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECSRequest(t, h, "TagResource", map[string]any{
				"resourceArn": tt.resourceArn,
				"tags":        tt.tags,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			if len(tt.removeKeys) > 0 {
				rec2 := doECSRequest(t, h, "UntagResource", map[string]any{
					"resourceArn": tt.resourceArn,
					"tagKeys":     tt.removeKeys,
				})
				require.Equal(t, http.StatusOK, rec2.Code)
			}

			rec3 := doECSRequest(t, h, "ListTagsForResource", map[string]any{
				"resourceArn": tt.resourceArn,
			})
			require.Equal(t, http.StatusOK, rec3.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp))

			tagList, ok := resp["tags"].([]any)
			require.True(t, ok)
			assert.Len(t, tagList, tt.wantLen)
		})
	}
}

// TestECS_ListServicesByNamespace verifies ListServicesByNamespace.
func TestECS_ListServicesByNamespace(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty cluster",
			input:    map[string]any{"cluster": "default"},
			wantCode: http.StatusOK,
		},
		{
			name:     "namespace filter",
			input:    map[string]any{"cluster": "default", "namespace": "frontend"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "ListServicesByNamespace", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)
		})
	}
}

// TestECS_ListServiceDeployments verifies ListServiceDeployments.
func TestECS_ListServiceDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input    map[string]any
		name     string
		wantCode int
	}{
		{
			name:     "empty cluster",
			input:    map[string]any{"cluster": "default"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "ListServiceDeployments", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			arns, ok := resp["serviceDeploymentArns"].([]any)
			require.True(t, ok)
			assert.Empty(t, arns)
		})
	}
}

// TestECS_StopServiceDeployment verifies StopServiceDeployment.
func TestECS_StopServiceDeployment(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:ecs:us-east-1:000000000000:service-deployment/test-cluster/my-service/deploy-1"

	tests := []struct {
		input    map[string]any
		setup    func(h *ecs.Handler)
		name     string
		wantCode int
	}{
		{
			name: "stop in-progress deployment",
			setup: func(h *ecs.Handler) {
				now := time.Now()
				b, ok := h.Backend.(*ecs.InMemoryBackend)
				if !ok {
					return
				}
				b.AddServiceDeploymentInternal(&ecs.ServiceDeployment{
					ServiceDeploymentArn: arn,
					ClusterArn:           "arn:aws:ecs:us-east-1:000000000000:cluster/test-cluster",
					ServiceArn:           "arn:aws:ecs:us-east-1:000000000000:service/test-cluster/my-service",
					Status:               "IN_PROGRESS",
					CreatedAt:            &now,
				})
			},
			input:    map[string]any{"serviceDeploymentArn": arn},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			input:    map[string]any{"serviceDeploymentArn": "arn:not-exist"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECSRequest(t, h, "StopServiceDeployment", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			sd, ok := resp["serviceDeployment"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "STOPPED", sd["status"])
		})
	}
}
