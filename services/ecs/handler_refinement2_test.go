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

// TestECS_ContinueServiceDeployment verifies real-vs-honest-error behavior:
// this backend never pauses a deployment at a lifecycle hook (blue/green
// PAUSE stages aren't modeled), so ContinueServiceDeployment must validate
// the deployment/hookId inputs for real and report ClientException rather
// than fabricating a successful continue/rollback.
func TestECS_ContinueServiceDeployment(t *testing.T) {
	t.Parallel()

	const arn = "arn:aws:ecs:us-east-1:000000000000:service-deployment/test-cluster/my-service/deploy-1"

	tests := []struct {
		input        map[string]any
		setup        func(h *ecs.Handler)
		name         string
		wantTypeIsIn []string
		wantCode     int
	}{
		{
			name: "existing deployment, no paused hook",
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
			input:        map[string]any{"serviceDeploymentArn": arn, "hookId": "hook-1"},
			wantCode:     http.StatusBadRequest,
			wantTypeIsIn: []string{"ClientException"},
		},
		{
			name:         "deployment not found",
			input:        map[string]any{"serviceDeploymentArn": "arn:not-exist", "hookId": "hook-1"},
			wantCode:     http.StatusBadRequest,
			wantTypeIsIn: []string{"ServiceDeploymentNotFoundException"},
		},
		{
			name:         "missing hookId",
			input:        map[string]any{"serviceDeploymentArn": arn},
			wantCode:     http.StatusBadRequest,
			wantTypeIsIn: []string{"InvalidParameterException"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doECSRequest(t, h, "ContinueServiceDeployment", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			assert.Contains(t, tt.wantTypeIsIn, resp["__type"])
		})
	}
}

// TestECS_RunTask_StartedBy verifies startedBy, platformVersion and tags are stored and returned.
func TestECS_RunTask_StartedBy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "default"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "webapp",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition":  "webapp",
		"startedBy":       "ecs-agent",
		"platformVersion": "1.4.0",
		"tags":            []map[string]any{{"key": "env", "value": "test"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks, ok := resp["tasks"].([]any)
	require.True(t, ok)
	require.Len(t, tasks, 1)

	task := tasks[0].(map[string]any)
	assert.Equal(t, "ecs-agent", task["startedBy"])
	assert.Equal(t, "1.4.0", task["platformVersion"])
}

// TestECS_RegisterTaskDefinition_CPU_Memory verifies CPU and Memory are stored.
func TestECS_RegisterTaskDefinition_CPU_Memory(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		input          map[string]any
		wantCPU        string
		wantMemory     string
		wantPlatFamily string
	}{
		{
			name: "fargate with cpu and memory",
			input: map[string]any{
				"family":               "fargate-app",
				"cpu":                  "256",
				"memory":               "512",
				"platformFamily":       "Linux",
				"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
			},
			wantCPU:        "256",
			wantMemory:     "512",
			wantPlatFamily: "Linux",
		},
		{
			name: "no cpu or memory",
			input: map[string]any{
				"family":               "ec2-app",
				"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
			},
			wantCPU:    "",
			wantMemory: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECSRequest(t, h, "RegisterTaskDefinition", tt.input)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			td, ok := resp["taskDefinition"].(map[string]any)
			require.True(t, ok)

			if tt.wantCPU != "" {
				assert.Equal(t, tt.wantCPU, td["cpu"])
			} else {
				assert.Nil(t, td["cpu"])
			}

			if tt.wantMemory != "" {
				assert.Equal(t, tt.wantMemory, td["memory"])
			} else {
				assert.Nil(t, td["memory"])
			}

			if tt.wantPlatFamily != "" {
				assert.Equal(t, tt.wantPlatFamily, td["platformFamily"])
			}
		})
	}
}

// TestECS_ListTasksFiltered verifies ListTasks desiredStatus filter.
func TestECS_ListTasksFiltered(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "filter-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "filterapp",
		"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
	})

	doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "filter-cluster",
		"taskDefinition": "filterapp",
		"count":          2,
	})

	// All tasks (RUNNING desired status)
	rec := doECSRequest(t, h, "ListTasks", map[string]any{
		"cluster":       "filter-cluster",
		"desiredStatus": "RUNNING",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns, ok := resp["taskArns"].([]any)
	require.True(t, ok)
	assert.Len(t, arns, 2)

	// STOPPED filter — should return none
	rec2 := doECSRequest(t, h, "ListTasks", map[string]any{
		"cluster":       "filter-cluster",
		"desiredStatus": "STOPPED",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	arns2, ok := resp2["taskArns"].([]any)
	require.True(t, ok)
	assert.Empty(t, arns2)
}

// TestECS_CreateService_Tags verifies service tags are stored and returned.
func TestECS_CreateService_Tags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "default"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tag-svc-td",
		"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "tag-svc",
		"taskDefinition": "tag-svc-td",
		"desiredCount":   1,
		"tags":           []map[string]any{{"key": "env", "value": "staging"}},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	svc, ok := resp["service"].(map[string]any)
	require.True(t, ok)

	tags, ok := svc["tags"].([]any)
	require.True(t, ok)
	require.Len(t, tags, 1)

	tag := tags[0].(map[string]any)
	assert.Equal(t, "env", tag["key"])
	assert.Equal(t, "staging", tag["value"])
}

// TestECS_StartTask_StartedBy verifies StartTask propagates startedBy field.
func TestECS_StartTask_StartedBy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "start-by-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "startapp-by",
		"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
	})

	ci, err := h.Backend.RegisterContainerInstance("start-by-cluster", "i-started-by")
	require.NoError(t, err)

	rec := doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "start-by-cluster",
		"taskDefinition":     "startapp-by",
		"startedBy":          "container-agent",
		"containerInstances": []string{ci.ContainerInstanceArn},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks, ok := resp["tasks"].([]any)
	require.True(t, ok)
	require.Len(t, tasks, 1)

	task := tasks[0].(map[string]any)
	assert.Equal(t, "container-agent", task["startedBy"])
	assert.Equal(t, ci.ContainerInstanceArn, task["containerInstanceArn"])
}

// TestECS_ListTasksFiltered_ContainerInstance verifies ListTasks containerInstance filter.
func TestECS_ListTasksFiltered_ContainerInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ci-filter-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ci-filter-td",
		"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
	})

	ci1, err := h.Backend.RegisterContainerInstance("ci-filter-cluster", "i-aaa")
	require.NoError(t, err)

	ci2, err := h.Backend.RegisterContainerInstance("ci-filter-cluster", "i-bbb")
	require.NoError(t, err)

	// Start a task on ci1 and ci2 respectively
	doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "ci-filter-cluster",
		"taskDefinition":     "ci-filter-td",
		"containerInstances": []string{ci1.ContainerInstanceArn},
	})
	doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "ci-filter-cluster",
		"taskDefinition":     "ci-filter-td",
		"containerInstances": []string{ci2.ContainerInstanceArn},
	})

	// Filter by ci1 only
	rec := doECSRequest(t, h, "ListTasks", map[string]any{
		"cluster":           "ci-filter-cluster",
		"containerInstance": ci1.ContainerInstanceArn,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	arns, ok := resp["taskArns"].([]any)
	require.True(t, ok)
	assert.Len(t, arns, 1)
}
