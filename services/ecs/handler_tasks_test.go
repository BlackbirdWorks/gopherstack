package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestECS_RunTask(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "run single task",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "run-task")

				return map[string]any{
					"taskDefinition": tdArn,
					"count":          1,
				}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "run multiple tasks",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "run-multi")

				return map[string]any{
					"taskDefinition": tdArn,
					"count":          3,
				}
			},
			wantCode: http.StatusOK,
			wantLen:  3,
		},
		{
			name: "default count is 1",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "run-default")

				return map[string]any{"taskDefinition": tdArn}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "missing task definition",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			input := tt.setup(h)
			rec := doECSRequest(t, h, "RunTask", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				tasks := resp["tasks"].([]any)
				assert.Len(t, tasks, tt.wantLen)

				task := tasks[0].(map[string]any)
				assert.NotEmpty(t, task["taskArn"])
				assert.Equal(t, "RUNNING", task["lastStatus"])
				assert.Equal(t, "RUNNING", task["desiredStatus"])
			}
		})
	}
}

func TestECS_DescribeTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "desc-task-t")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          2,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &runResp))

	taskArns := make([]string, 0, 2)
	for _, t := range runResp["tasks"].([]any) {
		taskArns = append(taskArns, t.(map[string]any)["taskArn"].(string))
	}

	// Describe all tasks on cluster.
	rec2 := doECSRequest(t, h, "DescribeTasks", map[string]any{"tasks": []string{}})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.Len(t, resp2["tasks"].([]any), 2)

	// Describe specific task.
	rec3 := doECSRequest(t, h, "DescribeTasks", map[string]any{"tasks": []string{taskArns[0]}})
	require.Equal(t, http.StatusOK, rec3.Code)

	var resp3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &resp3))
	assert.Len(t, resp3["tasks"].([]any), 1)
}

func TestECS_Backend_DescribeTasks_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, _, err := backend.DescribeTasks("nonexistent-cluster", []string{"task-arn"})
	require.Error(t, err)
}

func TestECS_Handler_RunTask_WithGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "group-task")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          1,
		"group":          "service:my-svc",
		"launchType":     "EC2",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks := resp["tasks"].([]any)
	task := tasks[0].(map[string]any)
	assert.Equal(t, "service:my-svc", task["group"])
	assert.Equal(t, "EC2", task["launchType"])
}

func TestECS_Backend_RunTask_LaunchTypeDefault(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "default-lt-run",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)
	// Default launch type is FARGATE.
	assert.Equal(t, "FARGATE", tasks[0].LaunchType)
}

func TestECS_Handler_DescribeTasks_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// First ensure the default cluster exists.
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "default"})
	rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"tasks": []string{"arn:aws:ecs:us-east-1:000000000000:task/default/nonexistent"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	tasks, _ := resp["tasks"].([]any)
	assert.Empty(t, tasks)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 1)
}

func (r *failingRunner) RunTask(
	_ *ecs.Task,
	_ *ecs.TaskDefinition,
) error {
	return errContainerStart
}

func (r *failingRunner) StopTask(_ *ecs.Task) error { return nil }

// TestECS_Backend_RunTask_ProvisioningStaysOnRunnerError verifies that when the
// TaskRunner returns an error, the task transitions to STOPPED (not stuck in PROVISIONING).
func TestECS_Backend_RunTask_ProvisioningStaysOnRunnerError(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, &failingRunner{})

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "prov-err-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx:latest"}},
	})
	require.NoError(t, err)

	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// Task must transition to STOPPED when the runner fails rather than staying
	// in PROVISIONING forever, which would be a resource leak with wrong semantics.
	assert.Equal(t, "STOPPED", tasks[0].LastStatus)
	assert.Equal(t, "STOPPED", tasks[0].DesiredStatus)
	assert.NotNil(t, tasks[0].StoppedAt)
	assert.NotEmpty(t, tasks[0].StoppedReason)
}

// TestECS_Backend_RunTask_TransitionToRunningWithRunner verifies that when the
// TaskRunner succeeds, the task transitions from PROVISIONING to RUNNING.
func TestECS_Backend_RunTask_TransitionToRunningWithRunner(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "prov-ok-task",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx:latest"}},
	})
	require.NoError(t, err)

	tasks, err := backend.RunTask(ecs.RunTaskInput{
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	// Noop runner succeeds → task transitions to RUNNING.
	assert.Equal(t, "RUNNING", tasks[0].LastStatus)
}

func TestRunTask_WithOverrides(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "override-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "override-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "override-cluster",
		"taskDefinition": "override-task",
		"count":          1,
		"overrides": map[string]any{
			"containerOverrides": []any{
				map[string]any{
					"name":    "app",
					"command": []string{"node", "server.js"},
					"environment": []any{
						map[string]any{"name": "PORT", "value": "8080"},
					},
				},
			},
			"taskRoleArn": "arn:aws:iam::000000000000:role/task-role",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	require.Len(t, tasks, 1)
}

func TestRunTask_Count_Multiple(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "multi-run-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "multi-run-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "multi-run-cluster",
		"taskDefinition": "multi-run-task",
		"count":          5,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	assert.Len(t, tasks, 5)

	// Each task has unique ARN
	arns := make(map[string]bool)
	for _, task := range tasks {
		arn := task.(map[string]any)["taskArn"].(string)
		arns[arn] = true
	}
	assert.Len(t, arns, 5)
}

func TestDescribeTasks_FailureSemantics(t *testing.T) {
	t.Parallel()

	t.Run("unknown returns failure", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dt-cluster"})

		rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
			"cluster": "dt-cluster",
			"tasks":   []string{"arn:aws:ecs:us-east-1:000000000000:task/dt-cluster/ghost"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		tasks, _ := resp["tasks"].([]any)
		assert.Empty(t, tasks)

		failures, _ := resp["failures"].([]any)
		require.Len(t, failures, 1)

		f := failures[0].(map[string]any)
		assert.Contains(t, f["arn"], "ghost")
		assert.Equal(t, "MISSING", f["reason"])
	})

	t.Run("mix found and missing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		tdArn := registerTestTaskDef(t, h, "dt-mix-task")
		runResp := doECSRequest(t, h, "RunTask", map[string]any{
			"taskDefinition": tdArn,
			"count":          1,
		})
		require.Equal(t, http.StatusOK, runResp.Code)

		var runOut map[string]any
		require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
		tasksRaw, _ := runOut["tasks"].([]any)
		require.Len(t, tasksRaw, 1)
		taskArn := tasksRaw[0].(map[string]any)["taskArn"].(string)

		rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
			"tasks": []string{taskArn, "arn:aws:ecs:us-east-1:000000000000:task/default/ghost"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		tasks, _ := resp["tasks"].([]any)
		assert.Len(t, tasks, 1)

		failures, _ := resp["failures"].([]any)
		assert.Len(t, failures, 1)
	})

	t.Run("cluster not found still errors", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
			"cluster": "nonexistent-cluster",
			"tasks":   []string{"some-task-arn"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("multiple unknown", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dt-multi"})

		rec := doECSRequest(t, h, "DescribeTasks", map[string]any{
			"cluster": "dt-multi",
			"tasks": []string{
				"arn:aws:ecs:us-east-1:000000000000:task/dt-multi/ghost1",
				"arn:aws:ecs:us-east-1:000000000000:task/dt-multi/ghost2",
				"arn:aws:ecs:us-east-1:000000000000:task/dt-multi/ghost3",
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		tasks, _ := resp["tasks"].([]any)
		assert.Empty(t, tasks)

		failures, _ := resp["failures"].([]any)
		assert.Len(t, failures, 3)
	})
}

func TestHandler_RunTask_NetworkConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "myapp",
		"count":          1,
		"networkConfiguration": map[string]any{
			"awsvpcConfiguration": map[string]any{
				"subnets":        []string{"subnet-abc123"},
				"assignPublicIp": "DISABLED",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	nc := task["networkConfiguration"].(map[string]any)
	avpc := nc["awsvpcConfiguration"].(map[string]any)
	subnets := avpc["subnets"].([]any)
	require.Len(t, subnets, 1)
	assert.Equal(t, "subnet-abc123", subnets[0].(string))
}

// TestHandler_RunTask_PlatformVersion verifies platform-version validation:
// an unknown version is rejected while LATEST is accepted.
func TestHandler_RunTask_PlatformVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		platformVersion string
		wantErrSubstr   string
		wantStatus      int
	}{
		{
			name:            "invalid platform version rejected",
			platformVersion: "9.9.9",
			wantErrSubstr:   "platform version",
			wantStatus:      http.StatusBadRequest,
		},
		{
			name:            "LATEST platform version accepted",
			platformVersion: "LATEST",
			wantErrSubstr:   "",
			wantStatus:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":               "myapp",
				"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
			})

			rec := doECSRequest(t, h, "RunTask", map[string]any{
				"taskDefinition":  "myapp",
				"count":           1,
				"platformVersion": tt.platformVersion,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantErrSubstr != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrSubstr)
			}
		})
	}
}

func TestHandler_RunTask_PropagateTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "myapp",
		"count":          1,
		"propagateTags":  "TASK_DEFINITION",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	assert.Equal(t, "TASK_DEFINITION", task["propagateTags"])
}
