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

func TestRunTask_ContainersField_Populated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a task def with two containers.
	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "web",
		"containerDefinitions": []map[string]any{
			{
				"name": "nginx", "image": "nginx:latest", "cpu": 256, "memory": 512,
				"portMappings": []map[string]any{
					{"containerPort": 80, "protocol": "tcp"},
				},
			},
			{"name": "sidecar", "image": "alpine:3", "cpu": 128},
		},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)

	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	// Run a task.
	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          1,
	})
	require.Equal(t, http.StatusOK, runRec.Code, runRec.Body.String())

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))

	tasks := runOut["tasks"].([]any)
	require.Len(t, tasks, 1)

	task := tasks[0].(map[string]any)

	// Task must have a containers field.
	require.Contains(t, task, "containers", "task response must include containers")
	containers := task["containers"].([]any)
	require.Len(t, containers, 2)

	// First container: nginx.
	c0 := containers[0].(map[string]any)
	assert.Equal(t, "nginx", c0["name"])
	assert.Equal(t, "nginx:latest", c0["image"])
	assert.NotEmpty(t, c0["containerArn"])
	assert.NotEmpty(t, c0["lastStatus"])

	// Network bindings should reflect the port mapping.
	bindings, ok := c0["networkBindings"].([]any)
	require.True(t, ok, "nginx container must have networkBindings")
	require.NotEmpty(t, bindings)
	b0 := bindings[0].(map[string]any)
	assert.InDelta(t, 80.0, b0["containerPort"], 0)

	// Second container: sidecar.
	c1 := containers[1].(map[string]any)
	assert.Equal(t, "sidecar", c1["name"])
}

func TestRunTask_ContainersStatus_Synced_To_Running(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "simple",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "app:1"},
		},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)

	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	runRec := doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	task := runOut["tasks"].([]any)[0].(map[string]any)

	containers := task["containers"].([]any)
	require.Len(t, containers, 1)

	// With noop runner the task goes straight to RUNNING.
	assert.Equal(t, "RUNNING", task["lastStatus"])
	assert.Equal(t, "RUNNING", containers[0].(map[string]any)["lastStatus"])
}

func TestRunTask_ContainersHealthStatus_WithHealthCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "healthapp",
		"containerDefinitions": []map[string]any{
			{
				"name":  "app",
				"image": "app:1",
				"healthCheck": map[string]any{
					"command":  []string{"CMD", "curl", "-f", "http://localhost/health"},
					"interval": 30,
					"timeout":  5,
					"retries":  3,
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)

	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	runRec := doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	task := runOut["tasks"].([]any)[0].(map[string]any)

	containers := task["containers"].([]any)
	require.Len(t, containers, 1)

	// After running, healthStatus should be HEALTHY (transitioned from UNKNOWN).
	healthStatus := containers[0].(map[string]any)["healthStatus"]
	assert.Equal(t, "HEALTHY", healthStatus)
}

func TestRunTask_PropagateTagsTaskDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register task def with tags.
	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tagtest",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "alpine"}},
		"tags": []map[string]any{
			{"key": "env", "value": "prod"},
			{"key": "owner", "value": "platform"},
		},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	// Run a task with propagateTags=TASK_DEFINITION.
	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"propagateTags":  "TASK_DEFINITION",
	})
	require.Equal(t, http.StatusOK, runRec.Code, runRec.Body.String())

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	task := runOut["tasks"].([]any)[0].(map[string]any)

	tags, ok := task["tags"].([]any)
	require.True(t, ok, "task must have tags when propagateTags=TASK_DEFINITION")
	require.NotEmpty(t, tags, "tags must be non-empty")

	// Verify the task definition tags were propagated.
	tagMap := make(map[string]string)

	for _, raw := range tags {
		t2 := raw.(map[string]any)
		tagMap[t2["key"].(string)] = t2["value"].(string)
	}

	assert.Equal(t, "prod", tagMap["env"])
	assert.Equal(t, "platform", tagMap["owner"])
}

func TestRunTask_PropagateTagsTaskDefinition_ExplicitTagWins(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tagmerge",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "alpine"}},
		"tags":                 []map[string]any{{"key": "env", "value": "staging"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	// Explicit tag overrides the propagated one.
	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"propagateTags":  "TASK_DEFINITION",
		"tags":           []map[string]any{{"key": "env", "value": "prod"}},
	})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	task := runOut["tasks"].([]any)[0].(map[string]any)

	tags := task["tags"].([]any)
	tagMap := make(map[string]string)

	for _, raw := range tags {
		t2 := raw.(map[string]any)
		tagMap[t2["key"].(string)] = t2["value"].(string)
	}

	// Explicit "prod" wins over propagated "staging".
	assert.Equal(t, "prod", tagMap["env"])
}

func TestRunTask_ECSManagedTags_Injected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "managed",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "alpine"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "mycluster"})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":              "mycluster",
		"taskDefinition":       tdArn,
		"enableECSManagedTags": true,
	})
	require.Equal(t, http.StatusOK, runRec.Code, runRec.Body.String())

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	task := runOut["tasks"].([]any)[0].(map[string]any)

	tags, ok := task["tags"].([]any)
	require.True(t, ok, "task must have managed tags when enableECSManagedTags=true")

	tagMap := make(map[string]string)

	for _, raw := range tags {
		t2 := raw.(map[string]any)
		tagMap[t2["key"].(string)] = t2["value"].(string)
	}

	assert.Equal(t, "mycluster", tagMap["aws:ecs:clusterName"])
	assert.Equal(t, "managed", tagMap["aws:ecs:taskDefinitionFamily"])
	assert.NotEmpty(t, tagMap["aws:ecs:taskDefinitionRevision"])
}

func TestRunTask_Tags_InResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "tagresp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "alpine"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"tags":           []map[string]any{{"key": "team", "value": "sre"}},
	})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	task := runOut["tasks"].([]any)[0].(map[string]any)

	tags, ok := task["tags"].([]any)
	require.True(t, ok, "task response must include tags")
	require.Len(t, tags, 1)

	tag := tags[0].(map[string]any)
	assert.Equal(t, "team", tag["key"])
	assert.Equal(t, "sre", tag["value"])
}

func TestDescribeTasks_ContainersField_Present(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "describe",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	runRec := doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	taskArn := runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	// DescribeTasks must also return containers.
	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"tasks": []string{taskArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	tasks := descOut["tasks"].([]any)
	require.Len(t, tasks, 1)

	task := tasks[0].(map[string]any)
	containers, ok := task["containers"].([]any)
	require.True(t, ok, "DescribeTasks must include containers")
	require.NotEmpty(t, containers)
	assert.Equal(t, "web", containers[0].(map[string]any)["name"])
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

func TestTaskLifecycle_PendingThenRunning(t *testing.T) {
	t.Parallel()

	// With the NoopRunner, the task transitions PROVISIONING → PENDING → RUNNING
	// synchronously before RunTask returns. The task must never be stuck in PROVISIONING.
	b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	tdArn := makeTaskDef(t, b, "lifecycle-td")

	tasks, err := b.RunTask(ecs.RunTaskInput{
		TaskDefinition: tdArn,
		Count:          1,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 1)

	assert.Equal(t, "RUNNING", tasks[0].LastStatus,
		"task must reach RUNNING (passed through PENDING)")
}

func TestFargateENI_UniquePerTask(t *testing.T) {
	t.Parallel()

	b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())
	tdArn := makeTaskDef(t, b, "eni-td")

	tasks, err := b.RunTask(ecs.RunTaskInput{
		TaskDefinition: tdArn,
		LaunchType:     "FARGATE",
		Count:          3,
	})
	require.NoError(t, err)
	require.Len(t, tasks, 3)

	eniIDs := make(map[string]bool)

	for _, task := range tasks {
		require.NotEmpty(t, task.Attachments, "Fargate task must have ENI attachment")

		for _, detail := range task.Attachments[0].Details {
			if detail.Name == "networkInterfaceId" {
				assert.False(t, eniIDs[detail.Value],
					"duplicate ENI ID %q across tasks", detail.Value)
				eniIDs[detail.Value] = true
			}
		}
	}

	assert.Len(t, eniIDs, 3, "each Fargate task must have a unique ENI ID")
}

func TestTaskRoleArn_Resolved(t *testing.T) {
	t.Parallel()

	b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	tdOut, err := b.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:      "role-td",
		TaskRoleArn: "arn:aws:iam::123456789012:role/td-role",
		ContainerDefinitions: []ecs.ContainerDefinition{
			{Name: "app", Image: "nginx"},
		},
	})
	require.NoError(t, err)

	t.Run("inherits task def role when no override", func(t *testing.T) {
		t.Parallel()

		tasks, runErr := b.RunTask(ecs.RunTaskInput{
			TaskDefinition: tdOut.TaskDefinitionArn,
			Count:          1,
		})
		require.NoError(t, runErr)
		require.Len(t, tasks, 1)
		assert.Equal(t, "arn:aws:iam::123456789012:role/td-role", tasks[0].TaskRoleArn)
	})

	t.Run("override role wins over task def role", func(t *testing.T) {
		t.Parallel()

		overrideRole := "arn:aws:iam::123456789012:role/override-role"
		tasks, runErr := b.RunTask(ecs.RunTaskInput{
			TaskDefinition: tdOut.TaskDefinitionArn,
			Count:          1,
			Overrides: &ecs.TaskOverride{
				TaskRoleArn: overrideRole,
			},
		})
		require.NoError(t, runErr)
		require.Len(t, tasks, 1)
		assert.Equal(t, overrideRole, tasks[0].TaskRoleArn)
	})

	t.Run("no role when task def has none and no override", func(t *testing.T) {
		t.Parallel()

		tdNoRole, regErr := b.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
			Family: "norole-td",
			ContainerDefinitions: []ecs.ContainerDefinition{
				{Name: "app", Image: "nginx"},
			},
		})
		require.NoError(t, regErr)

		tasks, runErr := b.RunTask(ecs.RunTaskInput{
			TaskDefinition: tdNoRole.TaskDefinitionArn,
			Count:          1,
		})
		require.NoError(t, runErr)
		require.Len(t, tasks, 1)
		assert.Empty(t, tasks[0].TaskRoleArn)
	})
}

func makeTaskDef(t *testing.T, b *ecs.InMemoryBackend, family string) string {
	t.Helper()

	td, err := b.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family: family,
		ContainerDefinitions: []ecs.ContainerDefinition{
			{Name: "app", Image: "nginx"},
		},
	})
	require.NoError(t, err)

	return td.TaskDefinitionArn
}
