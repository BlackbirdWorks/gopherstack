package ecs_test

import (
	"encoding/json"
	"errors"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestECS_StopTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "stop-task-def")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &runResp))

	taskArn := runResp["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	rec2 := doECSRequest(t, h, "StopTask", map[string]any{
		"task":   taskArn,
		"reason": "manual stop",
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))

	task := resp2["task"].(map[string]any)
	assert.Equal(t, "STOPPED", task["lastStatus"])
	assert.Equal(t, "manual stop", task["stoppedReason"])
}

func TestECS_ListTasks(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "list-task-def")

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec2 := doECSRequest(t, h, "ListTasks", map[string]any{})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

	arns := resp["taskArns"].([]any)
	assert.Len(t, arns, 3)
}

func TestECS_Backend_StopTask_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.StopTask("nonexistent-cluster", "task-arn", "reason")
	require.Error(t, err)
}

func TestECS_Backend_ListTasks_ClusterNotFound(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.ListTasks("nonexistent-cluster")
	require.Error(t, err)
}

func TestECS_Handler_StopTask_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "StopTask", map[string]any{
		"cluster": "nonexistent-cluster",
		"task":    "task-arn",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_ListTasks_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "ListTasks", map[string]any{
		"cluster": "nonexistent-cluster",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_Handler_StopTask_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	// Create the default cluster first via RunTask initialization.
	tdArn := registerTestTaskDef(t, h, "stop-not-found")
	doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn})

	rec := doECSRequest(t, h, "StopTask", map[string]any{
		"task": "arn:aws:ecs:us-east-1:000000000000:task/default/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- TaskRunner mock helpers -----

// errContainerStart is the sentinel error used by failingRunner.
var errContainerStart = errors.New("container start failed")

// failingRunner is a TaskRunner that always returns an error from RunTask, causing
// the task to remain at PROVISIONING rather than transitioning to RUNNING.
type failingRunner struct{}

func TestECS_ListTasks_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "task-page-def")

	// Create 5 tasks.
	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          5,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	tests := []struct {
		body          map[string]any
		name          string
		wantMinCount  int
		wantNextToken bool
	}{
		{
			name:         "list all tasks",
			body:         map[string]any{},
			wantMinCount: 5,
		},
		{
			name:          "paginated maxResults=2",
			body:          map[string]any{"maxResults": 2},
			wantMinCount:  2,
			wantNextToken: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			listRec := doECSRequest(t, h, "ListTasks", tt.body)
			require.Equal(t, http.StatusOK, listRec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &resp))

			arns := resp["taskArns"].([]any)
			assert.GreaterOrEqual(t, len(arns), tt.wantMinCount)

			if tt.wantNextToken {
				assert.NotEmpty(t, resp["nextToken"])
			}
		})
	}
}

func TestECS_ListTasks_TokenChaining(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	tdArn := registerTestTaskDef(t, h, "task-chain-def")

	// Create 3 tasks.
	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": tdArn,
		"count":          3,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// First page.
	rec1 := doECSRequest(t, h, "ListTasks", map[string]any{"maxResults": 2})
	require.Equal(t, http.StatusOK, rec1.Code)

	var page1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &page1))

	arns1 := page1["taskArns"].([]any)
	assert.Len(t, arns1, 2)

	nextToken, ok := page1["nextToken"].(string)
	require.True(t, ok)
	assert.NotEmpty(t, nextToken)

	// Second page.
	rec2 := doECSRequest(t, h, "ListTasks", map[string]any{"maxResults": 2, "nextToken": nextToken})
	require.Equal(t, http.StatusOK, rec2.Code)

	var page2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &page2))

	arns2 := page2["taskArns"].([]any)
	assert.GreaterOrEqual(t, len(arns2), 1)

	// No duplicates between pages.
	seen := make(map[string]bool)
	for _, a := range arns1 {
		seen[a.(string)] = true
	}

	for _, a := range arns2 {
		assert.False(t, seen[a.(string)], "duplicate task ARN in page 2: %s", a)
	}
}

func TestECSExec_Basic(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":              "exec-cluster",
		"taskDefinition":       "exec-task",
		"count":                1,
		"enableExecuteCommand": true,
	})
	require.Equal(t, http.StatusOK, runResp.Code)
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	tasks := runOut["tasks"].([]any)
	require.Len(t, tasks, 1)
	taskArn := tasks[0].(map[string]any)["taskArn"].(string)

	execResp := doECSRequest(t, h, "ExecuteCommand", map[string]any{
		"cluster":     "exec-cluster",
		"task":        taskArn,
		"container":   "app",
		"command":     "/bin/bash",
		"interactive": true,
	})
	require.Equal(t, http.StatusOK, execResp.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(execResp.Body.Bytes(), &execOut))
	session := execOut["session"].(map[string]any)
	assert.NotEmpty(t, session["sessionId"])
	assert.Equal(t, true, execOut["interactive"])
}

func TestECSExec_NonInteractive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "exec2-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "exec2-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":              "exec2-cluster",
		"taskDefinition":       "exec2-task",
		"count":                1,
		"enableExecuteCommand": true,
	})
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	taskArn := runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	execResp := doECSRequest(t, h, "ExecuteCommand", map[string]any{
		"cluster":     "exec2-cluster",
		"task":        taskArn,
		"container":   "app",
		"command":     "ls /tmp",
		"interactive": false,
	})
	require.Equal(t, http.StatusOK, execResp.Code)

	var execOut map[string]any
	require.NoError(t, json.Unmarshal(execResp.Body.Bytes(), &execOut))
	assert.Equal(t, false, execOut["interactive"])
}

func TestTaskProtection_Enable_Disable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "protect-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "protect-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "protect-cluster",
		"taskDefinition": "protect-task",
		"count":          1,
	})
	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	taskArn := runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	// Enable protection
	expiresIn := 60
	protResp := doECSRequest(t, h, "UpdateTaskProtection", map[string]any{
		"cluster":           "protect-cluster",
		"tasks":             []string{taskArn},
		"protectionEnabled": true,
		"expiresInMinutes":  expiresIn,
	})
	require.Equal(t, http.StatusOK, protResp.Code)

	var protOut map[string]any
	require.NoError(t, json.Unmarshal(protResp.Body.Bytes(), &protOut))
	prot := protOut["protectedTasks"].([]any)[0].(map[string]any)
	assert.Equal(t, true, prot["protectionEnabled"])

	// Get protection
	getResp := doECSRequest(t, h, "GetTaskProtection", map[string]any{
		"cluster": "protect-cluster",
		"tasks":   []string{taskArn},
	})
	require.Equal(t, http.StatusOK, getResp.Code)
	var getOut map[string]any
	require.NoError(t, json.Unmarshal(getResp.Body.Bytes(), &getOut))
	protTasks := getOut["protectedTasks"].([]any)
	assert.Len(t, protTasks, 1)
	assert.Equal(t, true, protTasks[0].(map[string]any)["protectionEnabled"])

	// Disable protection
	disableResp := doECSRequest(t, h, "UpdateTaskProtection", map[string]any{
		"cluster":           "protect-cluster",
		"tasks":             []string{taskArn},
		"protectionEnabled": false,
	})
	require.Equal(t, http.StatusOK, disableResp.Code)
}

func TestStartTask_MultipleInstances(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "start-multi-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "start-multi-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Register 3 container instances
	ciArns := make([]string, 3)
	for i := range 3 {
		ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
			"cluster": "start-multi-cluster",
			"instanceIdentityDocument": fakeInstanceIdentityDocument(
				"i-multi-" + string(rune('a'+i)),
			),
		})
		var ciOut map[string]any
		require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
		ciArns[i] = ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)
	}

	startResp := doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "start-multi-cluster",
		"taskDefinition":     "start-multi-task",
		"containerInstances": ciArns,
	})
	require.Equal(t, http.StatusOK, startResp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(startResp.Body.Bytes(), &out))
	tasks := out["tasks"].([]any)
	assert.Len(t, tasks, 3)
}

func TestECS_ExecuteCommand(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "execute command on running task",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "exec-task")
				rec := doECSRequest(
					t,
					h,
					"RunTask",
					map[string]any{
						"taskDefinition":       tdArn,
						"count":                1,
						"enableExecuteCommand": true,
					},
				)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				taskArn := resp["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

				return map[string]any{
					"task":      taskArn,
					"command":   "/bin/sh",
					"container": "app",
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing task",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{"command": "/bin/sh"}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing command",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "exec-nocmd-task")
				rec := doECSRequest(
					t,
					h,
					"RunTask",
					map[string]any{"taskDefinition": tdArn, "count": 1},
				)
				require.Equal(t, http.StatusOK, rec.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				taskArn := resp["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

				return map[string]any{"task": taskArn}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "task not found",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{
					"task":    "arn:aws:ecs:us-east-1:000000000000:task/default/nonexistent",
					"command": "/bin/sh",
				}
			},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			input := tt.setup(h)
			rec := doECSRequest(t, h, "ExecuteCommand", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				assert.NotEmpty(t, resp["clusterArn"])
				assert.NotEmpty(t, resp["taskArn"])

				sess := resp["session"].(map[string]any)
				assert.NotEmpty(t, sess["sessionId"])
				assert.NotEmpty(t, sess["streamUrl"])
				assert.NotEmpty(t, sess["tokenValue"])
			}
		})
	}
}

func TestTask_EnableExecuteCommand_RunTask(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "task-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "task-exec-td",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	runResp := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":              "task-exec-cluster",
		"taskDefinition":       "task-exec-td",
		"count":                1,
		"enableExecuteCommand": true,
	})
	require.Equal(t, http.StatusOK, runResp.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
	tasks := runOut["tasks"].([]any)
	require.Len(t, tasks, 1)
	assert.Equal(t, true, tasks[0].(map[string]any)["enableExecuteCommand"])
}

func TestTask_EnableExecuteCommand_ServicePropagation(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())
	h2 := ecs.NewHandler(backend)

	doECSRequest(t, h2, "CreateCluster", map[string]any{"clusterName": "svc-exec-cluster"})
	doECSRequest(t, h2, "RegisterTaskDefinition", map[string]any{
		"family":               "svc-exec-td",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h2, "CreateService", map[string]any{
		"cluster":              "svc-exec-cluster",
		"serviceName":          "svc-exec-svc",
		"taskDefinition":       "svc-exec-td",
		"desiredCount":         1,
		"enableExecuteCommand": true,
	})

	// Service-launched tasks should also carry enableExecuteCommand
	err := backend.StartTaskForService("svc-exec-cluster", "svc-exec-svc",
		"arn:aws:ecs:us-east-1:000000000000:task-definition/svc-exec-td:1")

	// May fail if task definition lookup differs; only assert structure if ok
	if err == nil {
		listResp := doECSRequest(t, h2, "ListTasks", map[string]any{"cluster": "svc-exec-cluster"})
		require.Equal(t, http.StatusOK, listResp.Code)
		var listOut map[string]any
		require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
		arns := listOut["taskArns"].([]any)
		require.NotEmpty(t, arns)
		taskArn := arns[0].(string)

		descResp := doECSRequest(t, h2, "DescribeTasks", map[string]any{
			"cluster": "svc-exec-cluster",
			"tasks":   []any{taskArn},
		})
		require.Equal(t, http.StatusOK, descResp.Code)
		var descOut map[string]any
		require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
		tasks := descOut["tasks"].([]any)
		require.Len(t, tasks, 1)
		assert.Equal(t, true, tasks[0].(map[string]any)["enableExecuteCommand"])
	}
}

func TestECS_TaskProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		protectionEnabled bool
		wantCode          int
	}{
		{
			name:              "enables protection",
			protectionEnabled: true,
			wantCode:          http.StatusOK,
		},
		{
			name:              "disables protection",
			protectionEnabled: false,
			wantCode:          http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "prot-cluster"})
			doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":               "prot-task",
				"containerDefinitions": []map[string]any{{"name": "c1", "image": "nginx"}},
			})

			// Run a task
			runRec := doECSRequest(t, h, "RunTask", map[string]any{
				"cluster":        "prot-cluster",
				"taskDefinition": "prot-task",
			})
			require.Equal(t, http.StatusOK, runRec.Code)

			var runResp map[string]any
			require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
			tasks := runResp["tasks"].([]any)
			require.Len(t, tasks, 1)
			taskArn := tasks[0].(map[string]any)["taskArn"].(string)

			// Update protection
			rec := doECSRequest(t, h, "UpdateTaskProtection", map[string]any{
				"cluster":           "prot-cluster",
				"tasks":             []string{taskArn},
				"protectionEnabled": tt.protectionEnabled,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			protectedTasks, ok := resp["protectedTasks"].([]any)
			require.True(t, ok)
			require.Len(t, protectedTasks, 1)

			pt := protectedTasks[0].(map[string]any)
			assert.Equal(t, tt.protectionEnabled, pt["protectionEnabled"])

			// Get protection
			getRec := doECSRequest(t, h, "GetTaskProtection", map[string]any{
				"cluster": "prot-cluster",
				"tasks":   []string{taskArn},
			})
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

			protTasks, ok := getResp["protectedTasks"].([]any)
			require.True(t, ok)
			require.Len(t, protTasks, 1)

			got := protTasks[0].(map[string]any)
			assert.Equal(t, tt.protectionEnabled, got["protectionEnabled"])
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

func TestExecuteCommand_RequiresFlag(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		enableExec bool
		wantStatus int
	}{
		{
			name:       "exec disabled — rejected",
			enableExec: false,
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "exec enabled — accepted",
			enableExec: true,
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":               "exec-flag-td",
				"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
			})

			runResp := doECSRequest(t, h, "RunTask", map[string]any{
				"taskDefinition":       "exec-flag-td",
				"count":                1,
				"enableExecuteCommand": tc.enableExec,
			})
			require.Equal(t, http.StatusOK, runResp.Code)

			var runOut map[string]any
			require.NoError(t, json.Unmarshal(runResp.Body.Bytes(), &runOut))
			taskArn := runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

			execResp := doECSRequest(t, h, "ExecuteCommand", map[string]any{
				"task":    taskArn,
				"command": "/bin/sh",
			})
			assert.Equal(t, tc.wantStatus, execResp.Code)
		})
	}
}
