package ecs_test

// parity_emr_test.go — table tests for parity gaps fixed in the audit-ecs branch:
//
//  1. Task lifecycle: PROVISIONING → PENDING → RUNNING (no-runner path)
//  2. PlacementStrategy forwarding from service to task launch
//  3. Deployment RolloutState advances to COMPLETED when desired count reached
//  4. Deployment per-deployment RunningCount populated in DescribeServices
//  5. ExecuteCommand rejects tasks without EnableExecuteCommand=true
//  6. Fargate ENI IDs are unique per task
//  7. TaskRoleArn resolved from override > task definition

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ---------------------------------------------------------------------------
// 1. Task lifecycle: PROVISIONING → PENDING → RUNNING (runner path)
// ---------------------------------------------------------------------------

func TestParityEMR_TaskLifecycle_PendingThenRunning(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 2. PlacementStrategy forwarding from service to task launch
// ---------------------------------------------------------------------------

func TestParityEMR_PlacementStrategy_ForwardedFromService(t *testing.T) {
	t.Parallel()

	b := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	// Register two container instances so the strategy has something to choose between.
	ci1, err := b.RegisterContainerInstance("default", "i-aaaa0001")
	require.NoError(t, err)
	ci2, err := b.RegisterContainerInstance("default", "i-aaaa0002")
	require.NoError(t, err)

	tdArn := makeTaskDef(t, b, "spread-td")

	svc, err := b.CreateService(ecs.CreateServiceInput{
		ServiceName:    "spread-svc",
		TaskDefinition: tdArn,
		DesiredCount:   2,
		LaunchType:     "EC2",
		PlacementStrategy: []ecs.PlacementStrategy{
			{Type: "spread", Field: "instanceId"},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, svc)

	// Two tasks via StartTaskForService — spread picks the least-loaded instance each time.
	require.NoError(t, b.StartTaskForService("default", "spread-svc", tdArn))
	require.NoError(t, b.StartTaskForService("default", "spread-svc", tdArn))

	tasks, _, err := b.DescribeTasks("default", nil)
	require.NoError(t, err)

	instanceCounts := make(map[string]int)
	for _, task := range tasks {
		if task.ContainerInstanceArn != "" {
			instanceCounts[task.ContainerInstanceArn]++
		}
	}

	assert.Equal(t, 1, instanceCounts[ci1.ContainerInstanceArn],
		"ci1 should host exactly one task with spread strategy")
	assert.Equal(t, 1, instanceCounts[ci2.ContainerInstanceArn],
		"ci2 should host exactly one task with spread strategy")
}

// ---------------------------------------------------------------------------
// 3 & 4. Deployment RolloutState COMPLETED + per-deployment RunningCount
// ---------------------------------------------------------------------------

func TestParityEMR_DeploymentRolloutState_CompletedWhenDesiredMet(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "rollout-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "rollout-td",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	createResp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "rollout-cluster",
		"serviceName":    "rollout-svc",
		"taskDefinition": "rollout-td",
		"desiredCount":   2,
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	// Run 2 tasks under this service's group so enrichService counts them.
	for range 2 {
		runResp := doECSRequest(t, h, "RunTask", map[string]any{
			"cluster":        "rollout-cluster",
			"taskDefinition": "rollout-td",
			"group":          "service:rollout-svc",
		})
		require.Equal(t, http.StatusOK, runResp.Code)
	}

	descResp := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "rollout-cluster",
		"services": []any{"rollout-svc"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &out))

	svcs, ok := out["services"].([]any)
	require.True(t, ok)
	require.Len(t, svcs, 1)

	svc := svcs[0].(map[string]any)
	deployments, ok := svc["deployments"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, deployments)

	primary := deployments[0].(map[string]any)
	assert.Equal(t, "PRIMARY", primary["status"])
	assert.Equal(t, "COMPLETED", primary["rolloutState"],
		"PRIMARY deployment must advance to COMPLETED when running >= desired")
	assert.Equal(t, 2, int(primary["runningCount"].(float64)),
		"per-deployment runningCount must reflect live tasks")
}

// ---------------------------------------------------------------------------
// 5. ExecuteCommand: rejects tasks without EnableExecuteCommand=true
// ---------------------------------------------------------------------------

func TestParityEMR_ExecuteCommand_RequiresFlag(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 6. Fargate ENI IDs are unique per task
// ---------------------------------------------------------------------------

func TestParityEMR_FargateENI_UniquePerTask(t *testing.T) {
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

// ---------------------------------------------------------------------------
// 7. TaskRoleArn resolved: override > task definition
// ---------------------------------------------------------------------------

func TestParityEMR_TaskRoleArn_Resolved(t *testing.T) {
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

// ---------------------------------------------------------------------------
// helpers
// ---------------------------------------------------------------------------

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
