package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

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
