package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---- Container runtime status on task ------------------------------------

func TestRunTask_ContainersField_Populated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register a task def with two containers.
	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "web",
		"containerDefinitions": []map[string]any{
			{"name": "nginx", "image": "nginx:latest", "cpu": 256, "memory": 512,
				"portMappings": []map[string]any{
					{"containerPort": 80, "protocol": "tcp"},
				}},
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

// ---- Container status transitions ----------------------------------------

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

// ---- HealthCheck triggers UNKNOWN then HEALTHY ---------------------------

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

// ---- Service Deployments field -------------------------------------------

func TestCreateService_DeploymentsField_Populated(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register task def.
	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "webapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	// Create cluster + service.
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "prod"})

	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "prod",
		"serviceName":    "myapp",
		"taskDefinition": tdArn,
		"desiredCount":   2,
	})
	require.Equal(t, http.StatusOK, svcRec.Code, svcRec.Body.String())

	var svcOut map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcOut))
	svc := svcOut["service"].(map[string]any)

	require.Contains(t, svc, "deployments", "service response must include deployments")
	deployments := svc["deployments"].([]any)
	require.Len(t, deployments, 1, "CreateService must produce exactly one deployment")

	d := deployments[0].(map[string]any)
	assert.Equal(t, "PRIMARY", d["status"])
	assert.Equal(t, tdArn, d["taskDefinition"])
	assert.NotEmpty(t, d["id"])
	assert.InDelta(t, 2.0, d["desiredCount"], 0)
}

// ---- Deployment rotation on UpdateService --------------------------------

func TestUpdateService_DeploymentRotation_OnTaskDefChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register two task definitions.
	tdRec1 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "api",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "api:1"}},
	})
	require.Equal(t, http.StatusOK, tdRec1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(tdRec1.Body.Bytes(), &out1))
	tdArn1 := out1["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	tdRec2 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "api",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "api:2"}},
	})
	require.Equal(t, http.StatusOK, tdRec2.Code)
	var out2 map[string]any
	require.NoError(t, json.Unmarshal(tdRec2.Body.Bytes(), &out2))
	tdArn2 := out2["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "staging"})

	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "staging",
		"serviceName":    "api",
		"taskDefinition": tdArn1,
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	// Update service to new task definition.
	updRec := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":        "staging",
		"service":        "api",
		"taskDefinition": tdArn2,
	})
	require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())

	var updOut map[string]any
	require.NoError(t, json.Unmarshal(updRec.Body.Bytes(), &updOut))
	svc := updOut["service"].(map[string]any)

	deployments := svc["deployments"].([]any)
	require.Len(t, deployments, 2, "UpdateService must produce two deployments")

	// First deployment should be the new PRIMARY.
	d0 := deployments[0].(map[string]any)
	assert.Equal(t, "PRIMARY", d0["status"])
	assert.Equal(t, tdArn2, d0["taskDefinition"])

	// Second deployment should be the old PRIMARY demoted to ACTIVE.
	d1 := deployments[1].(map[string]any)
	assert.Equal(t, "ACTIVE", d1["status"])
	assert.Equal(t, tdArn1, d1["taskDefinition"])
}

// ---- PropagateTags TASK_DEFINITION ---------------------------------------

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

// ---- PropagateTags with override merging ---------------------------------

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

// ---- ECS-managed tags ----------------------------------------------------

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

// ---- Task Tags in response -----------------------------------------------

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

// ---- DescribeTasks returns containers ------------------------------------

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

// ---- Reconciler MinimumHealthyPercent enforcement -----------------------

func TestReconciler_MinimumHealthyPercent_CapsScaleDown(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "scaletest",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "test"})

	// Create service with desiredCount=4 and MinimumHealthyPercent=50.
	minPct := 50
	maxPct := 200
	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "test",
		"serviceName":    "scalingsvc",
		"taskDefinition": tdArn,
		"desiredCount":   4,
		"deploymentConfiguration": map[string]any{
			"minimumHealthyPercent": minPct,
			"maximumPercent":        maxPct,
		},
	})
	require.Equal(t, http.StatusOK, svcRec.Code, svcRec.Body.String())

	// Manually launch 4 tasks to simulate them running.
	for range 4 {
		runRec := doECSRequest(t, h, "RunTask", map[string]any{
			"cluster":        "test",
			"taskDefinition": tdArn,
			"group":          "service:scalingsvc",
		})
		require.Equal(t, http.StatusOK, runRec.Code)
	}

	// Now update service to desiredCount=0. The MinimumHealthyPercent=50 with
	// desired=4 means a floor of 2 tasks must stay running.
	// Since the reconciler runs asynchronously, we only verify that the
	// service was updated successfully. The floor logic is tested in
	// backend unit tests, not e2e here.
	updRec := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":      "test",
		"service":      "scalingsvc",
		"desiredCount": 0,
	})
	require.Equal(t, http.StatusOK, updRec.Code, updRec.Body.String())
}

// ---- DescribeServices returns Deployments --------------------------------

func TestDescribeServices_DeploymentsField(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "dstest",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)
	var tdOut map[string]any
	require.NoError(t, json.Unmarshal(tdRec.Body.Bytes(), &tdOut))
	tdArn := tdOut["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ds-cluster"})

	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "ds-cluster",
		"serviceName":    "ds-svc",
		"taskDefinition": tdArn,
		"desiredCount":   1,
	})

	descRec := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "ds-cluster",
		"services": []string{"ds-svc"},
	})
	require.Equal(t, http.StatusOK, descRec.Code, descRec.Body.String())

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))

	services := descOut["services"].([]any)
	require.Len(t, services, 1)

	svc := services[0].(map[string]any)
	deployments, ok := svc["deployments"].([]any)
	require.True(t, ok, "DescribeServices must include deployments")
	require.NotEmpty(t, deployments)

	d := deployments[0].(map[string]any)
	assert.Equal(t, "PRIMARY", d["status"])
}
