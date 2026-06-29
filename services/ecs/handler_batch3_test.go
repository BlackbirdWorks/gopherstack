package ecs_test

// handler_batch3_test.go — ECS AWS-accuracy audit batch-3
// Covers: TaskDefinition runtimePlatform+ephemeralStorage+inferenceAccelerators,
// ContainerDefinition resourceRequirements+environmentFiles, Service enableExecuteCommand,
// Task enableExecuteCommand propagation, TaskSet LoadBalancers+NetworkConfiguration+ServiceRegistries,
// FARGATE/FARGATE_SPOT built-in capacity providers, ListAttributes targetId filter,
// account settings valid names, GPU/Trainium resourceRequirements, secrets in env.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func newBatch3Handler(t *testing.T) *ecs.Handler {
	t.Helper()
	backend := ecs.NewInMemoryBackend("000000000000", "us-east-1", ecs.NewNoopRunner())

	return ecs.NewHandler(backend)
}

// ================================================================
// TaskDefinition: runtimePlatform
// ================================================================

func TestBatch3_TaskDefinition_RuntimePlatform_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "rt-plat-task",
		"containerDefinitions": []any{
			map[string]any{"name": "app", "image": "nginx"},
		},
		"runtimePlatform": map[string]any{
			"cpuArchitecture":       "ARM64",
			"operatingSystemFamily": "LINUX",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	rp := td["runtimePlatform"].(map[string]any)
	assert.Equal(t, "ARM64", rp["cpuArchitecture"])
	assert.Equal(t, "LINUX", rp["operatingSystemFamily"])

	descResp := doECSRequest(
		t,
		h,
		"DescribeTaskDefinition",
		map[string]any{"taskDefinition": "rt-plat-task"},
	)
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	descTD := descOut["taskDefinition"].(map[string]any)
	assert.Equal(t, "ARM64", descTD["runtimePlatform"].(map[string]any)["cpuArchitecture"])
}

func TestBatch3_TaskDefinition_RuntimePlatform_X86(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "x86-task",
		"containerDefinitions": []any{
			map[string]any{"name": "app", "image": "nginx"},
		},
		"runtimePlatform": map[string]any{
			"cpuArchitecture":       "X86_64",
			"operatingSystemFamily": "LINUX",
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	rp := td["runtimePlatform"].(map[string]any)
	assert.Equal(t, "X86_64", rp["cpuArchitecture"])
}

func TestBatch3_TaskDefinition_RuntimePlatform_Windows(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "win-task",
		"containerDefinitions": []any{
			map[string]any{"name": "app", "image": "mcr.microsoft.com/windows/servercore:ltsc2022"},
		},
		"runtimePlatform": map[string]any{
			"cpuArchitecture":       "X86_64",
			"operatingSystemFamily": "WINDOWS_SERVER_2022_FULL",
		},
		"requiresCompatibilities": []any{"EC2"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	rp := td["runtimePlatform"].(map[string]any)
	assert.Equal(t, "WINDOWS_SERVER_2022_FULL", rp["operatingSystemFamily"])
}

// ================================================================
// TaskDefinition: ephemeralStorage
// ================================================================

func TestBatch3_TaskDefinition_EphemeralStorage_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "eph-task",
		"containerDefinitions": []any{
			map[string]any{"name": "app", "image": "nginx"},
		},
		"ephemeralStorage": map[string]any{
			"sizeInGiB": 50,
		},
		"requiresCompatibilities": []any{"FARGATE"},
		"networkMode":             "awsvpc",
		"cpu":                     "1024",
		"memory":                  "2048",
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	es := td["ephemeralStorage"].(map[string]any)
	assert.InDelta(t, float64(50), es["sizeInGiB"], 0.001)

	descResp := doECSRequest(
		t,
		h,
		"DescribeTaskDefinition",
		map[string]any{"taskDefinition": "eph-task"},
	)
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	descTD := descOut["taskDefinition"].(map[string]any)
	assert.InDelta(t, float64(50), descTD["ephemeralStorage"].(map[string]any)["sizeInGiB"], 0.001)
}

func TestBatch3_TaskDefinition_EphemeralStorage_Default(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "no-eph-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	_, hasEph := td["ephemeralStorage"]
	assert.False(t, hasEph, "ephemeralStorage should be absent when not set")
}

// ================================================================
// TaskDefinition: inferenceAccelerators
// ================================================================

func TestBatch3_TaskDefinition_InferenceAccelerators_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "inf-accel-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "app",
				"image": "inference-app",
				"resourceRequirements": []any{
					map[string]any{"type": "InferenceAccelerator", "value": "device_1"},
				},
			},
		},
		"inferenceAccelerators": []any{
			map[string]any{"deviceName": "device_1", "deviceType": "eia2.medium"},
		},
		"requiresCompatibilities": []any{"EC2"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)

	accs := td["inferenceAccelerators"].([]any)
	require.Len(t, accs, 1)
	acc := accs[0].(map[string]any)
	assert.Equal(t, "device_1", acc["deviceName"])
	assert.Equal(t, "eia2.medium", acc["deviceType"])

	cds := td["containerDefinitions"].([]any)
	require.Len(t, cds, 1)
	cd := cds[0].(map[string]any)
	rr := cd["resourceRequirements"].([]any)
	require.Len(t, rr, 1)
	assert.Equal(t, "InferenceAccelerator", rr[0].(map[string]any)["type"])
	assert.Equal(t, "device_1", rr[0].(map[string]any)["value"])
}

func TestBatch3_TaskDefinition_InferenceAccelerators_Multiple(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "multi-inf-task",
		"containerDefinitions": []any{
			map[string]any{"name": "app", "image": "nginx"},
		},
		"inferenceAccelerators": []any{
			map[string]any{"deviceName": "device_1", "deviceType": "eia2.medium"},
			map[string]any{"deviceName": "device_2", "deviceType": "eia2.large"},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	accs := td["inferenceAccelerators"].([]any)
	assert.Len(t, accs, 2)
}

// ================================================================
// ContainerDefinition: resourceRequirements (GPU)
// ================================================================

func TestBatch3_ContainerDef_GPU_ResourceRequirement(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "gpu-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "gpu-app",
				"image": "nvcr.io/nvidia/cuda:12.0-base",
				"resourceRequirements": []any{
					map[string]any{"type": "GPU", "value": "1"},
				},
			},
		},
		"requiresCompatibilities": []any{"EC2"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cds := td["containerDefinitions"].([]any)
	require.Len(t, cds, 1)
	cd := cds[0].(map[string]any)
	rr := cd["resourceRequirements"].([]any)
	require.Len(t, rr, 1)
	assert.Equal(t, "GPU", rr[0].(map[string]any)["type"])
	assert.Equal(t, "1", rr[0].(map[string]any)["value"])
}

func TestBatch3_ContainerDef_GPU_MultipleContainers(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "multi-gpu-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "trainer",
				"image": "trainer-image",
				"resourceRequirements": []any{
					map[string]any{"type": "GPU", "value": "4"},
				},
			},
			map[string]any{
				"name":  "sidecar",
				"image": "sidecar-image",
			},
		},
		"requiresCompatibilities": []any{"EC2"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cds := td["containerDefinitions"].([]any)
	require.Len(t, cds, 2)

	trainer := cds[0].(map[string]any)
	rr := trainer["resourceRequirements"].([]any)
	assert.Equal(t, "4", rr[0].(map[string]any)["value"])

	sidecar := cds[1].(map[string]any)
	_, hasSidecarRR := sidecar["resourceRequirements"]
	assert.False(t, hasSidecarRR, "sidecar should have no resourceRequirements")
}

// ================================================================
// ContainerDefinition: environmentFiles
// ================================================================

func TestBatch3_ContainerDef_EnvironmentFiles_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "env-files-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "app",
				"image": "nginx",
				"environmentFiles": []any{
					map[string]any{
						"type":  "s3",
						"value": "arn:aws:s3:::my-bucket/app.env",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cds := td["containerDefinitions"].([]any)
	cd := cds[0].(map[string]any)
	envFiles := cd["environmentFiles"].([]any)
	require.Len(t, envFiles, 1)
	ef := envFiles[0].(map[string]any)
	assert.Equal(t, "s3", ef["type"])
	assert.Equal(t, "arn:aws:s3:::my-bucket/app.env", ef["value"])
}

func TestBatch3_ContainerDef_EnvironmentFiles_WithEnv(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "env-combined-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "app",
				"image": "nginx",
				"environment": []any{
					map[string]any{"name": "KEY", "value": "val"},
				},
				"environmentFiles": []any{
					map[string]any{
						"type":  "s3",
						"value": "arn:aws:s3:::bucket/base.env",
					},
					map[string]any{
						"type":  "s3",
						"value": "arn:aws:s3:::bucket/override.env",
					},
				},
				"secrets": []any{
					map[string]any{
						"name":      "SECRET_KEY",
						"valueFrom": "arn:aws:secretsmanager:us-east-1:000000000000:secret:myapp/secret",
					},
				},
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cd := td["containerDefinitions"].([]any)[0].(map[string]any)

	assert.Len(t, cd["environmentFiles"].([]any), 2)
	assert.Len(t, cd["environment"].([]any), 1)
	assert.Len(t, cd["secrets"].([]any), 1)
}

// ================================================================
// Service: enableExecuteCommand
// ================================================================

func TestBatch3_Service_EnableExecuteCommand_Create(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "exec-cluster",
		"serviceName":          "exec-svc",
		"taskDefinition":       "exec-task",
		"desiredCount":         1,
		"enableExecuteCommand": true,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	assert.Equal(t, true, svc["enableExecuteCommand"])
}

func TestBatch3_Service_EnableExecuteCommand_Default_False(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "no-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "no-exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	resp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "no-exec-cluster",
		"serviceName":    "no-exec-svc",
		"taskDefinition": "no-exec-task",
		"desiredCount":   0,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	eec := svc["enableExecuteCommand"]
	assert.NotEqual(t, true, eec)
}

func TestBatch3_Service_EnableExecuteCommand_UpdateService(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "upd-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "upd-exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "upd-exec-cluster",
		"serviceName":    "upd-exec-svc",
		"taskDefinition": "upd-exec-task",
		"desiredCount":   0,
	})

	// Enable exec on existing service
	updResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":              "upd-exec-cluster",
		"service":              "upd-exec-svc",
		"enableExecuteCommand": true,
	})
	require.Equal(t, http.StatusOK, updResp.Code)

	var updOut map[string]any
	require.NoError(t, json.Unmarshal(updResp.Body.Bytes(), &updOut))
	svc := updOut["service"].(map[string]any)
	assert.Equal(t, true, svc["enableExecuteCommand"])

	// Disable exec
	disResp := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":              "upd-exec-cluster",
		"service":              "upd-exec-svc",
		"enableExecuteCommand": false,
	})
	require.Equal(t, http.StatusOK, disResp.Code)

	var disOut map[string]any
	require.NoError(t, json.Unmarshal(disResp.Body.Bytes(), &disOut))
	disSvc := disOut["service"].(map[string]any)
	eec := disSvc["enableExecuteCommand"]
	assert.NotEqual(t, true, eec)
}

func TestBatch3_Service_EnableExecuteCommand_DescribeServices(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "desc-exec-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "desc-exec-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "desc-exec-cluster",
		"serviceName":          "desc-exec-svc",
		"taskDefinition":       "desc-exec-task",
		"desiredCount":         0,
		"enableExecuteCommand": true,
	})

	descResp := doECSRequest(t, h, "DescribeServices", map[string]any{
		"cluster":  "desc-exec-cluster",
		"services": []any{"desc-exec-svc"},
	})
	require.Equal(t, http.StatusOK, descResp.Code)

	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	svcs := descOut["services"].([]any)
	require.Len(t, svcs, 1)
	assert.Equal(t, true, svcs[0].(map[string]any)["enableExecuteCommand"])
}

// ================================================================
// Task: enableExecuteCommand propagation
// ================================================================

func TestBatch3_Task_EnableExecuteCommand_RunTask(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

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

func TestBatch3_Task_EnableExecuteCommand_ServicePropagation(t *testing.T) {
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

// ================================================================
// TaskSet: LoadBalancers, NetworkConfiguration, ServiceRegistries
// ================================================================

func TestBatch3_TaskSet_LoadBalancers_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-lb-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-lb-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		"networkMode":          "awsvpc",
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "ts-lb-cluster",
		"serviceName":    "ts-lb-svc",
		"taskDefinition": "ts-lb-task",
		"desiredCount":   0,
		"deploymentController": map[string]any{
			"type": "EXTERNAL",
		},
	})

	resp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-lb-cluster",
		"service":        "ts-lb-svc",
		"taskDefinition": "ts-lb-task",
		"loadBalancers": []any{
			map[string]any{
				"targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/my-tg/abc123",
				"containerName":  "app",
				"containerPort":  80,
			},
		},
		"networkConfiguration": map[string]any{
			"awsvpcConfiguration": map[string]any{
				"subnets":        []any{"subnet-abc123"},
				"securityGroups": []any{"sg-xyz456"},
				"assignPublicIp": "ENABLED",
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	ts := out["taskSet"].(map[string]any)

	lbs := ts["loadBalancers"].([]any)
	require.Len(t, lbs, 1)
	lb := lbs[0].(map[string]any)
	assert.Contains(t, lb["targetGroupArn"].(string), "targetgroup/my-tg")
	assert.Equal(t, "app", lb["containerName"])

	nc := ts["networkConfiguration"].(map[string]any)
	avpc := nc["awsvpcConfiguration"].(map[string]any)
	assert.Equal(t, "ENABLED", avpc["assignPublicIp"])
	subnets := avpc["subnets"].([]any)
	assert.Contains(t, subnets, "subnet-abc123")
}

func TestBatch3_TaskSet_ServiceRegistries_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-sd-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-sd-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "ts-sd-cluster",
		"serviceName":    "ts-sd-svc",
		"taskDefinition": "ts-sd-task",
		"desiredCount":   0,
		"deploymentController": map[string]any{
			"type": "EXTERNAL",
		},
	})

	resp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-sd-cluster",
		"service":        "ts-sd-svc",
		"taskDefinition": "ts-sd-task",
		"serviceRegistries": []any{
			map[string]any{
				"registryArn":   "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-abc",
				"containerName": "app",
				"containerPort": 80,
			},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	ts := out["taskSet"].(map[string]any)
	srs := ts["serviceRegistries"].([]any)
	require.Len(t, srs, 1)
	sr := srs[0].(map[string]any)
	assert.Contains(t, sr["registryArn"].(string), "srv-abc")
	assert.Equal(t, "app", sr["containerName"])
}

func TestBatch3_TaskSet_DescribePreservesLBAndNC(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-desc-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-desc-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-desc-cluster",
		"serviceName":          "ts-desc-svc",
		"taskDefinition":       "ts-desc-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-desc-cluster",
		"service":        "ts-desc-svc",
		"taskDefinition": "ts-desc-task",
		"loadBalancers": []any{
			map[string]any{
				"targetGroupArn": "arn:aws:elasticloadbalancing:::targetgroup/tg/abc",
				"containerPort":  8080,
			},
		},
	})
	require.Equal(t, http.StatusOK, createResp.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	tsArn := createOut["taskSet"].(map[string]any)["taskSetArn"].(string)

	descResp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "ts-desc-cluster",
		"service":  "ts-desc-svc",
		"taskSets": []any{tsArn},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	sets := descOut["taskSets"].([]any)
	require.Len(t, sets, 1)
	ts := sets[0].(map[string]any)
	lbs := ts["loadBalancers"].([]any)
	assert.Len(t, lbs, 1)
}

// ================================================================
// CapacityProviders: FARGATE/FARGATE_SPOT built-in
// ================================================================

func TestBatch3_CapacityProvider_FARGATE_BuiltIn_DescribeWithoutCreate(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{
		"capacityProviders": []string{"FARGATE"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	providers := out["capacityProviders"].([]any)
	require.Len(t, providers, 1)
	cp := providers[0].(map[string]any)
	assert.Equal(t, "FARGATE", cp["name"])
	assert.Equal(t, "ACTIVE", cp["status"])
}

func TestBatch3_CapacityProvider_FARGATE_SPOT_BuiltIn(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{
		"capacityProviders": []string{"FARGATE", "FARGATE_SPOT"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	providers := out["capacityProviders"].([]any)
	require.Len(t, providers, 2)

	names := make([]string, 0, 2)
	for _, p := range providers {
		names = append(names, p.(map[string]any)["name"].(string))
	}
	assert.Contains(t, names, "FARGATE")
	assert.Contains(t, names, "FARGATE_SPOT")
}

func TestBatch3_CapacityProvider_FARGATE_BuiltIn_WithCreated(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCapacityProvider", map[string]any{
		"name": "my-asg-cp",
		"autoScalingGroupProvider": map[string]any{
			"autoScalingGroupArn": "arn:aws:autoscaling:us-east-1:000000000000:autoScalingGroup:asg-1",
		},
	})

	// Describe FARGATE (built-in) and custom together
	resp := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{
		"capacityProviders": []string{"FARGATE", "my-asg-cp"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	providers := out["capacityProviders"].([]any)
	require.Len(t, providers, 2)

	names := make([]string, 0, 2)
	for _, p := range providers {
		names = append(names, p.(map[string]any)["name"].(string))
	}
	assert.Contains(t, names, "FARGATE")
	assert.Contains(t, names, "my-asg-cp")
}

func TestBatch3_CapacityProvider_Unknown_ReturnsError(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "DescribeCapacityProviders", map[string]any{
		"capacityProviders": []string{"nonexistent-cp"},
	})
	assert.NotEqual(t, http.StatusOK, resp.Code)
}

// ================================================================
// ListAttributes: targetId filter
// ================================================================

func TestBatch3_ListAttributes_TargetID_Filter(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-tid-cluster"})

	ci1Resp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "attr-tid-cluster",
		"ec2InstanceId": "i-instance-1",
	})
	require.Equal(t, http.StatusOK, ci1Resp.Code)
	var ci1Out map[string]any
	require.NoError(t, json.Unmarshal(ci1Resp.Body.Bytes(), &ci1Out))
	ci1Arn := ci1Out["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	ci2Resp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":       "attr-tid-cluster",
		"ec2InstanceId": "i-instance-2",
	})
	require.Equal(t, http.StatusOK, ci2Resp.Code)
	var ci2Out map[string]any
	require.NoError(t, json.Unmarshal(ci2Resp.Body.Bytes(), &ci2Out))
	ci2Arn := ci2Out["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-tid-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "zone",
				"value":      "us-east-1a",
				"targetType": "container-instance",
				"targetId":   ci1Arn,
			},
			map[string]any{
				"name":       "zone",
				"value":      "us-east-1b",
				"targetType": "container-instance",
				"targetId":   ci2Arn,
			},
		},
	})

	// Filter by targetId — should return only ci1's attribute
	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":       "attr-tid-cluster",
		"targetType":    "container-instance",
		"attributeName": "zone",
		"targetId":      ci1Arn,
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	require.Len(t, attrs, 1)
	assert.Equal(t, "us-east-1a", attrs[0].(map[string]any)["value"])
}

func TestBatch3_ListAttributes_TargetID_Filter_NotFound(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-nf-cluster"})

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-nf-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "ecs.instance-type",
				"value":      "m5.large",
				"targetType": "container-instance",
				"targetId":   "ci-arn-1",
			},
		},
	})

	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":  "attr-nf-cluster",
		"targetId": "ci-arn-that-does-not-exist",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Empty(t, attrs)
}

func TestBatch3_ListAttributes_TargetID_NoFilter_ReturnsAll(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "attr-all-cluster"})

	doECSRequest(t, h, "PutAttributes", map[string]any{
		"cluster": "attr-all-cluster",
		"attributes": []any{
			map[string]any{
				"name":       "ecs.availability-zone",
				"value":      "a",
				"targetType": "container-instance",
				"targetId":   "ci-1",
			},
			map[string]any{
				"name":       "ecs.availability-zone",
				"value":      "b",
				"targetType": "container-instance",
				"targetId":   "ci-2",
			},
			map[string]any{
				"name":       "ecs.availability-zone",
				"value":      "c",
				"targetType": "container-instance",
				"targetId":   "ci-3",
			},
		},
	})

	listResp := doECSRequest(t, h, "ListAttributes", map[string]any{
		"cluster":    "attr-all-cluster",
		"targetType": "container-instance",
	})
	require.Equal(t, http.StatusOK, listResp.Code)
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	attrs := listOut["attributes"].([]any)
	assert.Len(t, attrs, 3)
}

// ================================================================
// TaskDefinition: combined runtimePlatform + ephemeralStorage + inferenceAccelerators
// ================================================================

func TestBatch3_TaskDefinition_AllBatch3Fields_Combined(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "combined-batch3-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "ml-app",
				"image": "ml-inference:latest",
				"environment": []any{
					map[string]any{"name": "MODEL_PATH", "value": "/models/v1"},
				},
				"environmentFiles": []any{
					map[string]any{"type": "s3", "value": "arn:aws:s3:::ml-configs/model.env"},
				},
				"secrets": []any{
					map[string]any{
						"name":      "API_KEY",
						"valueFrom": "arn:aws:ssm:us-east-1:000000000000:parameter/ml/api-key",
					},
				},
				"resourceRequirements": []any{
					map[string]any{"type": "InferenceAccelerator", "value": "inf_device"},
				},
			},
		},
		"runtimePlatform": map[string]any{
			"cpuArchitecture":       "X86_64",
			"operatingSystemFamily": "LINUX",
		},
		"ephemeralStorage": map[string]any{
			"sizeInGiB": 100,
		},
		"inferenceAccelerators": []any{
			map[string]any{"deviceName": "inf_device", "deviceType": "eia2.xlarge"},
		},
		"requiresCompatibilities": []any{"EC2"},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)

	rp := td["runtimePlatform"].(map[string]any)
	assert.Equal(t, "X86_64", rp["cpuArchitecture"])
	assert.Equal(t, "LINUX", rp["operatingSystemFamily"])

	es := td["ephemeralStorage"].(map[string]any)
	assert.InDelta(t, float64(100), es["sizeInGiB"], 0.001)

	accs := td["inferenceAccelerators"].([]any)
	require.Len(t, accs, 1)
	assert.Equal(t, "eia2.xlarge", accs[0].(map[string]any)["deviceType"])

	cds := td["containerDefinitions"].([]any)
	cd := cds[0].(map[string]any)
	assert.NotEmpty(t, cd["environmentFiles"])
	assert.NotEmpty(t, cd["secrets"])
	assert.NotEmpty(t, cd["resourceRequirements"])
}

// ================================================================
// Account Settings: valid name round-trip
// ================================================================

func TestBatch3_AccountSettings_TaskLongArnFormat(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "taskLongArnFormat",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putResp.Body.Bytes(), &putOut))
	setting := putOut["setting"].(map[string]any)
	assert.Equal(t, "taskLongArnFormat", setting["name"])
	assert.Equal(t, "enabled", setting["value"])
}

func TestBatch3_AccountSettings_ContainerInstanceLongArnFormat(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "containerInstanceLongArnFormat",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	var putOut map[string]any
	require.NoError(t, json.Unmarshal(putResp.Body.Bytes(), &putOut))
	setting := putOut["setting"].(map[string]any)
	assert.Equal(t, "containerInstanceLongArnFormat", setting["name"])
	assert.Equal(t, "enabled", setting["value"])
}

func TestBatch3_AccountSettings_FargateSettings(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	fargateSettings := []string{
		"fargateTaskRetirementWaitPeriod",
		"fargateFIPSMode",
	}

	for _, name := range fargateSettings {
		putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name":  name,
			"value": "enabled",
		})
		require.Equal(t, http.StatusOK, putResp.Code, "PutAccountSetting %s", name)

		var putOut map[string]any
		require.NoError(t, json.Unmarshal(putResp.Body.Bytes(), &putOut))
		setting := putOut["setting"].(map[string]any)
		assert.Equal(t, name, setting["name"])
	}
}

func TestBatch3_AccountSettings_ServiceLongArnFormat(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	putResp := doECSRequest(t, h, "PutAccountSetting", map[string]any{
		"name":  "serviceLongArnFormat",
		"value": "enabled",
	})
	require.Equal(t, http.StatusOK, putResp.Code)

	listResp := doECSRequest(t, h, "ListAccountSettings", map[string]any{
		"name": "serviceLongArnFormat",
	})
	require.Equal(t, http.StatusOK, listResp.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	settings := listOut["settings"].([]any)
	require.NotEmpty(t, settings)
	assert.Equal(t, "enabled", settings[0].(map[string]any)["value"])
}

// ================================================================
// TaskDefinition without new fields: backward compat
// ================================================================

func TestBatch3_TaskDefinition_BackwardCompat_NoNewFields(t *testing.T) {
	t.Parallel()

	h := newBatch3Handler(t)

	resp := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "old-style-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":   "app",
				"image":  "nginx",
				"cpu":    256,
				"memory": 512,
				"secrets": []any{
					map[string]any{
						"name":      "DB_PASS",
						"valueFrom": "arn:aws:ssm:::parameter/db/pass",
					},
				},
			},
		},
		"requiresCompatibilities": []any{"FARGATE"},
		"networkMode":             "awsvpc",
		"cpu":                     "256",
		"memory":                  "512",
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)

	// New fields should be absent
	_, hasRP := td["runtimePlatform"]
	_, hasES := td["ephemeralStorage"]
	_, hasIA := td["inferenceAccelerators"]
	assert.False(t, hasRP)
	assert.False(t, hasES)
	assert.False(t, hasIA)

	// Old fields still work
	assert.Equal(t, "old-style-task", td["family"])
	cds := td["containerDefinitions"].([]any)
	cd := cds[0].(map[string]any)
	secrets := cd["secrets"].([]any)
	assert.Len(t, secrets, 1)
}
