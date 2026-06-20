package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// RegisterTaskDefinition parity tests ---------------------------------------

func TestHandler_RegisterTaskDefinition_RequiresCompatibilities(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":                  "myapp",
		"requiresCompatibilities": []string{"FARGATE"},
		"networkMode":             "awsvpc",
		"cpu":                     "256",
		"memory":                  "512",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "nginx"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	rcs := td["requiresCompatibilities"].([]any)
	require.Len(t, rcs, 1)
	assert.Equal(t, "FARGATE", rcs[0].(string))
}

func TestHandler_RegisterTaskDefinition_FargateValidation_BadCPU(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":                  "myapp",
		"requiresCompatibilities": []string{"FARGATE"},
		"networkMode":             "awsvpc",
		"cpu":                     "128",
		"memory":                  "512",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "nginx"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid Fargate CPU")
}

func TestHandler_RegisterTaskDefinition_FargateValidation_BadMemory(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":                  "myapp",
		"requiresCompatibilities": []string{"FARGATE"},
		"networkMode":             "awsvpc",
		"cpu":                     "256",
		"memory":                  "9999",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "nginx"},
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "invalid Fargate memory")
}

func TestHandler_RegisterTaskDefinition_TaskRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":           "myapp",
		"taskRoleArn":      "arn:aws:iam::000000000000:role/task-role",
		"executionRoleArn": "arn:aws:iam::000000000000:role/exec-role",
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": "nginx"},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	assert.Equal(t, "arn:aws:iam::000000000000:role/task-role", td["taskRoleArn"])
	assert.Equal(t, "arn:aws:iam::000000000000:role/exec-role", td["executionRoleArn"])
}

func TestHandler_RegisterTaskDefinition_Volumes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "myapp",
		"volumes": []map[string]any{
			{
				"name": "data",
				"host": map[string]any{"sourcePath": "/tmp/data"},
			},
			{
				"name": "efs-vol",
				"efsVolumeConfiguration": map[string]any{
					"fileSystemId":      "fs-12345678",
					"rootDirectory":     "/data",
					"transitEncryption": "ENABLED",
				},
			},
		},
		"containerDefinitions": []map[string]any{
			{
				"name":  "app",
				"image": "nginx",
				"mountPoints": []map[string]any{
					{"sourceVolume": "data", "containerPath": "/mnt/data"},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	vols := td["volumes"].([]any)
	require.Len(t, vols, 2)

	vol0 := vols[0].(map[string]any)
	assert.Equal(t, "data", vol0["name"])

	vol1 := vols[1].(map[string]any)
	efsCfg := vol1["efsVolumeConfiguration"].(map[string]any)
	assert.Equal(t, "fs-12345678", efsCfg["fileSystemId"])
}

func TestHandler_RegisterTaskDefinition_LogConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "myapp",
		"containerDefinitions": []map[string]any{
			{
				"name":  "app",
				"image": "nginx",
				"logConfiguration": map[string]any{
					"logDriver": "awslogs",
					"options": map[string]string{
						"awslogs-group": "/ecs/myapp",
					},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cd := td["containerDefinitions"].([]any)[0].(map[string]any)
	lc := cd["logConfiguration"].(map[string]any)
	assert.Equal(t, "awslogs", lc["logDriver"])
}

func TestHandler_RegisterTaskDefinition_Secrets(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "myapp",
		"containerDefinitions": []map[string]any{
			{
				"name":  "app",
				"image": "nginx",
				"secrets": []map[string]any{
					{
						"name":      "DB_PASSWORD",
						"valueFrom": "arn:aws:secretsmanager:us-east-1:000000000000:secret:db-pass",
					},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cd := td["containerDefinitions"].([]any)[0].(map[string]any)
	secrets := cd["secrets"].([]any)
	require.Len(t, secrets, 1)
	s := secrets[0].(map[string]any)
	assert.Equal(t, "DB_PASSWORD", s["name"])
}

func TestHandler_RegisterTaskDefinition_PortMapping_Extended(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "myapp",
		"containerDefinitions": []map[string]any{
			{
				"name":  "app",
				"image": "nginx",
				"portMappings": []map[string]any{
					{
						"containerPort":      8080,
						"protocol":           "tcp",
						"appProtocol":        "http",
						"containerPortRange": "8080-8090",
						"name":               "web",
					},
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cd := td["containerDefinitions"].([]any)[0].(map[string]any)
	pm := cd["portMappings"].([]any)[0].(map[string]any)
	assert.Equal(t, "http", pm["appProtocol"])
	assert.Equal(t, "8080-8090", pm["containerPortRange"])
	assert.Equal(t, "web", pm["name"])
}

func TestHandler_RegisterTaskDefinition_HealthCheck(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "myapp",
		"containerDefinitions": []map[string]any{
			{
				"name":  "app",
				"image": "nginx",
				"healthCheck": map[string]any{
					"command":     []string{"CMD", "curl", "-f", "http://localhost/health"},
					"interval":    30,
					"timeout":     5,
					"retries":     3,
					"startPeriod": 10,
				},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	cd := td["containerDefinitions"].([]any)[0].(map[string]any)
	hc := cd["healthCheck"].(map[string]any)
	assert.InDelta(t, float64(30), hc["interval"], 0.001)
	assert.InDelta(t, float64(5), hc["timeout"], 0.001)
}

// ListTaskDefinitions parity tests ------------------------------------------

func TestHandler_ListTaskDefinitions_StatusFilter_Active(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Register two revisions.
	rec1 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:1"}},
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	arn1 := out1["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	rec2 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:2"}},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	// Deregister revision 1.
	derecRec := doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": arn1,
	})
	require.Equal(t, http.StatusOK, derecRec.Code)

	// Default list (ACTIVE only).
	listRec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "myapp",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	arns := listOut["taskDefinitionArns"].([]any)
	require.Len(t, arns, 1)
	assert.Contains(t, arns[0].(string), "myapp:2")
}

func TestHandler_ListTaskDefinitions_StatusFilter_Inactive(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec1 := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:1"}},
	})
	require.Equal(t, http.StatusOK, rec1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &out1))
	arn1 := out1["taskDefinition"].(map[string]any)["taskDefinitionArn"].(string)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx:2"}},
	})

	// Deregister revision 1.
	_ = doECSRequest(t, h, "DeregisterTaskDefinition", map[string]any{
		"taskDefinition": arn1,
	})

	// List INACTIVE.
	listRec := doECSRequest(t, h, "ListTaskDefinitions", map[string]any{
		"familyPrefix": "myapp",
		"status":       "INACTIVE",
	})
	require.Equal(t, http.StatusOK, listRec.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listOut))
	arns := listOut["taskDefinitionArns"].([]any)
	require.Len(t, arns, 1)
	assert.Contains(t, arns[0].(string), "myapp:1")
}

// CreateService parity tests ------------------------------------------------

func TestHandler_CreateService_DeploymentConfigDefaults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	assert.InDelta(t, float64(100), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(200), dc["maximumPercent"], 0.001)
}

func TestHandler_CreateService_DeploymentConfigCustom(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"deploymentConfiguration": map[string]any{
			"minimumHealthyPercent": 50,
			"maximumPercent":        150,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	assert.InDelta(t, float64(50), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(150), dc["maximumPercent"], 0.001)
}

func TestHandler_CreateService_DeploymentController_CodeDeploy_Rejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":          "my-svc",
		"taskDefinition":       "myapp",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "CODE_DEPLOY"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "CODE_DEPLOY")
}

func TestHandler_CreateService_DeploymentController_Rolling_Accepted(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":          "my-svc",
		"taskDefinition":       "myapp",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "ROLLING"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentController"].(map[string]any)
	assert.Equal(t, "ROLLING", dc["type"])
}

func TestHandler_CreateService_LoadBalancers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"loadBalancers": []map[string]any{
			{
				"targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/my-tg/abcd1234",
				"containerName":  "app",
				"containerPort":  8080,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	lbs := svc["loadBalancers"].([]any)
	require.Len(t, lbs, 1)
	lb := lbs[0].(map[string]any)
	assert.InDelta(t, float64(8080), lb["containerPort"], 0.001)
	assert.Equal(t, "app", lb["containerName"])
}

func TestHandler_CreateService_ServiceRegistries(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"serviceRegistries": []map[string]any{
			{
				"registryArn":   "arn:aws:servicediscovery:us-east-1:000000000000:service/srv-abc123",
				"containerName": "app",
				"containerPort": 80,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	regs := svc["serviceRegistries"].([]any)
	require.Len(t, regs, 1)
	reg := regs[0].(map[string]any)
	assert.Equal(t, "app", reg["containerName"])
}

func TestHandler_CreateService_NetworkConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"networkConfiguration": map[string]any{
			"awsvpcConfiguration": map[string]any{
				"subnets":        []string{"subnet-abc123", "subnet-def456"},
				"securityGroups": []string{"sg-12345678"},
				"assignPublicIp": "ENABLED",
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	nc := svc["networkConfiguration"].(map[string]any)
	avpc := nc["awsvpcConfiguration"].(map[string]any)
	subnets := avpc["subnets"].([]any)
	assert.Len(t, subnets, 2)
	assert.Equal(t, "ENABLED", avpc["assignPublicIp"])
}

func TestHandler_CreateService_PropagateTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
		"propagateTags":  "SERVICE",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	assert.Equal(t, "SERVICE", svc["propagateTags"])
}

func TestHandler_CreateService_PropagateTags_DefaultNone(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	assert.Equal(t, "NONE", svc["propagateTags"])
}

// RunTask parity tests -------------------------------------------------------

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

func TestHandler_RunTask_PlatformVersion_BadVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition":  "myapp",
		"count":           1,
		"platformVersion": "9.9.9",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "platform version")
}

func TestHandler_RunTask_PlatformVersion_Latest(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition":  "myapp",
		"count":           1,
		"platformVersion": "LATEST",
	})
	assert.Equal(t, http.StatusOK, rec.Code)
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

// UpdateService parity tests ------------------------------------------------

func TestHandler_UpdateService_NetworkConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	_ = doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service": "my-svc",
		"networkConfiguration": map[string]any{
			"awsvpcConfiguration": map[string]any{
				"subnets": []string{"subnet-new123"},
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	nc := svc["networkConfiguration"].(map[string]any)
	avpc := nc["awsvpcConfiguration"].(map[string]any)
	subnets := avpc["subnets"].([]any)
	require.Len(t, subnets, 1)
	assert.Equal(t, "subnet-new123", subnets[0])
}

func TestHandler_UpdateService_LoadBalancers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	_ = doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service": "my-svc",
		"loadBalancers": []map[string]any{
			{
				"targetGroupArn": "arn:aws:elasticloadbalancing:us-east-1:000000000000:targetgroup/new-tg/xyz",
				"containerPort":  9090,
			},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	lbs := svc["loadBalancers"].([]any)
	require.Len(t, lbs, 1)
	lb := lbs[0].(map[string]any)
	assert.InDelta(t, float64(9090), lb["containerPort"], 0.001)
}

func TestHandler_UpdateService_DeploymentConfig_MinMaxPercent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "myapp",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})
	_ = doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "my-svc",
		"taskDefinition": "myapp",
		"desiredCount":   0,
	})

	rec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service": "my-svc",
		"deploymentConfiguration": map[string]any{
			"minimumHealthyPercent": 25,
			"maximumPercent":        300,
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	svc := out["service"].(map[string]any)
	dc := svc["deploymentConfiguration"].(map[string]any)
	assert.InDelta(t, float64(25), dc["minimumHealthyPercent"], 0.001)
	assert.InDelta(t, float64(300), dc["maximumPercent"], 0.001)
}

// DescribeTaskDefinition parity tests ----------------------------------------

func TestHandler_DescribeTaskDefinition_Volumes_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "myapp",
		"volumes": []map[string]any{
			{"name": "my-vol", "host": map[string]any{"sourcePath": "/tmp"}},
		},
		"containerDefinitions": []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "myapp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	vols := td["volumes"].([]any)
	require.Len(t, vols, 1)
	vol := vols[0].(map[string]any)
	assert.Equal(t, "my-vol", vol["name"])
}

func TestHandler_DescribeTaskDefinition_RequiresCompatibilities_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	_ = doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":                  "myapp",
		"requiresCompatibilities": []string{"FARGATE", "EC2"},
		"networkMode":             "awsvpc",
		"cpu":                     "256",
		"memory":                  "512",
		"containerDefinitions":    []map[string]any{{"name": "app", "image": "nginx"}},
	})

	rec := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{
		"taskDefinition": "myapp",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	td := out["taskDefinition"].(map[string]any)
	rcs := td["requiresCompatibilities"].([]any)
	assert.Len(t, rcs, 2)
}
