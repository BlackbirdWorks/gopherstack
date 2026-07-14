package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskDefinition_RuntimePlatform_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_RuntimePlatform_X86(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_RuntimePlatform_Windows(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_EphemeralStorage_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_EphemeralStorage_Default(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_InferenceAccelerators_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_InferenceAccelerators_Multiple(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestContainerDef_GPU_ResourceRequirement(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestContainerDef_GPU_MultipleContainers(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestContainerDef_EnvironmentFiles_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestContainerDef_EnvironmentFiles_WithEnv(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_AllAdvancedFields_Combined(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_BackwardCompat_NoNewFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskDefinition_Volumes_EmptyNotNull(t *testing.T) {
	t.Parallel()

	t.Run("register response", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "vol-task",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
			// no volumes field
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		td := resp["taskDefinition"].(map[string]any)

		// AWS always returns volumes as an array, never absent.
		raw, exists := td["volumes"]
		assert.True(t, exists, "volumes must be present in RegisterTaskDefinition response")
		assert.NotNil(t, raw, "volumes must not be null")

		vols, ok := raw.([]any)
		assert.True(t, ok, "volumes must be an array")
		assert.Empty(t, vols, "must be [] when no volumes defined")
	})

	t.Run("describe response", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
			"family":               "dtd-task",
			"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
		})

		rec := doECSRequest(t, h, "DescribeTaskDefinition", map[string]any{
			"taskDefinition": "dtd-task",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		td := resp["taskDefinition"].(map[string]any)

		raw, exists := td["volumes"]
		assert.True(t, exists, "volumes present in DescribeTaskDefinition response")
		assert.NotNil(t, raw, "volumes not null")

		vols, ok := raw.([]any)
		assert.True(t, ok, "volumes is array")
		assert.Empty(t, vols)
	})
}

func TestTaskDefinition_Volumes_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "vol-rt-task",
		"containerDefinitions": []any{
			map[string]any{
				"name":  "app",
				"image": "nginx",
				"mountPoints": []any{
					map[string]any{"sourceVolume": "data", "containerPath": "/data"},
				},
			},
		},
		"volumes": []any{
			map[string]any{"name": "data"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	td := resp["taskDefinition"].(map[string]any)
	vols, _ := td["volumes"].([]any)
	require.Len(t, vols, 1)

	vol := vols[0].(map[string]any)
	assert.Equal(t, "data", vol["name"])
}

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

// TestHandler_RegisterTaskDefinition_FargateValidation rejects task definitions
// whose CPU or memory values are invalid for the Fargate launch type.
func TestHandler_RegisterTaskDefinition_FargateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		cpu     string
		memory  string
		wantErr string
	}{
		{
			name:    "invalid Fargate CPU",
			cpu:     "128",
			memory:  "512",
			wantErr: "invalid Fargate CPU",
		},
		{
			name:    "invalid Fargate memory",
			cpu:     "256",
			memory:  "9999",
			wantErr: "invalid Fargate memory",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
				"family":                  "myapp",
				"requiresCompatibilities": []string{"FARGATE"},
				"networkMode":             "awsvpc",
				"cpu":                     tt.cpu,
				"memory":                  tt.memory,
				"containerDefinitions": []map[string]any{
					{"name": "app", "image": "nginx"},
				},
			})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
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
