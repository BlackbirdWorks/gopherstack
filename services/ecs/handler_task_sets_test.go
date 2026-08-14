package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ----- shared task-set test helper -----

// createTestServiceForTaskSet creates a cluster+task-definition+service for task-set tests.
// It returns the task definition ARN for use in CreateTaskSet calls.
func createTestServiceForTaskSet(t *testing.T, h *ecs.Handler, cluster, serviceName string) string {
	t.Helper()

	if cluster != "" {
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": cluster})
	}

	tdArn := registerTestTaskDef(t, h, serviceName+"-td")

	body := map[string]any{
		"serviceName":    serviceName,
		"taskDefinition": tdArn,
		"desiredCount":   1,
	}
	if cluster != "" {
		body["cluster"] = cluster
	}

	rec := doECSRequest(t, h, "CreateService", body)
	require.Equal(t, http.StatusOK, rec.Code)

	return tdArn
}

func TestTaskSet_ExternalController_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})

	// Create service with EXTERNAL deployment controller (required for task sets)
	svcResp := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-cluster",
		"serviceName":          "ts-svc",
		"taskDefinition":       "ts-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})
	require.Equal(t, http.StatusOK, svcResp.Code)

	// Create a task set
	createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-cluster",
		"service":        "ts-svc",
		"taskDefinition": "ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 100},
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	taskSet := createOut["taskSet"].(map[string]any)
	taskSetArn := taskSet["taskSetArn"].(string)
	require.NotEmpty(t, taskSetArn)
	assert.Contains(t, taskSet["clusterArn"], "ts-cluster")
	assert.Equal(t, "ACTIVE", taskSet["status"])

	// Describe task sets
	describeResp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "ts-cluster",
		"service":  "ts-svc",
		"taskSets": []string{taskSetArn},
	})
	require.Equal(t, http.StatusOK, describeResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(describeResp.Body.Bytes(), &descOut))
	taskSets := descOut["taskSets"].([]any)
	require.Len(t, taskSets, 1)

	// Delete task set
	deleteResp := doECSRequest(t, h, "DeleteTaskSet", map[string]any{
		"cluster": "ts-cluster",
		"service": "ts-svc",
		"taskSet": taskSetArn,
	})
	require.Equal(t, http.StatusOK, deleteResp.Code)
}

func TestTaskSet_UpdateScale(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-scale-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-scale-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-scale-cluster",
		"serviceName":          "ts-scale-svc",
		"taskDefinition":       "ts-scale-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-scale-cluster",
		"service":        "ts-scale-svc",
		"taskDefinition": "ts-scale-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 50},
	})
	require.Equal(t, http.StatusOK, createResp.Code)
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	taskSetArn := createOut["taskSet"].(map[string]any)["taskSetArn"].(string)

	// Update scale to 100%
	updateResp := doECSRequest(t, h, "UpdateTaskSet", map[string]any{
		"cluster": "ts-scale-cluster",
		"service": "ts-scale-svc",
		"taskSet": taskSetArn,
		"scale":   map[string]any{"unit": "PERCENT", "value": 100},
	})
	require.Equal(t, http.StatusOK, updateResp.Code)
	var updateOut map[string]any
	require.NoError(t, json.Unmarshal(updateResp.Body.Bytes(), &updateOut))
	updatedScale := updateOut["taskSet"].(map[string]any)["scale"].(map[string]any)
	assert.InDelta(t, float64(100), updatedScale["value"], 0.001)
}

func TestTaskSet_UpdateServicePrimaryTaskSet_Switch(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "primary-ts-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "primary-ts-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "primary-ts-cluster",
		"serviceName":          "primary-ts-svc",
		"taskDefinition":       "primary-ts-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	// Create two task sets
	resp1 := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"taskDefinition": "primary-ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 50},
	})
	require.Equal(t, http.StatusOK, resp1.Code)
	var out1 map[string]any
	require.NoError(t, json.Unmarshal(resp1.Body.Bytes(), &out1))
	taskSet1Arn := out1["taskSet"].(map[string]any)["taskSetArn"].(string)

	resp2 := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"taskDefinition": "primary-ts-task",
		"scale":          map[string]any{"unit": "PERCENT", "value": 50},
	})
	require.Equal(t, http.StatusOK, resp2.Code)
	var out2 map[string]any
	require.NoError(t, json.Unmarshal(resp2.Body.Bytes(), &out2))
	taskSet2Arn := out2["taskSet"].(map[string]any)["taskSetArn"].(string)

	// Update primary task set to ts1
	primaryResp := doECSRequest(t, h, "UpdateServicePrimaryTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"primaryTaskSet": taskSet1Arn,
	})
	require.Equal(t, http.StatusOK, primaryResp.Code)

	// Verify ts1 is primary
	descResp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "primary-ts-cluster",
		"service":  "primary-ts-svc",
		"taskSets": []string{taskSet1Arn},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	ts := descOut["taskSets"].([]any)[0].(map[string]any)
	assert.Equal(t, "PRIMARY", ts["status"])

	// Switch primary to ts2
	primaryResp2 := doECSRequest(t, h, "UpdateServicePrimaryTaskSet", map[string]any{
		"cluster":        "primary-ts-cluster",
		"service":        "primary-ts-svc",
		"primaryTaskSet": taskSet2Arn,
	})
	require.Equal(t, http.StatusOK, primaryResp2.Code)

	// Now ts1 should revert to ACTIVE
	descResp2 := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "primary-ts-cluster",
		"service":  "primary-ts-svc",
		"taskSets": []string{taskSet1Arn},
	})
	require.Equal(t, http.StatusOK, descResp2.Code)
	var descOut2 map[string]any
	require.NoError(t, json.Unmarshal(descResp2.Body.Bytes(), &descOut2))
	ts1After := descOut2["taskSets"].([]any)[0].(map[string]any)
	assert.NotEqual(t, "PRIMARY", ts1After["status"])
}

func TestTaskSet_DescribeAll_NoFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ts-all-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "ts-all-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":              "ts-all-cluster",
		"serviceName":          "ts-all-svc",
		"taskDefinition":       "ts-all-task",
		"desiredCount":         0,
		"deploymentController": map[string]any{"type": "EXTERNAL"},
	})

	for range 3 {
		doECSRequest(t, h, "CreateTaskSet", map[string]any{
			"cluster":        "ts-all-cluster",
			"service":        "ts-all-svc",
			"taskDefinition": "ts-all-task",
			"scale":          map[string]any{"unit": "PERCENT", "value": 33},
		})
	}

	// Describe without filter returns all
	resp := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
		"cluster":  "ts-all-cluster",
		"service":  "ts-all-svc",
		"taskSets": []string{},
	})
	require.Equal(t, http.StatusOK, resp.Code)
	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	assert.Len(t, out["taskSets"].([]any), 3)
}

func TestECS_CreateTaskSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "create task set with defaults",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := createTestServiceForTaskSet(t, h, "ts-cluster", "ts-svc")

				return map[string]any{
					"cluster":        "ts-cluster",
					"service":        "ts-svc",
					"taskDefinition": tdArn,
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "create task set with scale",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := createTestServiceForTaskSet(t, h, "ts-scale-cluster", "ts-scale-svc")

				return map[string]any{
					"cluster":        "ts-scale-cluster",
					"service":        "ts-scale-svc",
					"taskDefinition": tdArn,
					"scale":          map[string]any{"unit": "PERCENT", "value": 50.0},
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "missing service",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := registerTestTaskDef(t, h, "ts-noservice-td")

				return map[string]any{
					"taskDefinition": tdArn,
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "missing task definition",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{
					"service": "some-service",
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
			rec := doECSRequest(t, h, "CreateTaskSet", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				ts := resp["taskSet"].(map[string]any)
				assert.NotEmpty(t, ts["taskSetArn"])
				assert.NotEmpty(t, ts["id"])
				assert.Equal(t, "ACTIVE", ts["status"])
				assert.NotEmpty(t, ts["createdAt"])
			}
		})
	}
}

func TestECS_DescribeTaskSets(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) (cluster, service string, taskSets []string)
		name     string
		wantCode int
		wantLen  int
	}{
		{
			name: "describe all task sets",
			setup: func(h *ecs.Handler) (string, string, []string) {
				tdArn := createTestServiceForTaskSet(t, h, "dts-all-cluster", "dts-all-svc")
				rec := doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "dts-all-cluster",
					"service":        "dts-all-svc",
					"taskDefinition": tdArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				return "dts-all-cluster", "dts-all-svc", nil
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "describe by ARN",
			setup: func(h *ecs.Handler) (string, string, []string) {
				tdArn := createTestServiceForTaskSet(t, h, "dts-arn-cluster", "dts-arn-svc")
				rec := doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "dts-arn-cluster",
					"service":        "dts-arn-svc",
					"taskDefinition": tdArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

				tsArn := createResp["taskSet"].(map[string]any)["taskSetArn"].(string)

				return "dts-arn-cluster", "dts-arn-svc", []string{tsArn}
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name: "task set ARN not found reports as failure, not an error",
			setup: func(h *ecs.Handler) (string, string, []string) {
				createTestServiceForTaskSet(t, h, "dts-notfound-cluster", "dts-notfound-svc")

				return "dts-notfound-cluster", "dts-notfound-svc",
					[]string{"arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ecs-svc-nonexistent"}
			},
			wantCode: http.StatusOK,
			wantLen:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			cluster, service, taskSets := tt.setup(h)

			body := map[string]any{"cluster": cluster, "service": service}
			if len(taskSets) > 0 {
				body["taskSets"] = taskSets
			}

			rec := doECSRequest(t, h, "DescribeTaskSets", body)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				sets := resp["taskSets"].([]any)
				assert.Len(t, sets, tt.wantLen)
			}
		})
	}
}

func TestECS_UpdateTaskSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantUnit string
		wantCode int
		wantVal  float64
	}{
		{
			name: "update scale to 25 percent",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := createTestServiceForTaskSet(t, h, "uts-cluster", "uts-svc")
				rec := doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "uts-cluster",
					"service":        "uts-svc",
					"taskDefinition": tdArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

				tsArn := createResp["taskSet"].(map[string]any)["taskSetArn"].(string)

				return map[string]any{
					"cluster": "uts-cluster",
					"service": "uts-svc",
					"taskSet": tsArn,
					"scale":   map[string]any{"unit": "PERCENT", "value": 25.0},
				}
			},
			wantCode: http.StatusOK,
			wantUnit: "PERCENT",
			wantVal:  25.0,
		},
		{
			name: "task set not found",
			setup: func(h *ecs.Handler) map[string]any {
				createTestServiceForTaskSet(t, h, "uts-notfound-cluster", "uts-notfound-svc")

				return map[string]any{
					"cluster": "uts-notfound-cluster",
					"service": "uts-notfound-svc",
					"taskSet": "arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ecs-svc-nonexistent",
					"scale":   map[string]any{"unit": "PERCENT", "value": 50.0},
				}
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name: "invalid scale unit rejected",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := createTestServiceForTaskSet(t, h, "uts-invalid-cluster", "uts-invalid-svc")
				rec := doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "uts-invalid-cluster",
					"service":        "uts-invalid-svc",
					"taskDefinition": tdArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

				tsArn := createResp["taskSet"].(map[string]any)["taskSetArn"].(string)

				return map[string]any{
					"cluster": "uts-invalid-cluster",
					"service": "uts-invalid-svc",
					"taskSet": tsArn,
					"scale":   map[string]any{"unit": "", "value": 50.0},
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
			rec := doECSRequest(t, h, "UpdateTaskSet", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				ts := resp["taskSet"].(map[string]any)
				scale := ts["scale"].(map[string]any)
				assert.Equal(t, tt.wantUnit, scale["unit"])
				assert.InDelta(t, tt.wantVal, scale["value"], 0.001)
			}
		})
	}
}

func TestECS_DeleteTaskSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantCode int
	}{
		{
			name: "delete existing task set",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := createTestServiceForTaskSet(t, h, "del-ts-cluster", "del-ts-svc")
				rec := doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "del-ts-cluster",
					"service":        "del-ts-svc",
					"taskDefinition": tdArn,
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

				tsArn := createResp["taskSet"].(map[string]any)["taskSetArn"].(string)

				return map[string]any{
					"cluster": "del-ts-cluster",
					"service": "del-ts-svc",
					"taskSet": tsArn,
				}
			},
			wantCode: http.StatusOK,
		},
		{
			name: "task set not found",
			setup: func(h *ecs.Handler) map[string]any {
				createTestServiceForTaskSet(t, h, "del-ts-notfound-cluster", "del-ts-notfound-svc")

				return map[string]any{
					"cluster": "del-ts-notfound-cluster",
					"service": "del-ts-notfound-svc",
					"taskSet": "arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ecs-svc-nonexistent",
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

			var tsArn string
			if input["taskSet"] != nil {
				tsArn = input["taskSet"].(string)
			}

			rec := doECSRequest(t, h, "DeleteTaskSet", input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				// Confirm deletion: the deleted task set now comes back as a
				// failure entry, not a whole-request error (DescribeTaskSets
				// reports missing IDs per-item, like its describe siblings).
				rec2 := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
					"cluster":  input["cluster"],
					"service":  input["service"],
					"taskSets": []string{tsArn},
				})
				require.Equal(t, http.StatusOK, rec2.Code)

				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

				sets, _ := resp["taskSets"].([]any)
				assert.Empty(t, sets)

				failures, _ := resp["failures"].([]any)
				require.Len(t, failures, 1)
				assert.Equal(t, tsArn, failures[0].(map[string]any)["arn"])
			}
		})
	}
}

func TestECS_UpdateServicePrimaryTaskSet(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(*ecs.Handler) map[string]any
		name        string
		wantCode    int
		wantPrimary bool
	}{
		{
			name: "set primary task set",
			setup: func(h *ecs.Handler) map[string]any {
				tdArn := createTestServiceForTaskSet(t, h, "primary-ts-cluster", "primary-ts-svc")
				rec1 := doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "primary-ts-cluster",
					"service":        "primary-ts-svc",
					"taskDefinition": tdArn,
				})
				require.Equal(t, http.StatusOK, rec1.Code)

				var ts1Resp map[string]any
				require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &ts1Resp))

				ts1Arn := ts1Resp["taskSet"].(map[string]any)["taskSetArn"].(string)

				// Create a second task set.
				doECSRequest(t, h, "CreateTaskSet", map[string]any{
					"cluster":        "primary-ts-cluster",
					"service":        "primary-ts-svc",
					"taskDefinition": tdArn,
				})

				return map[string]any{
					"cluster":        "primary-ts-cluster",
					"service":        "primary-ts-svc",
					"primaryTaskSet": ts1Arn,
					"_ts1Arn":        ts1Arn,
				}
			},
			wantCode:    http.StatusOK,
			wantPrimary: true,
		},
		{
			name: "task set not found",
			setup: func(h *ecs.Handler) map[string]any {
				createTestServiceForTaskSet(
					t,
					h,
					"primary-ts-notfound-cluster",
					"primary-ts-notfound-svc",
				)

				return map[string]any{
					"cluster":        "primary-ts-notfound-cluster",
					"service":        "primary-ts-notfound-svc",
					"primaryTaskSet": "arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ecs-svc-nonexistent",
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

			reqBody := map[string]any{
				"cluster":        input["cluster"],
				"service":        input["service"],
				"primaryTaskSet": input["primaryTaskSet"],
			}

			rec := doECSRequest(t, h, "UpdateServicePrimaryTaskSet", reqBody)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantPrimary {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				ts := resp["taskSet"].(map[string]any)
				assert.Equal(t, input["_ts1Arn"], ts["taskSetArn"])
				assert.Equal(t, "PRIMARY", ts["status"])
			}
		})
	}
}

func TestTaskSet_LoadBalancers_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskSet_ServiceRegistries_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

func TestTaskSet_DescribePreservesLBAndNC(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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

// TestDescribeTaskSets_TagsRequireInclude proves that DescribeTaskSets only
// returns tags when Include=["TAGS"] is specified, matching real AWS's
// DescribeTaskSetsInput.Include gating. Previously the wire shape had no
// tags field at all.
func TestDescribeTaskSets_TagsRequireInclude(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdArn := createTestServiceForTaskSet(t, h, "ts-tags-cluster", "ts-tags-svc")

	createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-tags-cluster",
		"service":        "ts-tags-svc",
		"taskDefinition": tdArn,
		"tags":           []map[string]any{{"key": "env", "value": "prod"}},
	})
	require.Equal(t, http.StatusOK, createResp.Code)

	var createOut map[string]any
	require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &createOut))
	taskSetArn := createOut["taskSet"].(map[string]any)["taskSetArn"].(string)

	// CreateTaskSet echoes tags unconditionally (no Include gating on Create).
	createTags := createOut["taskSet"].(map[string]any)["tags"].([]any)
	require.Len(t, createTags, 1)

	t.Run("without include", func(t *testing.T) {
		t.Parallel()

		rec := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
			"cluster":  "ts-tags-cluster",
			"service":  "ts-tags-svc",
			"taskSets": []string{taskSetArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ts := resp["taskSets"].([]any)[0].(map[string]any)
		assert.Nil(t, ts["tags"])
	})

	t.Run("with include TAGS", func(t *testing.T) {
		t.Parallel()

		rec := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
			"cluster":  "ts-tags-cluster",
			"service":  "ts-tags-svc",
			"taskSets": []string{taskSetArn},
			"include":  []string{"TAGS"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ts := resp["taskSets"].([]any)[0].(map[string]any)
		tags := ts["tags"].([]any)
		require.Len(t, tags, 1)
		assert.Equal(t, "env", tags[0].(map[string]any)["key"])
	})
}

// TestCreateTaskSet_CapacityProviderStrategy_Roundtrip proves that
// CreateTaskSet accepts and echoes a capacityProviderStrategy (previously
// entirely absent from CreateTaskSetInput and the TaskSet wire shape).
func TestCreateTaskSet_CapacityProviderStrategy_Roundtrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdArn := createTestServiceForTaskSet(t, h, "ts-cps-cluster", "ts-cps-svc")

	resp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
		"cluster":        "ts-cps-cluster",
		"service":        "ts-cps-svc",
		"taskDefinition": tdArn,
		"capacityProviderStrategy": []any{
			map[string]any{"capacityProvider": "FARGATE", "weight": 1},
			map[string]any{"capacityProvider": "FARGATE_SPOT", "weight": 3},
		},
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	strategy := out["taskSet"].(map[string]any)["capacityProviderStrategy"].([]any)
	require.Len(t, strategy, 2)
}

// TestDescribeTaskSets_FailureSemantics proves that a task set ID requested
// but not found is reported per-item in the "failures" wire field, matching
// every sibling batch-describe op (DescribeClusters, DescribeServices,
// DescribeTasks), rather than failing the whole request. Real
// DescribeTaskSetsOutput always carries a "failures" member; previously it
// was entirely absent from the wire shape and a single unknown ID aborted
// the call with a 400.
func TestDescribeTaskSets_FailureSemantics(t *testing.T) {
	t.Parallel()

	t.Run("unknown reported as failure not an error", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		createTestServiceForTaskSet(t, h, "dts-fail-cluster", "dts-fail-svc")

		rec := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
			"cluster":  "dts-fail-cluster",
			"service":  "dts-fail-svc",
			"taskSets": []string{"arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ghost"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		sets, _ := resp["taskSets"].([]any)
		assert.Empty(t, sets)

		failures, _ := resp["failures"].([]any)
		require.Len(t, failures, 1)

		f := failures[0].(map[string]any)
		assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ghost", f["arn"])
		assert.Equal(t, "MISSING", f["reason"])
	})

	t.Run("mix of found and missing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		tdArn := createTestServiceForTaskSet(t, h, "dts-fail-mix-cluster", "dts-fail-mix-svc")

		createResp := doECSRequest(t, h, "CreateTaskSet", map[string]any{
			"cluster":        "dts-fail-mix-cluster",
			"service":        "dts-fail-mix-svc",
			"taskDefinition": tdArn,
		})
		require.Equal(t, http.StatusOK, createResp.Code)

		var created map[string]any
		require.NoError(t, json.Unmarshal(createResp.Body.Bytes(), &created))
		realArn := created["taskSet"].(map[string]any)["taskSetArn"].(string)

		rec := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
			"cluster":  "dts-fail-mix-cluster",
			"service":  "dts-fail-mix-svc",
			"taskSets": []string{realArn, "arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ghost"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		sets, _ := resp["taskSets"].([]any)
		require.Len(t, sets, 1)
		assert.Equal(t, realArn, sets[0].(map[string]any)["taskSetArn"])

		failures, _ := resp["failures"].([]any)
		require.Len(t, failures, 1)
		assert.Equal(t, "arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ghost", failures[0].(map[string]any)["arn"])
	})
}
