package ecs_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ----- Container instance tests -----

func TestECS_RegisterContainerInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler) map[string]any
		name     string
		wantID   string
		wantCode int
	}{
		{
			name: "register to default cluster",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{"ec2InstanceId": "i-12345678"}
			},
			wantCode: http.StatusOK,
			wantID:   "i-12345678",
		},
		{
			name: "register to explicit cluster",
			setup: func(h *ecs.Handler) map[string]any {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ec2-cluster"})

				return map[string]any{
					"cluster":       "ec2-cluster",
					"ec2InstanceId": "i-abcdef00",
				}
			},
			wantCode: http.StatusOK,
			wantID:   "i-abcdef00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			input := tt.setup(h)
			rec := doECSRequest(t, h, "RegisterContainerInstance", input)

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				ci := resp["containerInstance"].(map[string]any)
				assert.Equal(t, tt.wantID, ci["ec2InstanceId"])
				assert.NotEmpty(t, ci["containerInstanceArn"])
				assert.Equal(t, "ACTIVE", ci["status"])
				assert.Equal(t, true, ci["agentConnected"])
			}
		})
	}
}

func TestECS_ListContainerInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler)
		name     string
		cluster  string
		wantCode int
		wantLen  int
	}{
		{
			name: "empty list after no registrations",
			setup: func(h *ecs.Handler) {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "list-ci-empty"})
			},
			cluster:  "list-ci-empty",
			wantCode: http.StatusOK,
			wantLen:  0,
		},
		{
			name: "two instances registered",
			setup: func(h *ecs.Handler) {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "list-ci-two"})
				doECSRequest(
					t,
					h,
					"RegisterContainerInstance",
					map[string]any{"cluster": "list-ci-two", "ec2InstanceId": "i-1"},
				)
				doECSRequest(
					t,
					h,
					"RegisterContainerInstance",
					map[string]any{"cluster": "list-ci-two", "ec2InstanceId": "i-2"},
				)
			},
			cluster:  "list-ci-two",
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name:     "unknown cluster",
			setup:    func(_ *ecs.Handler) {},
			cluster:  "nonexistent-cluster",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doECSRequest(t, h, "ListContainerInstances", map[string]any{"cluster": tt.cluster})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				arns := resp["containerInstanceArns"].([]any)
				assert.Len(t, arns, tt.wantLen)
			}
		})
	}
}

func TestECS_DescribeContainerInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		wantLen  int
		byARN    bool
	}{
		{
			name:     "describe all instances",
			wantCode: http.StatusOK,
			wantLen:  1,
			byARN:    false,
		},
		{
			name:     "describe by ARN",
			wantCode: http.StatusOK,
			wantLen:  1,
			byARN:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "desc-ci-cluster"})

			rec := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
				"cluster":       "desc-ci-cluster",
				"ec2InstanceId": "i-describe",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var regResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))

			ciArn := regResp["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

			var filter []string
			if tt.byARN {
				filter = []string{ciArn}
			}

			rec2 := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
				"cluster":            "desc-ci-cluster",
				"containerInstances": filter,
			})
			require.Equal(t, tt.wantCode, rec2.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

			cis := resp["containerInstances"].([]any)
			assert.Len(t, cis, tt.wantLen)

			if tt.wantLen > 0 {
				assert.Equal(t, "i-describe", cis[0].(map[string]any)["ec2InstanceId"])
			}
		})
	}
}

func TestECS_DescribeContainerInstances_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "desc-ci-notfound"})

	rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster": "desc-ci-notfound",
		"containerInstances": []string{
			"arn:aws:ecs:us-east-1:000000000000:container-instance/desc-ci-notfound/nonexistent",
		},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ContainerInstanceNotFoundException")
}

func TestECS_DeregisterContainerInstance(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "deregister existing instance",
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dereg-cluster"})

			// Register.
			rec := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
				"cluster":       "dereg-cluster",
				"ec2InstanceId": "i-dereg",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var regResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))

			ciArn := regResp["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

			// Deregister.
			rec2 := doECSRequest(t, h, "DeregisterContainerInstance", map[string]any{
				"cluster":           "dereg-cluster",
				"containerInstance": ciArn,
			})

			require.Equal(t, tt.wantCode, rec2.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

				ci := resp["containerInstance"].(map[string]any)
				assert.Equal(t, "INACTIVE", ci["status"])
			}
		})
	}
}

func TestECS_DeregisterContainerInstance_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dereg-notfound-cluster"})

	rec := doECSRequest(t, h, "DeregisterContainerInstance", map[string]any{
		"cluster":           "dereg-notfound-cluster",
		"containerInstance": "arn:aws:ecs:us-east-1:000000000000:container-instance/x/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "ContainerInstanceNotFoundException")
}

func TestECS_DeregisterContainerInstance_WithoutForce_NoLinkedTasks(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "ci-running-td",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "ci-running-cluster"})
	require.NoError(t, err)

	ci, err := backend.RegisterContainerInstance("ci-running-cluster", "i-running")
	require.NoError(t, err)

	// RunTask doesn't set ContainerInstanceArn, so this task is not linked to the CI.
	_, err = backend.RunTask(ecs.RunTaskInput{
		Cluster:        "ci-running-cluster",
		TaskDefinition: td.TaskDefinitionArn,
		Count:          1,
	})
	require.NoError(t, err)

	// Deregister without force succeeds because no tasks are linked to this CI.
	_, err = backend.DeregisterContainerInstance("ci-running-cluster", ci.ContainerInstanceArn, false)
	require.NoError(t, err)
}

func TestECS_UpdateContainerInstancesState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		newStatus  string
		wantStatus string
		wantCode   int
	}{
		{
			name:       "drain instance",
			newStatus:  "DRAINING",
			wantCode:   http.StatusOK,
			wantStatus: "DRAINING",
		},
		{
			name:       "reactivate instance",
			newStatus:  "ACTIVE",
			wantCode:   http.StatusOK,
			wantStatus: "ACTIVE",
		},
		{
			name:      "invalid status rejected",
			newStatus: "INVALID",
			wantCode:  http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "update-ci-cluster"})

			rec := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
				"cluster":       "update-ci-cluster",
				"ec2InstanceId": "i-update",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var regResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))

			ciArn := regResp["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

			rec2 := doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
				"cluster":            "update-ci-cluster",
				"containerInstances": []string{ciArn},
				"status":             tt.newStatus,
			})

			require.Equal(t, tt.wantCode, rec2.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp))

				cis := resp["containerInstances"].([]any)
				require.Len(t, cis, 1)
				assert.Equal(t, tt.wantStatus, cis[0].(map[string]any)["status"])
			}
		})
	}
}

func TestECS_UpdateContainerInstancesState_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "update-ci-notfound"})

	rec := doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "update-ci-notfound",
		"containerInstances": []string{"arn:aws:ecs:us-east-1:000000000000:container-instance/x/nonexistent"},
		"status":             "DRAINING",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ----- ListServices tests -----

func TestECS_ListServices(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup    func(*ecs.Handler)
		name     string
		cluster  string
		wantCode int
		wantLen  int
	}{
		{
			name: "empty list",
			setup: func(h *ecs.Handler) {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "list-svc-empty"})
			},
			cluster:  "list-svc-empty",
			wantCode: http.StatusOK,
			wantLen:  0,
		},
		{
			name: "two services",
			setup: func(h *ecs.Handler) {
				tdArn := registerTestTaskDef(t, h, "list-svc-td")
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "list-svc-two"})
				doECSRequest(t, h, "CreateService", map[string]any{
					"cluster": "list-svc-two", "serviceName": "svc-a", "taskDefinition": tdArn, "desiredCount": 1,
				})
				doECSRequest(t, h, "CreateService", map[string]any{
					"cluster": "list-svc-two", "serviceName": "svc-b", "taskDefinition": tdArn, "desiredCount": 1,
				})
			},
			cluster:  "list-svc-two",
			wantCode: http.StatusOK,
			wantLen:  2,
		},
		{
			name:     "unknown cluster",
			setup:    func(_ *ecs.Handler) {},
			cluster:  "nonexistent",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)
			rec := doECSRequest(t, h, "ListServices", map[string]any{"cluster": tt.cluster})

			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				arns := resp["serviceArns"].([]any)
				assert.Len(t, arns, tt.wantLen)
			}
		})
	}
}

// ----- Task set tests -----

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
			name: "task set ARN not found",
			setup: func(h *ecs.Handler) (string, string, []string) {
				createTestServiceForTaskSet(t, h, "dts-notfound-cluster", "dts-notfound-svc")

				return "dts-notfound-cluster", "dts-notfound-svc",
					[]string{"arn:aws:ecs:us-east-1:000000000000:task-set/x/y/ecs-svc-nonexistent"}
			},
			wantCode: http.StatusBadRequest,
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
				// Confirm deletion.
				rec2 := doECSRequest(t, h, "DescribeTaskSets", map[string]any{
					"cluster":  input["cluster"],
					"service":  input["service"],
					"taskSets": []string{tsArn},
				})
				assert.Equal(t, http.StatusBadRequest, rec2.Code)
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
				createTestServiceForTaskSet(t, h, "primary-ts-notfound-cluster", "primary-ts-notfound-svc")

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

// ----- ExecuteCommand tests -----

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
				rec := doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn, "count": 1})
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
				rec := doECSRequest(t, h, "RunTask", map[string]any{"taskDefinition": tdArn, "count": 1})
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

// ----- ExtractResource tests for new fields -----

func TestECS_ExtractResource_NewFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body string
		name string
		want string
	}{
		{
			name: "containerInstance field",
			body: `{"containerInstance":"arn:aws:ecs:us-east-1:000000000000:container-instance/c/abc"}`,
			want: "arn:aws:ecs:us-east-1:000000000000:container-instance/c/abc",
		},
		{
			name: "taskSet field",
			body: `{"taskSet":"arn:aws:ecs:us-east-1:000000000000:task-set/c/s/ecs-svc-abc"}`,
			want: "arn:aws:ecs:us-east-1:000000000000:task-set/c/s/ecs-svc-abc",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			e := echo.New()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader([]byte(tt.body)))
			c := e.NewContext(req, httptest.NewRecorder())
			assert.Equal(t, tt.want, h.ExtractResource(c))
		})
	}
}

func TestECS_DeleteCluster_CleansUpTaskSets(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "cleanup-cluster"})
	require.NoError(t, err)

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "cleanup-td",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	svc, err := backend.CreateService(ecs.CreateServiceInput{
		Cluster:        "cleanup-cluster",
		ServiceName:    "cleanup-svc",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)

	_, err = backend.CreateTaskSet(ecs.CreateTaskSetInput{
		Cluster:        "cleanup-cluster",
		Service:        svc.ServiceArn,
		TaskDefinition: td.TaskDefinitionArn,
	})
	require.NoError(t, err)

	// Delete the cluster — should clean up the task set too.
	_, err = backend.DeleteCluster("cleanup-cluster")
	require.NoError(t, err)

	// Recreate the cluster and service with the same names — no stale task sets.
	_, err = backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "cleanup-cluster"})
	require.NoError(t, err)

	svc2, err := backend.CreateService(ecs.CreateServiceInput{
		Cluster:        "cleanup-cluster",
		ServiceName:    "cleanup-svc",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)

	sets, err := backend.DescribeTaskSets("cleanup-cluster", svc2.ServiceArn, nil)
	require.NoError(t, err)
	assert.Empty(t, sets, "no stale task sets after cluster delete+recreate")
}

func TestECS_DeleteService_CleansUpTaskSets(t *testing.T) {
	t.Parallel()

	backend := ecs.NewInMemoryBackend(testAccountID, testRegion, ecs.NewNoopRunner())

	_, err := backend.CreateCluster(ecs.CreateClusterInput{ClusterName: "svccleanup-cluster"})
	require.NoError(t, err)

	td, err := backend.RegisterTaskDefinition(ecs.RegisterTaskDefinitionInput{
		Family:               "svccleanup-td",
		ContainerDefinitions: []ecs.ContainerDefinition{{Name: "app", Image: "nginx"}},
	})
	require.NoError(t, err)

	svc, err := backend.CreateService(ecs.CreateServiceInput{
		Cluster:        "svccleanup-cluster",
		ServiceName:    "svccleanup-svc",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)

	_, err = backend.CreateTaskSet(ecs.CreateTaskSetInput{
		Cluster:        "svccleanup-cluster",
		Service:        svc.ServiceArn,
		TaskDefinition: td.TaskDefinitionArn,
	})
	require.NoError(t, err)

	// Delete the service — task sets should be cleaned up.
	_, err = backend.DeleteService("svccleanup-cluster", "svccleanup-svc")
	require.NoError(t, err)

	// Recreate the service with the same name — no stale task sets.
	svc2, err := backend.CreateService(ecs.CreateServiceInput{
		Cluster:        "svccleanup-cluster",
		ServiceName:    "svccleanup-svc",
		TaskDefinition: td.TaskDefinitionArn,
		DesiredCount:   1,
	})
	require.NoError(t, err)

	sets, err := backend.DescribeTaskSets("svccleanup-cluster", svc2.ServiceArn, nil)
	require.NoError(t, err)
	assert.Empty(t, sets, "no stale task sets after service delete+recreate")
}
