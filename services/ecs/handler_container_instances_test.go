package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

func TestContainerInstance_DRAINING_State(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "draining-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "draining-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-drain-1234"),
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)
	assert.Equal(t, "ACTIVE", ciOut["containerInstance"].(map[string]any)["status"])

	// Transition to DRAINING
	drainResp := doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "draining-cluster",
		"containerInstances": []string{ciArn},
		"status":             "DRAINING",
	})
	require.Equal(t, http.StatusOK, drainResp.Code)

	var drainOut map[string]any
	require.NoError(t, json.Unmarshal(drainResp.Body.Bytes(), &drainOut))
	instances := drainOut["containerInstances"].([]any)
	require.Len(t, instances, 1)
	assert.Equal(t, "DRAINING", instances[0].(map[string]any)["status"])

	// Describe and verify DRAINING state persisted
	descResp := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
		"cluster":            "draining-cluster",
		"containerInstances": []string{ciArn},
	})
	require.Equal(t, http.StatusOK, descResp.Code)
	var descOut map[string]any
	require.NoError(t, json.Unmarshal(descResp.Body.Bytes(), &descOut))
	described := descOut["containerInstances"].([]any)
	require.Len(t, described, 1)
	assert.Equal(t, "DRAINING", described[0].(map[string]any)["status"])
}

func TestContainerInstance_DRAINING_to_ACTIVE(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "reactivate-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "reactivate-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-react-5678"),
	})
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	// Drain then re-activate
	doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "reactivate-cluster",
		"containerInstances": []string{ciArn},
		"status":             "DRAINING",
	})
	activeResp := doECSRequest(t, h, "UpdateContainerInstancesState", map[string]any{
		"cluster":            "reactivate-cluster",
		"containerInstances": []string{ciArn},
		"status":             "ACTIVE",
	})
	require.Equal(t, http.StatusOK, activeResp.Code)

	var activeOut map[string]any
	require.NoError(t, json.Unmarshal(activeResp.Body.Bytes(), &activeOut))
	instances := activeOut["containerInstances"].([]any)
	assert.Equal(t, "ACTIVE", instances[0].(map[string]any)["status"])
}

func TestContainerInstance_UpdateAgent(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "update-agent-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "update-agent-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-agent-9999"),
	})
	require.Equal(t, http.StatusOK, ciResp.Code)
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	resp := doECSRequest(t, h, "UpdateContainerAgent", map[string]any{
		"cluster":           "update-agent-cluster",
		"containerInstance": ciArn,
	})
	require.Equal(t, http.StatusOK, resp.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(resp.Body.Bytes(), &out))
	ci := out["containerInstance"].(map[string]any)
	assert.NotEmpty(t, ci["containerInstanceArn"])
}

func TestContainerInstance_Deregister_Force(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "deregforce-cluster"})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "deregforce-task",
		"containerDefinitions": []any{map[string]any{"name": "app", "image": "nginx"}},
	})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "deregforce-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-force-1234"),
	})
	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	// Start a task on this container instance
	doECSRequest(t, h, "StartTask", map[string]any{
		"cluster":            "deregforce-cluster",
		"taskDefinition":     "deregforce-task",
		"containerInstances": []string{ciArn},
	})

	// Force deregister even with running tasks
	deregResp := doECSRequest(t, h, "DeregisterContainerInstance", map[string]any{
		"cluster":           "deregforce-cluster",
		"containerInstance": ciArn,
		"force":             true,
	})
	require.Equal(t, http.StatusOK, deregResp.Code)

	// Verify it's gone
	listResp := doECSRequest(t, h, "ListContainerInstances", map[string]any{
		"cluster": "deregforce-cluster",
	})
	var listOut map[string]any
	require.NoError(t, json.Unmarshal(listResp.Body.Bytes(), &listOut))
	arns := listOut["containerInstanceArns"].([]any)
	assert.Empty(t, arns)
}

// TestECS_RegisterContainerInstance proves that the EC2 instance ID
// surfaced on the resulting ContainerInstance (ec2InstanceId, a real
// response field) is derived from the request's instanceIdentityDocument --
// the real RegisterContainerInstanceRequest has no ec2InstanceId field of
// its own, so no real typed client could ever send one directly.
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
				return map[string]any{
					"instanceIdentityDocument": fakeInstanceIdentityDocument("i-12345678"),
				}
			},
			wantCode: http.StatusOK,
			wantID:   "i-12345678",
		},
		{
			name: "register to explicit cluster",
			setup: func(h *ecs.Handler) map[string]any {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ec2-cluster"})

				return map[string]any{
					"cluster":                  "ec2-cluster",
					"instanceIdentityDocument": fakeInstanceIdentityDocument("i-abcdef00"),
				}
			},
			wantCode: http.StatusOK,
			wantID:   "i-abcdef00",
		},
		{
			// A real typed SDK client cannot send ec2InstanceId (it isn't a
			// field on RegisterContainerInstanceRequest); proves it is
			// ignored rather than accidentally still wired up.
			name: "ec2InstanceId in raw body is ignored",
			setup: func(_ *ecs.Handler) map[string]any {
				return map[string]any{"ec2InstanceId": "i-should-be-ignored"}
			},
			wantCode: http.StatusOK,
			wantID:   "",
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
					map[string]any{
						"cluster":                  "list-ci-two",
						"instanceIdentityDocument": fakeInstanceIdentityDocument("i-1"),
					},
				)
				doECSRequest(
					t,
					h,
					"RegisterContainerInstance",
					map[string]any{
						"cluster":                  "list-ci-two",
						"instanceIdentityDocument": fakeInstanceIdentityDocument("i-2"),
					},
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
			rec := doECSRequest(
				t,
				h,
				"ListContainerInstances",
				map[string]any{"cluster": tt.cluster},
			)

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
				"cluster":                  "desc-ci-cluster",
				"instanceIdentityDocument": fakeInstanceIdentityDocument("i-describe"),
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
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	instances, _ := resp["containerInstances"].([]any)
	assert.Empty(t, instances)

	failures, _ := resp["failures"].([]any)
	assert.Len(t, failures, 1)
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
				"cluster":                  "dereg-cluster",
				"instanceIdentityDocument": fakeInstanceIdentityDocument("i-dereg"),
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

// TestECS_DeregisterContainerInstance_NotFound asserts the real code:
// DeregisterContainerInstance's own deserializer models no
// "ContainerInstanceNotFoundException" (that shape doesn't exist anywhere in
// ecs@v1.90.0), only InvalidParameterException for this condition.
func TestECS_DeregisterContainerInstance_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dereg-notfound-cluster"})

	rec := doECSRequest(t, h, "DeregisterContainerInstance", map[string]any{
		"cluster":           "dereg-notfound-cluster",
		"containerInstance": "arn:aws:ecs:us-east-1:000000000000:container-instance/x/nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "InvalidParameterException")
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
	_, err = backend.DeregisterContainerInstance(
		"ci-running-cluster",
		ci.ContainerInstanceArn,
		false,
	)
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
				"cluster":                  "update-ci-cluster",
				"instanceIdentityDocument": fakeInstanceIdentityDocument("i-update"),
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
		"cluster": "update-ci-notfound",
		"containerInstances": []string{
			"arn:aws:ecs:us-east-1:000000000000:container-instance/x/nonexistent",
		},
		"status": "DRAINING",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestECS_UpdateContainerAgent(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		valid    bool
	}{
		{
			name:     "updates agent status",
			wantCode: http.StatusOK,
			valid:    true,
		},
		{
			name:     "not found instance",
			wantCode: http.StatusBadRequest,
			valid:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "agent-cluster"})

			var containerInstanceArn string

			if tt.valid {
				rec := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
					"cluster":                  "agent-cluster",
					"instanceIdentityDocument": fakeInstanceIdentityDocument("i-agent-update"),
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var regResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &regResp))
				ci := regResp["containerInstance"].(map[string]any)
				containerInstanceArn = ci["containerInstanceArn"].(string)
			} else {
				containerInstanceArn = "nonexistent"
			}

			rec := doECSRequest(t, h, "UpdateContainerAgent", map[string]any{
				"cluster":           "agent-cluster",
				"containerInstance": containerInstanceArn,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			ci, ok := resp["containerInstance"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "PENDING", ci["agentUpdateStatus"])
		})
	}
}

func TestDescribeContainerInstances_FailureSemantics(t *testing.T) {
	t.Parallel()

	t.Run("unknown returns failure", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dci-cluster"})

		rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
			"cluster": "dci-cluster",
			"containerInstances": []string{
				"arn:aws:ecs:us-east-1:000000000000:container-instance/dci-cluster/ghost",
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		instances, _ := resp["containerInstances"].([]any)
		assert.Empty(t, instances)

		failures, _ := resp["failures"].([]any)
		require.Len(t, failures, 1)

		f := failures[0].(map[string]any)
		assert.Contains(t, f["arn"], "ghost")
		assert.Equal(t, "MISSING", f["reason"])
	})

	t.Run("mix found and missing", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dci-mix"})
		regResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
			"cluster": "dci-mix",
		})
		require.Equal(t, http.StatusOK, regResp.Code)

		var regOut map[string]any
		require.NoError(t, json.Unmarshal(regResp.Body.Bytes(), &regOut))
		ciArn := regOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

		rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
			"cluster": "dci-mix",
			"containerInstances": []string{
				ciArn,
				"arn:aws:ecs:us-east-1:000000000000:container-instance/dci-mix/ghost",
			},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		instances, _ := resp["containerInstances"].([]any)
		assert.Len(t, instances, 1)

		failures, _ := resp["failures"].([]any)
		assert.Len(t, failures, 1)
	})

	t.Run("cluster not found still errors", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
			"cluster":            "nonexistent-cluster",
			"containerInstances": []string{"any-instance"},
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("empty list returns all", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dci-all"})
		doECSRequest(t, h, "RegisterContainerInstance", map[string]any{"cluster": "dci-all"})
		doECSRequest(t, h, "RegisterContainerInstance", map[string]any{"cluster": "dci-all"})

		rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
			"cluster":            "dci-all",
			"containerInstances": []string{},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		instances, _ := resp["containerInstances"].([]any)
		assert.Len(t, instances, 2)

		failures, _ := resp["failures"].([]any)
		assert.Empty(t, failures)
	})
}

// TestDescribeContainerInstances_TagsRequireInclude proves that
// DescribeContainerInstances only returns tags when Include=["TAGS"] is
// specified, matching real AWS's DescribeContainerInstancesInput.Include
// gating. Previously the wire shape had no tags field at all, so tags
// applied via TagResource were invisible on Describe regardless of Include.
func TestDescribeContainerInstances_TagsRequireInclude(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ci-tags-cluster"})
	ciResp := doECSRequest(t, h, "RegisterContainerInstance", map[string]any{
		"cluster":                  "ci-tags-cluster",
		"instanceIdentityDocument": fakeInstanceIdentityDocument("i-tags-1234"),
	})
	require.Equal(t, http.StatusOK, ciResp.Code)

	var ciOut map[string]any
	require.NoError(t, json.Unmarshal(ciResp.Body.Bytes(), &ciOut))
	ciArn := ciOut["containerInstance"].(map[string]any)["containerInstanceArn"].(string)

	tagResp := doECSRequest(t, h, "TagResource", map[string]any{
		"resourceArn": ciArn,
		"tags":        []map[string]any{{"key": "env", "value": "prod"}},
	})
	require.Equal(t, http.StatusOK, tagResp.Code)

	t.Run("without include", func(t *testing.T) {
		t.Parallel()

		rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
			"cluster":            "ci-tags-cluster",
			"containerInstances": []string{ciArn},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ci := resp["containerInstances"].([]any)[0].(map[string]any)
		assert.Nil(t, ci["tags"])
	})

	t.Run("with include TAGS", func(t *testing.T) {
		t.Parallel()

		rec := doECSRequest(t, h, "DescribeContainerInstances", map[string]any{
			"cluster":            "ci-tags-cluster",
			"containerInstances": []string{ciArn},
			"include":            []string{"TAGS"},
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		ci := resp["containerInstances"].([]any)[0].(map[string]any)
		tags := ci["tags"].([]any)
		require.Len(t, tags, 1)
		assert.Equal(t, "env", tags[0].(map[string]any)["key"])
	})
}
