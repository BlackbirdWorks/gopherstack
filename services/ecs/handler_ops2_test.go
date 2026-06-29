package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ----- ListAccountSettings tests -----

func TestECS_ListAccountSettings(t *testing.T) {
	t.Parallel()

	t.Run("empty list", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)
		rec := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		settings, ok := resp["settings"].([]any)
		require.True(t, ok)
		assert.Empty(t, settings)
	})

	t.Run("lists all after put", func(t *testing.T) {
		t.Parallel()

		h := newTestHandler(t)

		// Seed two account settings.
		putRec := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name": "containerInsights", "value": "enabled",
		})
		require.Equal(t, http.StatusOK, putRec.Code)

		putRec2 := doECSRequest(t, h, "PutAccountSetting", map[string]any{
			"name": "serviceLongArnFormat", "value": "enabled",
		})
		require.Equal(t, http.StatusOK, putRec2.Code)

		// List and verify.
		rec := doECSRequest(t, h, "ListAccountSettings", map[string]any{})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

		settings, ok := resp["settings"].([]any)
		require.True(t, ok)
		assert.Len(t, settings, 2)

		// Verify field shape of the first setting.
		first := settings[0].(map[string]any)
		assert.NotEmpty(t, first["name"])
		assert.NotEmpty(t, first["value"])
	})
}

// ----- PutAccountSetting tests -----

func TestECS_PutAccountSetting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		wantName  string
		wantValue string
		wantCode  int
	}{
		{
			name:      "creates setting",
			input:     map[string]any{"name": "containerInstanceLongArnFormat", "value": "enabled"},
			wantCode:  http.StatusOK,
			wantName:  "containerInstanceLongArnFormat",
			wantValue: "enabled",
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{"value": "enabled"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "missing value returns 400",
			input:    map[string]any{"name": "containerInstanceLongArnFormat"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "PutAccountSetting", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			setting, ok := resp["setting"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, setting["name"])
			assert.Equal(t, tt.wantValue, setting["value"])
		})
	}
}

// ----- PutAccountSettingDefault tests -----

func TestECS_PutAccountSettingDefault(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input     map[string]any
		name      string
		wantName  string
		wantValue string
		wantCode  int
	}{
		{
			name:      "creates default setting",
			input:     map[string]any{"name": "taskLongArnFormat", "value": "enabled"},
			wantCode:  http.StatusOK,
			wantName:  "taskLongArnFormat",
			wantValue: "enabled",
		},
		{
			name:     "missing name returns 400",
			input:    map[string]any{"value": "enabled"},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "PutAccountSettingDefault", tt.input)
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			setting, ok := resp["setting"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, tt.wantName, setting["name"])
			assert.Equal(t, tt.wantValue, setting["value"])
		})
	}
}

// ----- ListAttributes / PutAttributes tests -----

func TestECS_PutAndListAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		cluster  string
		attrs    []map[string]any
		wantCode int
		wantLen  int
	}{
		{
			name:    "puts single attribute",
			cluster: "test-attrs-cluster",
			attrs: []map[string]any{
				{
					"name":       "ecs.capability.docker-remote-api.1.21",
					"targetId":   "container-instance/abc",
					"targetType": "container-instance",
				},
			},
			wantCode: http.StatusOK,
			wantLen:  1,
		},
		{
			name:    "puts multiple attributes",
			cluster: "test-attrs-cluster2",
			attrs: []map[string]any{
				{"name": "attr1", "targetId": "ci/1"},
				{"name": "attr2", "targetId": "ci/2"},
			},
			wantCode: http.StatusOK,
			wantLen:  2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": tt.cluster})

			// Put attributes
			rec := doECSRequest(t, h, "PutAttributes", map[string]any{
				"cluster":    tt.cluster,
				"attributes": tt.attrs,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var putResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &putResp))

			attrs, ok := putResp["attributes"].([]any)
			require.True(t, ok)
			assert.Len(t, attrs, tt.wantLen)

			// List attributes
			listRec := doECSRequest(t, h, "ListAttributes", map[string]any{"cluster": tt.cluster})
			require.Equal(t, http.StatusOK, listRec.Code)

			var listResp map[string]any
			require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))

			listedAttrs, ok := listResp["attributes"].([]any)
			require.True(t, ok)
			assert.Len(t, listedAttrs, tt.wantLen)
		})
	}
}

// ----- PutClusterCapacityProviders tests -----

func TestECS_PutClusterCapacityProviders(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name              string
		capacityProviders []string
		wantCode          int
	}{
		{
			name:              "sets capacity providers",
			capacityProviders: []string{"FARGATE", "FARGATE_SPOT"},
			wantCode:          http.StatusOK,
		},
		{
			name:              "empty capacity providers",
			capacityProviders: []string{},
			wantCode:          http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cp-cluster"})

			rec := doECSRequest(t, h, "PutClusterCapacityProviders", map[string]any{
				"cluster":           "cp-cluster",
				"capacityProviders": tt.capacityProviders,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			cluster, ok := resp["cluster"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "cp-cluster", cluster["clusterName"])
		})
	}
}

// ----- UpdateClusterSettings tests -----

func TestECS_UpdateClusterSettings(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		settings []map[string]any
		wantCode int
	}{
		{
			name:     "updates cluster settings",
			settings: []map[string]any{{"name": "containerInsights", "value": "enabled"}},
			wantCode: http.StatusOK,
		},
		{
			name:     "not found cluster",
			settings: []map[string]any{},
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			cluster := "settings-cluster"
			if tt.wantCode == http.StatusOK {
				doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": cluster})
			} else {
				cluster = "nonexistent-cluster"
			}

			rec := doECSRequest(t, h, "UpdateClusterSettings", map[string]any{
				"cluster":  cluster,
				"settings": tt.settings,
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			c, ok := resp["cluster"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, cluster, c["clusterName"])
		})
	}
}

// ----- UpdateContainerAgent tests -----

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
					"cluster":       "agent-cluster",
					"ec2InstanceId": "i-agent-update",
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

// ----- UpdateExpressGatewayService tests -----

func TestECS_UpdateExpressGatewayService(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "updates service",
			wantCode: http.StatusOK,
		},
		{
			name:     "not found",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var serviceArn string

			if tt.wantCode == http.StatusOK {
				rec := doECSRequest(t, h, "CreateExpressGatewayService", map[string]any{
					"executionRoleArn":      "arn:aws:iam::000000000000:role/exec",
					"infrastructureRoleArn": "arn:aws:iam::000000000000:role/infra",
					"serviceName":           "my-gw-service",
				})
				require.Equal(t, http.StatusOK, rec.Code)

				var createResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
				svc := createResp["service"].(map[string]any)
				serviceArn = svc["serviceArn"].(string)
			} else {
				serviceArn = "arn:aws:ecs:us-east-1:000000000000:service/nonexistent"
			}

			rec := doECSRequest(t, h, "UpdateExpressGatewayService", map[string]any{
				"serviceArn":            serviceArn,
				"executionRoleArn":      "arn:aws:iam::000000000000:role/new-exec",
				"infrastructureRoleArn": "arn:aws:iam::000000000000:role/new-infra",
			})
			require.Equal(t, tt.wantCode, rec.Code)

			if tt.wantCode != http.StatusOK {
				return
			}

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			svc, ok := resp["service"].(map[string]any)
			require.True(t, ok)
			assert.Equal(t, "arn:aws:iam::000000000000:role/new-exec", svc["executionRoleArn"])
			assert.Equal(
				t,
				"arn:aws:iam::000000000000:role/new-infra",
				svc["infrastructureRoleArn"],
			)
		})
	}
}

// ----- GetTaskProtection / UpdateTaskProtection tests -----

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
