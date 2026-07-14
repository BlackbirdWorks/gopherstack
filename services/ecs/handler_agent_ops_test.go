package ecs_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ----- DescribeServiceRevisions -----

func TestECS_DescribeServiceRevisions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	tdRec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": "revfam",
		"containerDefinitions": []map[string]any{
			{"name": "web", "image": "nginx", "memory": 128},
		},
	})
	require.Equal(t, http.StatusOK, tdRec.Code)

	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "rev-svc",
		"taskDefinition": "revfam",
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	var svcResp map[string]any
	require.NoError(t, json.Unmarshal(svcRec.Body.Bytes(), &svcResp))

	svc := svcResp["service"].(map[string]any)
	deployments := svc["deployments"].([]any)
	require.Len(t, deployments, 1)

	// ServiceRevisionArn is tracked internally (mirroring how real AWS surfaces it
	// via ListServiceDeployments' TargetServiceRevision) rather than on the plain
	// Deployment wire shape, so fetch it directly from backend state.
	backendSvcs, _, err := h.Backend.DescribeServices("default", []string{"rev-svc"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 1)

	revisionArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, revisionArn, "deployment must carry a serviceRevisionArn")
	assert.Contains(t, revisionArn, ":service-revision/default/rev-svc/")

	tests := []struct {
		name         string
		arns         []string
		wantRevCount int
		wantFailures int
	}{
		{
			name:         "known revision arn returns the revision",
			arns:         []string{revisionArn},
			wantRevCount: 1,
			wantFailures: 0,
		},
		{
			name: "unknown arn returns a failure",
			arns: []string{
				"arn:aws:ecs:us-east-1:000000000000:service-revision/default/rev-svc/does-not-exist",
			},
			wantRevCount: 0,
			wantFailures: 1,
		},
		{
			name: "mixed known and unknown",
			arns: []string{
				revisionArn,
				"arn:aws:ecs:us-east-1:000000000000:service-revision/default/rev-svc/missing",
			},
			wantRevCount: 1,
			wantFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
				"serviceRevisionArns": tt.arns,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			revs, _ := resp["serviceRevisions"].([]any)
			failures, _ := resp["failures"].([]any)
			assert.Len(t, revs, tt.wantRevCount)
			assert.Len(t, failures, tt.wantFailures)

			if tt.wantRevCount > 0 {
				rev := revs[0].(map[string]any)
				assert.Equal(t, svc["serviceArn"], rev["serviceArn"])
				assert.Equal(t, svc["taskDefinition"], rev["taskDefinition"])
				assert.Equal(t, revisionArn, rev["serviceRevisionArn"])
			}
		})
	}
}

func TestECS_DescribeServiceRevisions_TracksUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "revfam2",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx", "memory": 128}},
	})
	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "revfam2",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx:2", "memory": 128}},
	})

	svcRec := doECSRequest(t, h, "CreateService", map[string]any{
		"serviceName":    "rev-svc-2",
		"taskDefinition": "revfam2:1",
		"desiredCount":   1,
	})
	require.Equal(t, http.StatusOK, svcRec.Code)

	backendSvcs, _, err := h.Backend.DescribeServices("default", []string{"rev-svc-2"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 1)
	firstArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, firstArn)

	updRec := doECSRequest(t, h, "UpdateService", map[string]any{
		"service":        "rev-svc-2",
		"taskDefinition": "revfam2:2",
	})
	require.Equal(t, http.StatusOK, updRec.Code)

	backendSvcs, _, err = h.Backend.DescribeServices("default", []string{"rev-svc-2"})
	require.NoError(t, err)
	require.Len(t, backendSvcs, 1)
	require.Len(t, backendSvcs[0].Deployments, 2)

	secondArn := backendSvcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, secondArn)
	assert.NotEqual(t, firstArn, secondArn, "update must mint a new service revision")

	rec := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
		"serviceRevisionArns": []string{firstArn, secondArn},
	})

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	revs, _ := resp["serviceRevisions"].([]any)
	assert.Len(t, revs, 2, "both the original and rotated revision remain describable")
}

// ----- DiscoverPollEndpoint -----

func TestECS_DiscoverPollEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "DiscoverPollEndpoint", map[string]any{
		"clusterArn":           "default",
		"containerInstanceArn": "arn:aws:ecs:us-east-1:000000000000:container-instance/default/abc",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	assert.Contains(t, resp["endpoint"], testRegion)
	assert.Contains(t, resp["serviceConnectEndpoint"], testRegion)
	assert.Contains(t, resp["telemetryEndpoint"], testRegion)
}

// ----- Submit*StateChange -----

func TestECS_SubmitTaskStateChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "statefam",
		"containerDefinitions": []map[string]any{{"name": "web", "image": "nginx", "memory": 128}},
	})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "statefam",
	})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
	tasks := runResp["tasks"].([]any)
	require.Len(t, tasks, 1)
	task := tasks[0].(map[string]any)
	taskArn := task["taskArn"].(string)

	rec := doECSRequest(t, h, "SubmitTaskStateChange", map[string]any{
		"cluster": "default",
		"task":    taskArn,
		"status":  "STOPPED",
		"reason":  "essential container exited",
		"containers": []map[string]any{
			{"containerName": "web", "exitCode": 137, "reason": "OOMKilled", "runtimeId": "rt-123"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACK", resp["acknowledgment"])

	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "default",
		"tasks":   []string{taskArn},
	})
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	descTasks := descResp["tasks"].([]any)
	require.Len(t, descTasks, 1)

	updated := descTasks[0].(map[string]any)
	assert.Equal(t, "STOPPED", updated["lastStatus"])
	assert.Equal(t, "essential container exited", updated["stoppedReason"])

	containers := updated["containers"].([]any)
	require.Len(t, containers, 1)
	container := containers[0].(map[string]any)
	assert.InDelta(t, float64(137), container["exitCode"], 0)
	assert.Equal(t, "OOMKilled", container["reason"])
	assert.Equal(t, "rt-123", container["runtimeId"])
}

func TestECS_SubmitTaskStateChange_UnknownTaskIsNoop(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doECSRequest(t, h, "SubmitTaskStateChange", map[string]any{
		"cluster": "default",
		"task":    "does-not-exist",
		"status":  "STOPPED",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ACK", resp["acknowledgment"])
}

func TestECS_SubmitContainerStateChange(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "cstatefam",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "app:1", "memory": 128}},
	})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "cstatefam",
	})

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
	taskArn := runResp["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)

	rec := doECSRequest(t, h, "SubmitContainerStateChange", map[string]any{
		"cluster":       "default",
		"task":          taskArn,
		"containerName": "app",
		"status":        "RUNNING",
		"runtimeId":     "rt-999",
		"networkBindings": []map[string]any{
			{"bindIP": "0.0.0.0", "containerPort": 80, "hostPort": 8080, "protocol": "tcp"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "default",
		"tasks":   []string{taskArn},
	})

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	updated := descResp["tasks"].([]any)[0].(map[string]any)
	container := updated["containers"].([]any)[0].(map[string]any)

	assert.Equal(t, "RUNNING", container["lastStatus"])
	assert.Equal(t, "rt-999", container["runtimeId"])
	bindings := container["networkBindings"].([]any)
	require.Len(t, bindings, 1)
	assert.InDelta(t, float64(8080), bindings[0].(map[string]any)["hostPort"], 0)
}

func TestECS_SubmitAttachmentStateChanges(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family":               "attachfam",
		"containerDefinitions": []map[string]any{{"name": "app", "image": "app:1", "memory": 128}},
	})

	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"taskDefinition": "attachfam",
		"launchType":     "FARGATE",
	})

	var runResp map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runResp))
	task := runResp["tasks"].([]any)[0].(map[string]any)
	taskArn := task["taskArn"].(string)
	attachments, _ := task["attachments"].([]any)
	require.NotEmpty(t, attachments, "FARGATE tasks are created with an ENI attachment")
	attachmentID := attachments[0].(map[string]any)["id"].(string)

	rec := doECSRequest(t, h, "SubmitAttachmentStateChanges", map[string]any{
		"cluster": "default",
		"attachments": []map[string]any{
			{"attachmentArn": attachmentID, "status": "DETACHED"},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	descRec := doECSRequest(t, h, "DescribeTasks", map[string]any{
		"cluster": "default",
		"tasks":   []string{taskArn},
	})

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	updated := descResp["tasks"].([]any)[0].(map[string]any)
	updatedAttachments := updated["attachments"].([]any)
	require.NotEmpty(t, updatedAttachments)
	assert.Equal(t, "DETACHED", updatedAttachments[0].(map[string]any)["status"])
}

func TestHandler_DiscoverPollEndpoint(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input           map[string]any
		name            string
		wantEndpointPfx string
		wantStatus      int
		wantTelemetry   bool
	}{
		{
			name:            "no cluster arg returns regional endpoint",
			input:           map[string]any{},
			wantStatus:      http.StatusOK,
			wantEndpointPfx: "https://ecs-a-1.us-east-1.amazonaws.com/",
			wantTelemetry:   true,
		},
		{
			name: "with cluster and container instance arg",
			input: map[string]any{
				"clusterArn":           "arn:aws:ecs:us-east-1:000000000000:cluster/test",
				"containerInstanceArn": "arn:aws:ecs:us-east-1:000000000000:container-instance/abc",
			},
			wantStatus:      http.StatusOK,
			wantEndpointPfx: "https://ecs-a-1.us-east-1.amazonaws.com/",
			wantTelemetry:   true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "DiscoverPollEndpoint", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			assert.Equal(t, tt.wantEndpointPfx, out["endpoint"])
			if tt.wantTelemetry {
				assert.NotEmpty(t, out["telemetryEndpoint"])
			}
		})
	}
}

func TestHandler_SubmitTaskStateChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(h interface{ Handler() any }) // unused, h is the handler
		clusterFn  func(t *testing.T) (clusterName string)
		input      func(clusterName, taskArn string) map[string]any
		name       string
		wantStatus int
		wantACK    bool
	}{
		{
			name: "valid task state change returns ACK",
			input: func(clusterName, taskArn string) map[string]any {
				return map[string]any{
					"cluster": clusterName,
					"task":    taskArn,
					"status":  "RUNNING",
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
		{
			name: "STOPPED status updates task",
			input: func(clusterName, taskArn string) map[string]any {
				return map[string]any{
					"cluster": clusterName,
					"task":    taskArn,
					"status":  "STOPPED",
					"reason":  "agent stopped it",
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
		{
			// Real AWS documents SubmitTaskStateChange as "only used by the
			// Amazon ECS agent, and not intended for use outside of the
			// agent" — consistent with that eventually-consistent,
			// agent-internal contract, an unknown cluster/task reference is
			// tolerated as a no-op ACK rather than surfaced as a client error
			// (see backend_agent_ops.go's package doc comment).
			name: "unknown cluster is tolerated as a no-op ACK",
			input: func(_, _ string) map[string]any {
				return map[string]any{
					"cluster": "nonexistent",
					"task":    "arn:aws:ecs:us-east-1:000000000000:task/nonexistent/abc",
					"status":  "RUNNING",
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			clusterName := "submit-task-test"
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": clusterName})

			taskArn := ""
			if tt.name != "unknown cluster is tolerated as a no-op ACK" {
				registerTaskDef(t, h, "basic", "nginx")
				runRec := doECSRequest(t, h, "RunTask", map[string]any{
					"cluster":        clusterName,
					"taskDefinition": "basic",
					"count":          1,
				})
				require.Equal(t, http.StatusOK, runRec.Code)

				var runOut map[string]any
				require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
				tasks := runOut["tasks"].([]any)
				require.NotEmpty(t, tasks)
				taskArn = tasks[0].(map[string]any)["taskArn"].(string)
			}

			rec := doECSRequest(t, h, "SubmitTaskStateChange", tt.input(clusterName, taskArn))
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantACK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "ACK", out["acknowledgment"])
			}
		})
	}
}

func TestHandler_SubmitContainerStateChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		inputFn    func(cluster, taskArn string) map[string]any
		name       string
		wantStatus int
		wantACK    bool
	}{
		{
			name: "valid container state change returns ACK",
			inputFn: func(cluster, taskArn string) map[string]any {
				return map[string]any{
					"cluster":       cluster,
					"task":          taskArn,
					"containerName": "app",
					"status":        "RUNNING",
					"runtimeId":     "abc123",
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
		{
			name: "with exit code on stopped container",
			inputFn: func(cluster, taskArn string) map[string]any {
				ec := 0
				_ = ec

				return map[string]any{
					"cluster":       cluster,
					"task":          taskArn,
					"containerName": "app",
					"status":        "STOPPED",
					"exitCode":      0,
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
		{
			// See backend_agent_ops.go's package doc comment: agent-facing
			// Submit* operations tolerate unknown cluster/task/container
			// references as no-ops rather than surfacing a client error,
			// matching the real, agent-internal, eventually-consistent
			// contract AWS documents for these operations.
			name: "nonexistent cluster is tolerated as a no-op ACK",
			inputFn: func(_, _ string) map[string]any {
				return map[string]any{
					"cluster":       "does-not-exist",
					"task":          "arn:aws:ecs:us-east-1:000000000000:task/c/t",
					"containerName": "x",
					"status":        "RUNNING",
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			clusterName := "container-sc-test"
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": clusterName})
			registerTaskDef(t, h, "appdef", "nginx")

			taskArn := ""
			if tt.name != "nonexistent cluster is tolerated as a no-op ACK" {
				runRec := doECSRequest(t, h, "RunTask", map[string]any{
					"cluster":        clusterName,
					"taskDefinition": "appdef",
					"count":          1,
				})
				require.Equal(t, http.StatusOK, runRec.Code)
				var runOut map[string]any
				require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
				taskArn = runOut["tasks"].([]any)[0].(map[string]any)["taskArn"].(string)
			}

			rec := doECSRequest(
				t,
				h,
				"SubmitContainerStateChange",
				tt.inputFn(clusterName, taskArn),
			)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantACK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "ACK", out["acknowledgment"])
			}
		})
	}
}

func TestHandler_SubmitAttachmentStateChanges(t *testing.T) {
	t.Parallel()

	tests := []struct {
		inputFn    func(cluster string) map[string]any
		name       string
		wantStatus int
		wantACK    bool
	}{
		{
			name: "valid cluster with attachments returns ACK",
			inputFn: func(cluster string) map[string]any {
				return map[string]any{
					"cluster": cluster,
					"attachments": []map[string]any{
						{
							"attachmentArn": "arn:aws:ecs:us-east-1:000000000000:attachment/abc",
							"status":        "ATTACHED",
						},
					},
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
		{
			name: "empty attachments returns ACK",
			inputFn: func(cluster string) map[string]any {
				return map[string]any{
					"cluster":     cluster,
					"attachments": []any{},
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
		{
			// See backend_agent_ops.go's package doc comment: agent-facing
			// Submit* operations tolerate an unknown cluster as a no-op
			// rather than surfacing a client error, matching the real,
			// agent-internal, eventually-consistent contract AWS documents
			// for these operations.
			name: "nonexistent cluster is tolerated as a no-op ACK",
			inputFn: func(_ string) map[string]any {
				return map[string]any{
					"cluster":     "does-not-exist",
					"attachments": []any{},
				}
			},
			wantStatus: http.StatusOK,
			wantACK:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			clusterName := "attach-sc-test"
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": clusterName})

			input := tt.inputFn(clusterName)
			rec := doECSRequest(t, h, "SubmitAttachmentStateChanges", input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantACK {
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				assert.Equal(t, "ACK", out["acknowledgment"])
			}
		})
	}
}

func TestHandler_DescribeServiceRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setupFn        func(h *testHandlerWrapper) string // returns service ARN
		revisionArnsFn func(serviceArn string) []string
		name           string
		wantStatus     int
		wantRevCount   int
		wantFailures   int
	}{
		{
			name: "service revision created on CreateService",
			setupFn: func(w *testHandlerWrapper) string {
				w.createCluster("rev-cluster")
				registerTaskDef(t, w.h, "revtask", "nginx")
				rec := doECSRequest(t, w.h, "CreateService", map[string]any{
					"cluster":        "rev-cluster",
					"serviceName":    "mysvc",
					"taskDefinition": "revtask",
					"desiredCount":   0,
				})
				require.Equal(t, http.StatusOK, rec.Code)
				var out map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
				svc := out["service"].(map[string]any)

				return svc["serviceArn"].(string)
			},
			revisionArnsFn: func(_ string) []string {
				// We don't know the ARN without querying - we'll use the full describe flow
				return nil
			},
			wantStatus:   http.StatusOK,
			wantRevCount: 0, // tested separately below
		},
		{
			name: "empty ARN list returns empty result",
			setupFn: func(_ *testHandlerWrapper) string {
				return ""
			},
			revisionArnsFn: func(_ string) []string {
				return []string{}
			},
			wantStatus:   http.StatusOK,
			wantRevCount: 0,
			wantFailures: 0,
		},
		{
			name: "unknown revision ARN goes to failures",
			setupFn: func(_ *testHandlerWrapper) string {
				return ""
			},
			revisionArnsFn: func(_ string) []string {
				return []string{"arn:aws:ecs:us-east-1:000000000000:service-revision/c/s/99"}
			},
			wantStatus:   http.StatusOK,
			wantRevCount: 0,
			wantFailures: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			w := &testHandlerWrapper{h: h, t: t}

			_ = tt.setupFn(w)

			arns := tt.revisionArnsFn("")
			rec := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
				"serviceRevisionArns": arns,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))

			revs, _ := out["serviceRevisions"].([]any)
			assert.Len(t, revs, tt.wantRevCount)

			fails, _ := out["failures"].([]any)
			assert.Len(t, fails, tt.wantFailures)
		})
	}
}

func TestHandler_DescribeServiceRevisions_CreateAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "rc"})
	registerTaskDef(t, h, "td1", "nginx")
	registerTaskDef(t, h, "td1", "nginx:latest")

	// Create service → revision 1.
	rec := doECSRequest(t, h, "CreateService", map[string]any{
		"cluster":        "rc",
		"serviceName":    "svc",
		"taskDefinition": "td1:1",
		"desiredCount":   0,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Revision ARNs are derived from a per-deployment UUID (see
	// serviceRevisionArnFor in backend_services.go), not a predictable sequential
	// number, and are intentionally omitted from the DescribeServices/
	// CreateService wire view (see toDeploymentView's doc comment) to match the
	// real AWS Deployment wire shape. So the test reads the ARN back via the
	// Backend directly, the same way the real ECS agent's own control loop would
	// observe it internally, rather than trying to discover it over HTTP.
	svcs, failures, err := h.Backend.DescribeServices("rc", []string{"svc"})
	require.NoError(t, err)
	require.Empty(t, failures)
	require.Len(t, svcs, 1)
	require.Len(t, svcs[0].Deployments, 1)
	rev1Arn := svcs[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, rev1Arn)

	rec2 := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
		"serviceRevisionArns": []string{rev1Arn},
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var out2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &out2))
	revs := out2["serviceRevisions"].([]any)
	require.Len(t, revs, 1, "expected one revision after CreateService")
	rev := revs[0].(map[string]any)
	assert.Equal(t, rev1Arn, rev["serviceRevisionArn"])
	assert.NotEmpty(t, rev["taskDefinition"])

	// Update service → new PRIMARY deployment with its own revision.
	updateRec := doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":        "rc",
		"service":        "svc",
		"taskDefinition": "td1:2",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	svcs2, failures2, err := h.Backend.DescribeServices("rc", []string{"svc"})
	require.NoError(t, err)
	require.Empty(t, failures2)
	require.Len(t, svcs2, 1)
	require.NotEmpty(t, svcs2[0].Deployments)
	rev2Arn := svcs2[0].Deployments[0].ServiceRevisionArn
	require.NotEmpty(t, rev2Arn)
	require.NotEqual(t, rev1Arn, rev2Arn, "update should produce a new revision ARN")

	rec3 := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
		"serviceRevisionArns": []string{rev2Arn},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var out3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &out3))
	revs3 := out3["serviceRevisions"].([]any)
	assert.Len(t, revs3, 1, "expected one revision after UpdateService")
}

// testHandlerWrapper wraps Handler for setup helpers in table tests.
type testHandlerWrapper struct {
	h *ecs.Handler
	t *testing.T
}

func (w *testHandlerWrapper) createCluster(name string) {
	w.t.Helper()
	doECSRequest(w.t, w.h, "CreateCluster", map[string]any{"clusterName": name})
}

// registerTaskDef registers a minimal task definition for tests.
func registerTaskDef(t *testing.T, h *ecs.Handler, family, image string) {
	t.Helper()
	rec := doECSRequest(t, h, "RegisterTaskDefinition", map[string]any{
		"family": family,
		"containerDefinitions": []map[string]any{
			{"name": "app", "image": image, "essential": true},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "registerTaskDef: %s", rec.Body.String())
}
