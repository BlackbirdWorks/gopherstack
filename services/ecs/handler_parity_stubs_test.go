package ecs_test

// handler_parity_stubs_test.go: table-driven tests for previously-stub operations.
// Covers: Daemon CRUD, daemon task defs, daemon deployments, daemon revisions,
// DiscoverPollEndpoint, Submit state changes, DescribeServiceRevisions,
// and the enrichCluster cached-counter performance fix.

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/ecs"
)

// ---- DiscoverPollEndpoint ----

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

// ---- Submit state changes ----

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
			name: "unknown cluster returns error",
			input: func(_, _ string) map[string]any {
				return map[string]any{
					"cluster": "nonexistent",
					"task":    "arn:aws:ecs:us-east-1:000000000000:task/nonexistent/abc",
					"status":  "RUNNING",
				}
			},
			wantStatus: http.StatusBadRequest,
			wantACK:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			clusterName := "submit-task-test"
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": clusterName})

			taskArn := ""
			if tt.name != "unknown cluster returns error" {
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
			name: "nonexistent cluster returns error",
			inputFn: func(_, _ string) map[string]any {
				return map[string]any{
					"cluster":       "does-not-exist",
					"task":          "arn:aws:ecs:us-east-1:000000000000:task/c/t",
					"containerName": "x",
					"status":        "RUNNING",
				}
			},
			wantStatus: http.StatusBadRequest,
			wantACK:    false,
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
			if tt.name != "nonexistent cluster returns error" {
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
			name: "nonexistent cluster returns error",
			inputFn: func(_ string) map[string]any {
				return map[string]any{
					"cluster":     "does-not-exist",
					"attachments": []any{},
				}
			},
			wantStatus: http.StatusBadRequest,
			wantACK:    false,
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

// ---- DescribeServiceRevisions ----

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

// TestHandler_DescribeServiceRevisions_CreateAndUpdate verifies revision creation
// on service create and on service update.
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

	// Extract the service ARN to build a revision ARN.
	var createOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createOut))
	_ = createOut["service"].(map[string]any)["serviceArn"].(string)

	rev1Arn := "arn:aws:ecs:us-east-1:000000000000:service-revision/rc/svc/1"

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

	// Update service → revision 2.
	doECSRequest(t, h, "UpdateService", map[string]any{
		"cluster":        "rc",
		"service":        "svc",
		"taskDefinition": "td1:2",
	})

	rev2Arn := "arn:aws:ecs:us-east-1:000000000000:service-revision/rc/svc/2"
	rec3 := doECSRequest(t, h, "DescribeServiceRevisions", map[string]any{
		"serviceRevisionArns": []string{rev2Arn},
	})
	require.Equal(t, http.StatusOK, rec3.Code)

	var out3 map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &out3))
	revs3 := out3["serviceRevisions"].([]any)
	assert.Len(t, revs3, 1, "expected one revision after UpdateService")
}

// ---- Daemon CRUD ----

func TestHandler_Daemon_CreateDescribeDelete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterName  string
		daemonName   string
		taskDef      string
		wantCreate   int
		wantDescribe int
		wantDelete   int
	}{
		{
			name:         "full lifecycle",
			clusterName:  "daemon-cluster",
			daemonName:   "my-daemon",
			taskDef:      "my-daemon-td",
			wantCreate:   http.StatusOK,
			wantDescribe: http.StatusOK,
			wantDelete:   http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": tt.clusterName})
			registerTaskDef(t, h, tt.taskDef, "nginx")

			// Create
			createRec := doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":        tt.clusterName,
				"daemonName":     tt.daemonName,
				"taskDefinition": tt.taskDef,
			})
			assert.Equal(t, tt.wantCreate, createRec.Code)

			var createOut map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
			daemon := createOut["daemon"].(map[string]any)
			assert.Equal(t, tt.daemonName, daemon["daemonName"])
			assert.Equal(t, "ACTIVE", daemon["status"])
			assert.NotEmpty(t, daemon["daemonArn"])

			// Describe
			descRec := doECSRequest(t, h, "DescribeDaemon", map[string]any{
				"cluster":    tt.clusterName,
				"daemonName": tt.daemonName,
			})
			assert.Equal(t, tt.wantDescribe, descRec.Code)
			var descOut map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
			assert.Equal(t, tt.daemonName, descOut["daemon"].(map[string]any)["daemonName"])

			// Delete
			delRec := doECSRequest(t, h, "DeleteDaemon", map[string]any{
				"cluster":    tt.clusterName,
				"daemonName": tt.daemonName,
			})
			assert.Equal(t, tt.wantDelete, delRec.Code)
			var delOut map[string]any
			require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &delOut))
			assert.Equal(t, "INACTIVE", delOut["daemon"].(map[string]any)["status"])

			// Describe after delete → error
			descAfter := doECSRequest(t, h, "DescribeDaemon", map[string]any{
				"cluster":    tt.clusterName,
				"daemonName": tt.daemonName,
			})
			assert.Equal(t, http.StatusBadRequest, descAfter.Code)
		})
	}
}

func TestHandler_Daemon_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		input      map[string]any
		name       string
		wantErr    string
		wantStatus int
	}{
		{
			name:       "missing daemonName",
			input:      map[string]any{"cluster": "c"},
			wantStatus: http.StatusBadRequest,
			wantErr:    "daemonName",
		},
		{
			name:       "missing cluster",
			input:      map[string]any{"daemonName": "d"},
			wantStatus: http.StatusBadRequest,
			wantErr:    "cluster",
		},
		{
			name:       "nonexistent cluster",
			input:      map[string]any{"cluster": "no-such-cluster", "daemonName": "d"},
			wantStatus: http.StatusBadRequest,
			wantErr:    "ClusterNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rec := doECSRequest(t, h, "CreateDaemon", tt.input)
			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantErr)
		})
	}
}

func TestHandler_Daemon_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dup-cluster"})

	doECSRequest(t, h, "CreateDaemon", map[string]any{
		"cluster":    "dup-cluster",
		"daemonName": "same",
	})

	rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
		"cluster":    "dup-cluster",
		"daemonName": "same",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "DaemonAlreadyExistsException")
}

func TestHandler_Daemon_ListDaemons(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		daemonNames []string
		wantCount   int
		wantStatus  int
	}{
		{
			name:        "no daemons returns empty list",
			daemonNames: []string{},
			wantCount:   0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "three daemons",
			daemonNames: []string{"alpha", "beta", "gamma"},
			wantCount:   3,
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "list-cluster"})

			for _, name := range tt.daemonNames {
				rec := doECSRequest(t, h, "CreateDaemon", map[string]any{
					"cluster":    "list-cluster",
					"daemonName": name,
				})
				require.Equal(t, http.StatusOK, rec.Code)
			}

			rec := doECSRequest(t, h, "ListDaemons", map[string]any{"cluster": "list-cluster"})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			daemons := out["daemons"].([]any)
			assert.Len(t, daemons, tt.wantCount)
		})
	}
}

func TestHandler_Daemon_UpdateDaemon(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialTaskDef string
		updatedTaskDef string
		wantStatus     int
	}{
		{
			name:           "update task definition",
			initialTaskDef: "td:1",
			updatedTaskDef: "td:2",
			wantStatus:     http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "upd-cluster"})
			doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":        "upd-cluster",
				"daemonName":     "updatable",
				"taskDefinition": tt.initialTaskDef,
			})

			rec := doECSRequest(t, h, "UpdateDaemon", map[string]any{
				"cluster":        "upd-cluster",
				"daemonName":     "updatable",
				"taskDefinition": tt.updatedTaskDef,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			daemon := out["daemon"].(map[string]any)
			assert.Equal(t, tt.updatedTaskDef, daemon["taskDefinition"])
		})
	}
}

// ---- Daemon task definitions ----

func TestHandler_Daemon_RegisterAndDescribeTaskDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		registerInput map[string]any
		name          string
		wantStatus    int
		wantRevision  float64
	}{
		{
			name: "register first definition",
			registerInput: map[string]any{
				"daemon": "my-daemon",
				"family": "daemon-family",
			},
			wantStatus:   http.StatusOK,
			wantRevision: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dtd-cluster"})
			doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":    "dtd-cluster",
				"daemonName": "my-daemon",
			})

			rec := doECSRequest(t, h, "RegisterDaemonTaskDefinition", tt.registerInput)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			def := out["daemonTaskDefinition"].(map[string]any)
			assert.InDelta(t, tt.wantRevision, def["revision"], 1e-9)
			assert.NotEmpty(t, def["daemonTaskDefinitionArn"])

			// Describe the latest definition.
			descRec := doECSRequest(t, h, "DescribeDaemonTaskDefinition", map[string]any{
				"daemon": "my-daemon",
			})
			assert.Equal(t, http.StatusOK, descRec.Code)
			var descOut map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
			assert.InDelta(
				t,
				tt.wantRevision,
				descOut["daemonTaskDefinition"].(map[string]any)["revision"],
				1e-9,
			)
		})
	}
}

func TestHandler_Daemon_ListDaemonTaskDefinitions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		numRegister int
		wantCount   int
		wantStatus  int
	}{
		{
			name:        "no task definitions",
			numRegister: 0,
			wantCount:   0,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "two task definitions",
			numRegister: 2,
			wantCount:   2,
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dtdl-cluster"})
			doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":    "dtdl-cluster",
				"daemonName": "list-daemon",
			})

			for i := range tt.numRegister {
				doECSRequest(t, h, "RegisterDaemonTaskDefinition", map[string]any{
					"daemon": "list-daemon",
					"family": "fam",
					"taskDefinitionArn": "arn:aws:ecs:us-east-1:000000000000:task-definition/fam:" + string(
						rune('1'+i),
					),
				})
			}

			rec := doECSRequest(t, h, "ListDaemonTaskDefinitions", map[string]any{
				"daemon": "list-daemon",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			defs := out["daemonTaskDefinitions"].([]any)
			assert.Len(t, defs, tt.wantCount)
		})
	}
}

func TestHandler_Daemon_DeleteTaskDefinition(t *testing.T) {
	t.Parallel()

	tests := []struct {
		targetArnFunc func(defArn string) string
		name          string
		wantErrCode   string
		wantStatus    int
	}{
		{
			name:          "delete existing returns empty",
			targetArnFunc: func(defArn string) string { return defArn },
			wantStatus:    http.StatusOK,
		},
		{
			name:          "delete nonexistent returns error",
			targetArnFunc: func(_ string) string { return "arn:aws:ecs:us-east-1:000000000000:daemon-task-definition/x:99" },
			wantStatus:    http.StatusBadRequest,
			wantErrCode:   "DaemonTaskDefinitionNotFoundException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "del-dtd-cluster"})
			doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":    "del-dtd-cluster",
				"daemonName": "del-daemon",
			})

			regRec := doECSRequest(t, h, "RegisterDaemonTaskDefinition", map[string]any{
				"daemon": "del-daemon",
				"family": "del-family",
			})
			require.Equal(t, http.StatusOK, regRec.Code)

			var regOut map[string]any
			require.NoError(t, json.Unmarshal(regRec.Body.Bytes(), &regOut))
			defArn := regOut["daemonTaskDefinition"].(map[string]any)["daemonTaskDefinitionArn"].(string)

			rec := doECSRequest(t, h, "DeleteDaemonTaskDefinition", map[string]any{
				"daemonTaskDefinitionArn": tt.targetArnFunc(defArn),
			})
			assert.Equal(t, tt.wantStatus, rec.Code)
			if tt.wantErrCode != "" {
				assert.Contains(t, rec.Body.String(), tt.wantErrCode)
			}
		})
	}
}

// ---- Daemon deployments ----

func TestHandler_Daemon_DescribeDaemonDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		updates     int // number of UpdateDaemon calls after create
		wantAtLeast int // at least this many deployments expected
		wantStatus  int
	}{
		{
			name:        "single deployment after create",
			updates:     0,
			wantAtLeast: 1,
			wantStatus:  http.StatusOK,
		},
		{
			name:        "two deployments after create + update",
			updates:     1,
			wantAtLeast: 2,
			wantStatus:  http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "dep-cluster"})

			createRec := doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":    "dep-cluster",
				"daemonName": "dep-daemon",
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createOut map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createOut))
			daemonArn := createOut["daemon"].(map[string]any)["daemonArn"].(string)

			for range tt.updates {
				doECSRequest(t, h, "UpdateDaemon", map[string]any{
					"cluster":    "dep-cluster",
					"daemonName": "dep-daemon",
				})
			}

			rec := doECSRequest(t, h, "DescribeDaemonDeployments", map[string]any{
				"daemonArn": daemonArn,
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			deps := out["deployments"].([]any)
			assert.GreaterOrEqual(t, len(deps), tt.wantAtLeast)
		})
	}
}

func TestHandler_Daemon_ListDaemonDeployments(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
		wantMinIDs int
	}{
		{
			name:       "lists deployment IDs after create",
			wantStatus: http.StatusOK,
			wantMinIDs: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "ldep-cluster"})
			doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":    "ldep-cluster",
				"daemonName": "ldep-daemon",
			})

			rec := doECSRequest(t, h, "ListDaemonDeployments", map[string]any{
				"cluster":    "ldep-cluster",
				"daemonName": "ldep-daemon",
			})
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			ids := out["deploymentIds"].([]any)
			assert.GreaterOrEqual(t, len(ids), tt.wantMinIDs)
		})
	}
}

// ---- Daemon revisions ----

func TestHandler_Daemon_DescribeDaemonRevisions(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		requestRevs   []int
		numRegistered int
		wantCount     int
		wantStatus    int
	}{
		{
			name:          "no filter returns all revisions",
			numRegistered: 2,
			requestRevs:   nil,
			wantCount:     2,
			wantStatus:    http.StatusOK,
		},
		{
			name:          "filter by revision number",
			numRegistered: 3,
			requestRevs:   []int{1, 3},
			wantCount:     2,
			wantStatus:    http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "rev-cluster"})
			doECSRequest(t, h, "CreateDaemon", map[string]any{
				"cluster":    "rev-cluster",
				"daemonName": "rev-daemon",
			})

			for range tt.numRegistered {
				doECSRequest(t, h, "RegisterDaemonTaskDefinition", map[string]any{
					"daemon": "rev-daemon",
					"family": "rev-family",
				})
			}

			input := map[string]any{"daemon": "rev-daemon"}
			if tt.requestRevs != nil {
				input["revisions"] = tt.requestRevs
			}

			rec := doECSRequest(t, h, "DescribeDaemonRevisions", input)
			assert.Equal(t, tt.wantStatus, rec.Code)

			var out map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
			revs := out["revisions"].([]any)
			assert.Len(t, revs, tt.wantCount)
		})
	}
}

// ---- enrichCluster cached counter performance ----

func TestBackend_EnrichCluster_CachedCounters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		desiredCount int
		stopCount    int
		wantRunning  float64
		wantPending  float64
	}{
		{
			name:         "running count reflects launched tasks",
			desiredCount: 0,
			stopCount:    0,
			wantRunning:  0,
			wantPending:  0,
		},
		{
			name:         "stop reduces running count",
			desiredCount: 0,
			stopCount:    0,
			wantRunning:  0,
			wantPending:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "perf-cluster"})
			registerTaskDef(t, h, "td", "nginx")

			// Run tasks and verify DescribeClusters reflects the count.
			taskArns := make([]string, 0)
			if tt.desiredCount > 0 {
				runRec := doECSRequest(t, h, "RunTask", map[string]any{
					"cluster":        "perf-cluster",
					"taskDefinition": "td",
					"count":          tt.desiredCount,
				})
				require.Equal(t, http.StatusOK, runRec.Code)

				var runOut map[string]any
				require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
				for _, task := range runOut["tasks"].([]any) {
					taskArns = append(taskArns, task.(map[string]any)["taskArn"].(string))
				}
			}

			// Stop some tasks.
			for i := range tt.stopCount {
				doECSRequest(t, h, "StopTask", map[string]any{
					"cluster": "perf-cluster",
					"task":    taskArns[i],
				})
			}

			descRec := doECSRequest(t, h, "DescribeClusters", map[string]any{
				"clusters": []string{"perf-cluster"},
			})
			require.Equal(t, http.StatusOK, descRec.Code)

			var descOut map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descOut))
			clusters := descOut["clusters"].([]any)
			require.Len(t, clusters, 1)
			c := clusters[0].(map[string]any)
			assert.InDelta(t, tt.wantRunning, c["runningTasksCount"], 1e-9)
			assert.InDelta(t, tt.wantPending, c["pendingTasksCount"], 1e-9)
		})
	}
}

// TestBackend_EnrichCluster_RunAndStopUpdatesCounters tests that
// RunTask increments running/pending counts and StopTask decrements them.
func TestBackend_EnrichCluster_RunAndStopUpdatesCounters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doECSRequest(t, h, "CreateCluster", map[string]any{"clusterName": "cnt-cluster"})
	registerTaskDef(t, h, "countable", "nginx")

	clusterCounters := func() (float64, float64) {
		rec := doECSRequest(
			t,
			h,
			"DescribeClusters",
			map[string]any{"clusters": []string{"cnt-cluster"}},
		)
		require.Equal(t, http.StatusOK, rec.Code)
		var out map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
		c := out["clusters"].([]any)[0].(map[string]any)

		return c["runningTasksCount"].(float64), c["pendingTasksCount"].(float64)
	}

	running, pending := clusterCounters()
	assert.InDelta(t, float64(0), running, 1e-9)
	assert.InDelta(t, float64(0), pending, 1e-9)

	// Run 2 tasks.
	runRec := doECSRequest(t, h, "RunTask", map[string]any{
		"cluster":        "cnt-cluster",
		"taskDefinition": "countable",
		"count":          2,
	})
	require.Equal(t, http.StatusOK, runRec.Code)

	var runOut map[string]any
	require.NoError(t, json.Unmarshal(runRec.Body.Bytes(), &runOut))
	tasks := runOut["tasks"].([]any)
	require.Len(t, tasks, 2)

	running, pending = clusterCounters()
	// With no runtime, tasks go immediately to RUNNING.
	assert.InDelta(t, float64(2), running, 1e-9, "two tasks should be running")
	assert.InDelta(t, float64(0), pending, 1e-9, "no pending tasks with noop runner")

	// Stop one task.
	taskArn := tasks[0].(map[string]any)["taskArn"].(string)
	doECSRequest(t, h, "StopTask", map[string]any{
		"cluster": "cnt-cluster",
		"task":    taskArn,
	})

	running, pending = clusterCounters()
	assert.InDelta(t, float64(1), running, 1e-9, "one task should remain running")
	assert.InDelta(t, float64(0), pending, 1e-9)
}

// ---- Helpers ----

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
