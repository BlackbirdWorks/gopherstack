package dms_test

import (
	"encoding/json"
	"net/http"
	"strconv"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestModifyReplicationTask(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("mt-ri", "dms.t3.medium")
	h.Backend.AddEndpointInternal("mt-src", "source", "mysql")
	h.Backend.AddEndpointInternal("mt-tgt", "target", "postgres")
	h.Backend.AddReplicationTaskInternal("mt-task", "mt-src", "mt-tgt", "mt-ri", "full-load")

	descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	tasks := parseJSON(t, descRec)["ReplicationTasks"].([]any)
	require.Len(t, tasks, 1)
	taskArn := tasks[0].(map[string]any)["ReplicationTaskArn"].(string)

	rec := doDMS(t, h, "ModifyReplicationTask", map[string]any{
		"ReplicationTaskArn": taskArn,
		"MigrationType":      "cdc",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "ModifyReplicationTask", map[string]any{
		"ReplicationTaskArn": "arn:nonexistent",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestMoveReplicationTask(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	h.Backend.AddReplicationInstanceInternal("move-ri-src", "dms.t3.medium")
	h.Backend.AddReplicationInstanceInternal("move-ri-tgt", "dms.t3.medium")
	h.Backend.AddEndpointInternal("move-src", "source", "mysql")
	h.Backend.AddEndpointInternal("move-tgt", "target", "postgres")
	h.Backend.AddReplicationTaskInternal("move-task", "move-src", "move-tgt", "move-ri-src", "full-load")

	descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
	require.Equal(t, http.StatusOK, descRec.Code)
	tasks := parseJSON(t, descRec)["ReplicationTasks"].([]any)
	require.Len(t, tasks, 1)
	taskArn := tasks[0].(map[string]any)["ReplicationTaskArn"].(string)

	rec := doDMS(t, h, "MoveReplicationTask", map[string]any{
		"ReplicationTaskArn":           taskArn,
		"TargetReplicationInstanceArn": "arn:aws:dms:us-east-1:123:rep:move-ri-tgt",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec2 := doDMS(t, h, "MoveReplicationTask", map[string]any{
		"ReplicationTaskArn":           "nonexistent",
		"TargetReplicationInstanceArn": "arn:aws:dms:us-east-1:123:rep:move-ri-tgt",
	})
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

func TestCreateReplicationTask_ARNValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		omitSource     bool
		omitTarget     bool
		omitInstance   bool
		badSourceArn   bool
		badTargetArn   bool
		badInstanceArn bool
	}{
		{name: "nonexistent_source_endpoint", badSourceArn: true},
		{name: "nonexistent_target_endpoint", badTargetArn: true},
		{name: "nonexistent_replication_instance", badInstanceArn: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "arn-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, riRec.Code)
			riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

			srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "arn-src",
				"EndpointType":       "source",
				"EngineName":         "mysql",
			})
			require.Equal(t, http.StatusOK, srcRec.Code)
			srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "arn-tgt",
				"EndpointType":       "target",
				"EngineName":         "s3",
			})
			require.Equal(t, http.StatusOK, tgtRec.Code)
			tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			useSrcArn := srcArn
			useTgtArn := tgtArn
			useRiArn := riArn

			if tt.badSourceArn {
				useSrcArn = "arn:aws:dms:us-east-1:123:endpoint:nonexistent-src"
			}

			if tt.badTargetArn {
				useTgtArn = "arn:aws:dms:us-east-1:123:endpoint:nonexistent-tgt"
			}

			if tt.badInstanceArn {
				useRiArn = "arn:aws:dms:us-east-1:123:rep:nonexistent-ri"
			}

			rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "arn-task",
				"SourceEndpointArn":         useSrcArn,
				"TargetEndpointArn":         useTgtArn,
				"ReplicationInstanceArn":    useRiArn,
				"MigrationType":             "full-load",
			})

			require.Equal(t, http.StatusNotFound, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "ResourceNotFoundFault", body["__type"],
				"non-existent ARN in CreateReplicationTask must return ResourceNotFoundFault")
		})
	}
}

func TestCreateReplicationTask_InvalidMigrationType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		migrationType string
	}{
		{name: "bad_type", migrationType: "bad-type"},
		{name: "empty_after_required_check", migrationType: "full_load"},
		{name: "cdc_caps", migrationType: "CDC"},
		{name: "unknown", migrationType: "incremental"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "task-1",
				"SourceEndpointArn":         "arn:aws:dms:us-east-1:123:endpoint:src",
				"TargetEndpointArn":         "arn:aws:dms:us-east-1:123:endpoint:tgt",
				"ReplicationInstanceArn":    "arn:aws:dms:us-east-1:123:rep:ri",
				"MigrationType":             tt.migrationType,
			})

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "ValidationException", body["__type"],
				"invalid MigrationType must return ValidationException")
		})
	}
}

func TestStopReplicationTask_NotRunning(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		stop bool // whether to stop it before trying a second stop
	}{
		{name: "stop_ready_task", stop: false},
		{name: "stop_already_stopped_task", stop: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()

			riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
				"ReplicationInstanceIdentifier": "stop-ri",
				"ReplicationInstanceClass":      "dms.t3.medium",
			})
			require.Equal(t, http.StatusOK, riRec.Code)
			riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

			srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "stop-src",
				"EndpointType":       "source",
				"EngineName":         "mysql",
			})
			require.Equal(t, http.StatusOK, srcRec.Code)
			srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
				"EndpointIdentifier": "stop-tgt",
				"EndpointType":       "target",
				"EngineName":         "s3",
			})
			require.Equal(t, http.StatusOK, tgtRec.Code)
			tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

			taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "stop-task",
				"SourceEndpointArn":         srcArn,
				"TargetEndpointArn":         tgtArn,
				"ReplicationInstanceArn":    riArn,
				"MigrationType":             "full-load",
			})
			require.Equal(t, http.StatusOK, taskRec.Code)
			taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

			if tt.stop {
				// Start then stop to put it in stopped state.
				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				stopRec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				require.Equal(t, http.StatusOK, stopRec.Code)
			}

			// Stop a non-running task (ready or already stopped) — must fail.
			rec := doDMS(t, h, "StopReplicationTask", map[string]any{
				"ReplicationTaskArn": taskArn,
			})

			require.Equal(t, http.StatusBadRequest, rec.Code)

			var body map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, "InvalidResourceStateFault", body["__type"],
				"stopping a non-running task must return InvalidResourceStateFault")
		})
	}
}

func TestHandler_ReplicationTaskCRUD(t *testing.T) {
	t.Parallel()

	// Helper: create a fully wired environment with an instance and two endpoints.
	createTaskEnv := func(t *testing.T, h *dms.Handler) (string, string, string) {
		t.Helper()

		instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "task-inst",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, instRec.Code)
		instResp := parseJSON(t, instRec)
		ri := instResp["ReplicationInstance"].(map[string]any)
		theInstArn := ri["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "task-src",
			"EndpointType":       "SOURCE",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcResp := parseJSON(t, srcRec)
		theSrcArn := srcResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

		dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "task-dst",
			"EndpointType":       "TARGET",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, dstRec.Code)
		dstResp := parseJSON(t, dstRec)
		theDstArn := dstResp["Endpoint"].(map[string]any)["EndpointArn"].(string)

		return theSrcArn, theDstArn, theInstArn
	}

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "create_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "my-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
					"TableMappings":             `{"rules":[]}`,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				rt, ok := resp["ReplicationTask"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, "my-task", rt["ReplicationTaskIdentifier"])
				assert.Equal(t, "ready", rt["Status"])
				assert.NotEmpty(t, rt["ReplicationTaskArn"])
			},
		},
		{
			name: "create_duplicate_conflict",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				body := map[string]any{
					"ReplicationTaskIdentifier": "dup-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				}
				doDMS(t, h, "CreateReplicationTask", body)
				rec := doDMS(t, h, "CreateReplicationTask", body)
				assert.Equal(t, http.StatusConflict, rec.Code)
			},
		},
		{
			name: "describe_all",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "list-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				assert.Equal(t, http.StatusOK, rec.Code)
				resp := parseJSON(t, rec)
				list, ok := resp["ReplicationTasks"].([]any)
				require.True(t, ok)
				assert.Len(t, list, 1)
			},
		},
		{
			name: "start_and_stop",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				create := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "ss-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				taskArn := createResp["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				assert.Equal(t, http.StatusOK, startRec.Code)
				startResp := parseJSON(t, startRec)
				rt := startResp["ReplicationTask"].(map[string]any)
				assert.Equal(t, "running", rt["Status"])

				stopRec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, stopRec.Code)
				stopResp := parseJSON(t, stopRec)
				rtStop := stopResp["ReplicationTask"].(map[string]any)
				assert.Equal(t, "stopped", rtStop["Status"])
			},
		},
		{
			name: "delete_success",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				create := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "del-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
					"MigrationType":             "full-load",
				})
				require.Equal(t, http.StatusOK, create.Code)
				createResp := parseJSON(t, create)
				taskArn := createResp["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

				rec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, rec.Code)
			},
		},
		{
			name: "create_missing_identifier",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"SourceEndpointArn":      srcArn,
					"TargetEndpointArn":      dstArn,
					"ReplicationInstanceArn": instArn,
					"MigrationType":          "full-load",
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "create_missing_migration_type",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				srcArn, dstArn, instArn := createTaskEnv(t, h)
				rec := doDMS(t, h, "CreateReplicationTask", map[string]any{
					"ReplicationTaskIdentifier": "no-type-task",
					"SourceEndpointArn":         srcArn,
					"TargetEndpointArn":         dstArn,
					"ReplicationInstanceArn":    instArn,
				})
				assert.Equal(t, http.StatusBadRequest, rec.Code)
			},
		},
		{
			name: "stop_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": "arn:aws:dms:us-east-1:000000000000:task:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
		{
			name: "delete_not_found",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				rec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": "arn:aws:dms:us-east-1:000000000000:task:nonexistent",
				})
				assert.Equal(t, http.StatusNotFound, rec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_DescribeTasksByArn(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "arn-filter-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, instRec.Code)
	instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "arn-src",
		"EndpointType":       "SOURCE",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "arn-dst",
		"EndpointType":       "TARGET",
		"EngineName":         "s3",
	})
	require.Equal(t, http.StatusOK, dstRec.Code)
	dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
		"ReplicationTaskIdentifier": "arn-filter-task",
		"SourceEndpointArn":         srcArn,
		"TargetEndpointArn":         dstArn,
		"ReplicationInstanceArn":    instArn,
		"MigrationType":             "full-load",
	})
	require.Equal(t, http.StatusOK, taskRec.Code)
	taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

	// Filter by ARN.
	rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-task-arn", "Values": []string{taskArn}},
		},
	})
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := parseJSON(t, rec)
	list := resp["ReplicationTasks"].([]any)
	assert.Len(t, list, 1)
}

// TestDescribeReplicationTasksPagination verifies Marker/MaxRecords pagination.
func TestDescribeReplicationTasksPagination(t *testing.T) {
	t.Parallel()

	// Helper to create the prerequisite replication instance and endpoints.
	setupTaskEnv := func(t *testing.T, h *dms.Handler, n int) {
		t.Helper()

		instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "pg-inst",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, instRec.Code)
		instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "pg-src",
			"EndpointType":       "SOURCE",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "pg-dst",
			"EndpointType":       "TARGET",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, dstRec.Code)
		dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		for i := range n {
			doDMS(t, h, "CreateReplicationTask", map[string]any{
				"ReplicationTaskIdentifier": "task-" + strconv.Itoa(i),
				"SourceEndpointArn":         srcArn,
				"TargetEndpointArn":         dstArn,
				"ReplicationInstanceArn":    instArn,
				"MigrationType":             "full-load",
			})
		}
	}

	tests := []struct {
		name       string
		count      int
		maxRecords int
		wantCount  int
		wantMarker bool
	}{
		{
			name:       "first_page_limited",
			count:      3,
			maxRecords: 2,
			wantCount:  2,
			wantMarker: true,
		},
		{
			name:       "all_results_no_marker",
			count:      2,
			maxRecords: 10,
			wantCount:  2,
			wantMarker: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			setupTaskEnv(t, h, tt.count)

			rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{"MaxRecords": tt.maxRecords})
			require.Equal(t, http.StatusOK, rec.Code)

			resp := parseJSON(t, rec)
			list, ok := resp["ReplicationTasks"].([]any)
			require.True(t, ok)
			assert.Len(t, list, tt.wantCount)

			_, hasMarker := resp["Marker"]
			assert.Equal(t, tt.wantMarker, hasMarker)
		})
	}
}

// TestDescribeReplicationTasksContinuation verifies a two-page traversal.
func TestDescribeReplicationTasksContinuation(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	instRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
		"ReplicationInstanceIdentifier": "cont-inst",
		"ReplicationInstanceClass":      "dms.t3.medium",
	})
	require.Equal(t, http.StatusOK, instRec.Code)
	instArn := parseJSON(t, instRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "cont-src",
		"EndpointType":       "SOURCE",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	dstRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "cont-dst",
		"EndpointType":       "TARGET",
		"EngineName":         "s3",
	})
	require.Equal(t, http.StatusOK, dstRec.Code)
	dstArn := parseJSON(t, dstRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	for i := range 3 {
		doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": "task-" + strconv.Itoa(i),
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         dstArn,
			"ReplicationInstanceArn":    instArn,
			"MigrationType":             "full-load",
		})
	}

	rec1 := doDMS(t, h, "DescribeReplicationTasks", map[string]any{"MaxRecords": 2})
	require.Equal(t, http.StatusOK, rec1.Code)
	resp1 := parseJSON(t, rec1)
	page1, ok := resp1["ReplicationTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, page1, 2)

	marker, hasMarker := resp1["Marker"].(string)
	require.True(t, hasMarker)
	require.NotEmpty(t, marker)

	rec2 := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"MaxRecords": 2,
		"Marker":     marker,
	})
	require.Equal(t, http.StatusOK, rec2.Code)
	resp2 := parseJSON(t, rec2)
	page2, ok := resp2["ReplicationTasks"].([]any)
	require.True(t, ok)
	assert.Len(t, page2, 1)

	_, stillHasMarker := resp2["Marker"]
	assert.False(t, stillHasMarker)
}

func TestDescribeTasksFilterMiss(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()
	rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-task-id", "Values": []string{"nonexistent"}},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	body := parseJSON(t, rec)
	tasks := body["ReplicationTasks"].([]any)
	assert.Empty(t, tasks)
}

func TestHandler_DeleteRunningTaskFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "delete_running_task_rejected",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				h.Backend.AddReplicationInstanceInternal("sm-ri", "dms.t3.medium")
				h.Backend.AddEndpointInternal("sm-src", "source", "mysql")
				h.Backend.AddEndpointInternal("sm-tgt", "target", "s3")
				h.Backend.AddReplicationTaskInternal("sm-task", "sm-src", "sm-tgt", "sm-ri", "full-load")

				descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				taskArn := parseJSON(t, descRec)["ReplicationTasks"].([]any)[0].(map[string]any)["ReplicationTaskArn"].(string)

				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				delRec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusBadRequest, delRec.Code)
			},
		},
		{
			name: "delete_stopped_task_succeeds",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				h.Backend.AddReplicationInstanceInternal("sm2-ri", "dms.t3.medium")
				h.Backend.AddEndpointInternal("sm2-src", "source", "mysql")
				h.Backend.AddEndpointInternal("sm2-tgt", "target", "s3")
				h.Backend.AddReplicationTaskInternal("sm2-task", "sm2-src", "sm2-tgt", "sm2-ri", "full-load")

				descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				taskArn := parseJSON(t, descRec)["ReplicationTasks"].([]any)[0].(map[string]any)["ReplicationTaskArn"].(string)

				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				stopRec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				require.Equal(t, http.StatusOK, stopRec.Code)

				delRec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, delRec.Code)
			},
		},
		{
			name: "delete_ready_task_succeeds",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				h.Backend.AddReplicationInstanceInternal("sm3-ri", "dms.t3.medium")
				h.Backend.AddEndpointInternal("sm3-src", "source", "mysql")
				h.Backend.AddEndpointInternal("sm3-tgt", "target", "s3")
				h.Backend.AddReplicationTaskInternal("sm3-task", "sm3-src", "sm3-tgt", "sm3-ri", "full-load")

				descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				taskArn := parseJSON(t, descRec)["ReplicationTasks"].([]any)[0].(map[string]any)["ReplicationTaskArn"].(string)

				delRec := doDMS(t, h, "DeleteReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				assert.Equal(t, http.StatusOK, delRec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_ModifyRunningTaskFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, h *dms.Handler)
		name string
	}{
		{
			name: "modify_running_task_rejected",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				h.Backend.AddReplicationInstanceInternal("mrt-ri", "dms.t3.medium")
				h.Backend.AddEndpointInternal("mrt-src", "source", "mysql")
				h.Backend.AddEndpointInternal("mrt-tgt", "target", "s3")
				h.Backend.AddReplicationTaskInternal("mrt-task", "mrt-src", "mrt-tgt", "mrt-ri", "full-load")

				descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				taskArn := parseJSON(t, descRec)["ReplicationTasks"].([]any)[0].(map[string]any)["ReplicationTaskArn"].(string)

				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				modRec := doDMS(t, h, "ModifyReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
					"MigrationType":      "cdc",
				})
				assert.Equal(t, http.StatusBadRequest, modRec.Code)
			},
		},
		{
			name: "modify_stopped_task_succeeds",
			run: func(t *testing.T, h *dms.Handler) {
				t.Helper()
				h.Backend.AddReplicationInstanceInternal("mst-ri", "dms.t3.medium")
				h.Backend.AddEndpointInternal("mst-src", "source", "mysql")
				h.Backend.AddEndpointInternal("mst-tgt", "target", "s3")
				h.Backend.AddReplicationTaskInternal("mst-task", "mst-src", "mst-tgt", "mst-ri", "full-load")

				descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
				require.Equal(t, http.StatusOK, descRec.Code)
				taskArn := parseJSON(t, descRec)["ReplicationTasks"].([]any)[0].(map[string]any)["ReplicationTaskArn"].(string)

				startRec := doDMS(t, h, "StartReplicationTask", map[string]any{
					"ReplicationTaskArn":       taskArn,
					"StartReplicationTaskType": "start-replication",
				})
				require.Equal(t, http.StatusOK, startRec.Code)

				stopRec := doDMS(t, h, "StopReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
				})
				require.Equal(t, http.StatusOK, stopRec.Code)

				modRec := doDMS(t, h, "ModifyReplicationTask", map[string]any{
					"ReplicationTaskArn": taskArn,
					"MigrationType":      "cdc",
				})
				assert.Equal(t, http.StatusOK, modRec.Code)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			tt.run(t, h)
		})
	}
}

func TestHandler_StartReplicationTaskTypeValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		startType  string
		wantStatus int
	}{
		{
			name:       "start_replication_valid",
			startType:  "start-replication",
			wantStatus: http.StatusOK,
		},
		{
			name:       "resume_processing_valid",
			startType:  "resume-processing",
			wantStatus: http.StatusOK,
		},
		{
			name:       "reload_target_valid",
			startType:  "reload-target",
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_type_rejected",
			startType:  "invalid-type",
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "empty_type_defaults_to_start",
			startType:  "",
			wantStatus: http.StatusOK,
		},
	}

	setupEnv := func(t *testing.T, h *dms.Handler) string {
		t.Helper()
		h.Backend.AddReplicationInstanceInternal("srtt-ri", "dms.t3.medium")
		h.Backend.AddEndpointInternal("srtt-src", "source", "mysql")
		h.Backend.AddEndpointInternal("srtt-tgt", "target", "s3")
		h.Backend.AddReplicationTaskInternal("srtt-task", "srtt-src", "srtt-tgt", "srtt-ri", "full-load")
		descRec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{})
		require.Equal(t, http.StatusOK, descRec.Code)

		return parseJSON(t, descRec)["ReplicationTasks"].([]any)[0].(map[string]any)["ReplicationTaskArn"].(string)
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestDMSHandler()
			taskArn := setupEnv(t, h)

			body := map[string]any{"ReplicationTaskArn": taskArn}
			if tt.startType != "" {
				body["StartReplicationTaskType"] = tt.startType
			}

			rec := doDMS(t, h, "StartReplicationTask", body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}
