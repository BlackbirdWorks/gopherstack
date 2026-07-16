package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

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
