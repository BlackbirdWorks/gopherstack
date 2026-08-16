package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dms"
)

// TestReloadTables verifies ReloadTables validates its required fields, 404s
// on an unknown ReplicationTaskArn, rejects a task that is not RUNNING with
// InvalidResourceStateFault (matching real AWS: "You can only use this
// operation with a task in the RUNNING state"), and succeeds once running.
func TestReloadTables(t *testing.T) {
	t.Parallel()

	tablesToReload := []map[string]any{{"SchemaName": "public", "TableName": "orders"}}

	t.Run("missing ReplicationTaskArn is a validation error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadTables", map[string]any{"TablesToReload": tablesToReload})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing TablesToReload is a validation error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadTables", map[string]any{"ReplicationTaskArn": "rt-arn"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("invalid ReloadOption is a validation error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadTables", map[string]any{
			"ReplicationTaskArn": "rt-arn",
			"TablesToReload":     tablesToReload,
			"ReloadOption":       "bogus",
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown task is not found", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadTables", map[string]any{
			"ReplicationTaskArn": "arn:aws:dms:us-east-1:123456789012:task:does-not-exist",
			"TablesToReload":     tablesToReload,
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("task not running is an invalid state error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		h.Backend.AddReplicationInstanceInternal("rlt-ri", "dms.t3.medium")
		h.Backend.AddEndpointInternal("rlt-src", "source", "mysql")
		h.Backend.AddEndpointInternal("rlt-tgt", "target", "postgres")
		h.Backend.AddReplicationTaskInternal("rlt-task", "rlt-src", "rlt-tgt", "rlt-ri", "full-load")

		taskArn := describeTaskArn(t, h, "rlt-task")

		rec := doDMS(t, h, "ReloadTables", map[string]any{
			"ReplicationTaskArn": taskArn,
			"TablesToReload":     tablesToReload,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("running task reloads successfully", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		h.Backend.AddReplicationInstanceInternal("rlt2-ri", "dms.t3.medium")
		h.Backend.AddEndpointInternal("rlt2-src", "source", "mysql")
		h.Backend.AddEndpointInternal("rlt2-tgt", "target", "postgres")
		h.Backend.AddReplicationTaskInternal("rlt2-task", "rlt2-src", "rlt2-tgt", "rlt2-ri", "full-load")

		taskArn := describeTaskArn(t, h, "rlt2-task")

		startRec := doDMS(t, h, "StartReplicationTask", map[string]any{"ReplicationTaskArn": taskArn})
		require.Equal(t, http.StatusOK, startRec.Code)

		rec := doDMS(t, h, "ReloadTables", map[string]any{
			"ReplicationTaskArn": taskArn,
			"TablesToReload":     tablesToReload,
			"ReloadOption":       "data-reload",
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, taskArn, parseJSON(t, rec)["ReplicationTaskArn"])
	})
}

// TestReloadReplicationTables verifies ReloadReplicationTables uses
// ReplicationConfigArn (not ReplicationTaskArn -- a previous implementation
// used the wrong field name, silently discarding the client's ARN), 404s on
// an unknown config, rejects a non-RUNNING replication with
// InvalidResourceStateFault, and succeeds once running.
func TestReloadReplicationTables(t *testing.T) {
	t.Parallel()

	tablesToReload := []map[string]any{{"SchemaName": "public", "TableName": "orders"}}

	t.Run("missing ReplicationConfigArn is a validation error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadReplicationTables", map[string]any{"TablesToReload": tablesToReload})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("missing TablesToReload is a validation error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadReplicationTables", map[string]any{"ReplicationConfigArn": "rc-arn"})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("unknown config is not found", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "ReloadReplicationTables", map[string]any{
			"ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config:does-not-exist",
			"TablesToReload":       tablesToReload,
		})
		assert.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("config not running is an invalid state error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rcArn := createServerlessConfig(t, h, "rrt")

		rec := doDMS(t, h, "ReloadReplicationTables", map[string]any{
			"ReplicationConfigArn": rcArn,
			"TablesToReload":       tablesToReload,
		})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("running replication reloads successfully", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rcArn := createServerlessConfig(t, h, "rrt2")

		startRec := doDMS(t, h, "StartReplication", map[string]any{
			"ReplicationConfigArn": rcArn,
			"StartReplicationType": "start-replication",
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		rec := doDMS(t, h, "ReloadReplicationTables", map[string]any{
			"ReplicationConfigArn": rcArn,
			"TablesToReload":       tablesToReload,
		})
		require.Equal(t, http.StatusOK, rec.Code)
		assert.Equal(t, rcArn, parseJSON(t, rec)["ReplicationConfigArn"])
	})
}

// describeTaskArn resolves a replication task's ARN from its identifier via
// DescribeReplicationTasks.
func describeTaskArn(t *testing.T, h *dms.Handler, identifier string) string {
	t.Helper()

	rec := doDMS(t, h, "DescribeReplicationTasks", map[string]any{
		"Filters": []map[string]any{{"Name": "replication-task-id", "Values": []string{identifier}}},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	tasks := parseJSON(t, rec)["ReplicationTasks"].([]any)
	require.Len(t, tasks, 1)

	return tasks[0].(map[string]any)["ReplicationTaskArn"].(string)
}

// createServerlessConfig creates source/target endpoints and a DMS
// Serverless ReplicationConfig, returning its ARN.
func createServerlessConfig(t *testing.T, h *dms.Handler, prefix string) string {
	t.Helper()

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": prefix + "-src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": prefix + "-tgt",
		"EndpointType":       "target",
		"EngineName":         "postgres",
	})
	require.Equal(t, http.StatusOK, tgtRec.Code)
	tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	rcRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": prefix + "-rc",
		"ReplicationType":             "full-load-and-cdc",
		"SourceEndpointArn":           srcArn,
		"TargetEndpointArn":           tgtArn,
		"TableMappings":               "{}",
		"ComputeConfig":               map[string]any{},
	})
	require.Equal(t, http.StatusOK, rcRec.Code)

	return parseJSON(t, rcRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)
}
