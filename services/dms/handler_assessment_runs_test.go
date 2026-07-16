package dms_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dms"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAssessmentRun_Lifecycle(t *testing.T) {
	t.Parallel()

	// Helper to build RI + endpoints + task.
	setupTask := func(t *testing.T, h *dms.Handler, prefix string) string {
		t.Helper()

		riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": prefix + "-ri",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, riRec.Code)
		riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

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
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, tgtRec.Code)
		tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": prefix + "-task",
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         tgtArn,
			"ReplicationInstanceArn":    riArn,
			"MigrationType":             "full-load",
		})
		require.Equal(t, http.StatusOK, taskRec.Code)

		return parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)
	}

	t.Run("start_nonexistent_task_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "StartReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskArn":   "arn:aws:dms:us-east-1:123:task:nonexistent",
			"ServiceAccessRoleArn": "arn:aws:iam::123:role/role",
			"ResultLocationBucket": "my-bucket",
			"AssessmentRunName":    "test-run",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)
	})

	t.Run("start_stores_run_describable_deletable", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		taskArn := setupTask(t, h, "ar-lifecycle")

		// Start assessment run.
		startRec := doDMS(t, h, "StartReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskArn":   taskArn,
			"ServiceAccessRoleArn": "arn:aws:iam::123:role/role",
			"ResultLocationBucket": "my-bucket",
			"AssessmentRunName":    "my-run",
		})
		require.Equal(t, http.StatusOK, startRec.Code)
		runBody := parseJSON(t, startRec)["ReplicationTaskAssessmentRun"].(map[string]any)
		runArn, _ := runBody["ReplicationTaskAssessmentRunArn"].(string)
		assert.NotEmpty(t, runArn, "assessment run ARN must be non-empty")

		// DescribeReplicationTaskAssessmentRuns must return it.
		descRec := doDMS(t, h, "DescribeReplicationTaskAssessmentRuns", map[string]any{})
		require.Equal(t, http.StatusOK, descRec.Code)
		runs := parseJSON(t, descRec)["ReplicationTaskAssessmentRuns"].([]any)
		assert.Len(t, runs, 1)

		// DeleteReplicationTaskAssessmentRun must succeed.
		delRec := doDMS(t, h, "DeleteReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": runArn,
		})
		require.Equal(t, http.StatusOK, delRec.Code)

		// Second delete must return 404.
		del2Rec := doDMS(t, h, "DeleteReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": runArn,
		})
		require.Equal(t, http.StatusNotFound, del2Rec.Code)
	})

	t.Run("cancel_existing_run_succeeds", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		taskArn := setupTask(t, h, "ar-cancel")

		startRec := doDMS(t, h, "StartReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskArn":   taskArn,
			"ServiceAccessRoleArn": "arn:aws:iam::123:role/role",
			"ResultLocationBucket": "bucket",
			"AssessmentRunName":    "cancel-run",
		})
		require.Equal(t, http.StatusOK, startRec.Code)
		runBody2 := parseJSON(t, startRec)["ReplicationTaskAssessmentRun"].(map[string]any)
		runArn, _ := runBody2["ReplicationTaskAssessmentRunArn"].(string)

		cancelRec := doDMS(t, h, "CancelReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": runArn,
		})
		require.Equal(t, http.StatusOK, cancelRec.Code)
	})

	t.Run("cancel_nonexistent_run_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "CancelReplicationTaskAssessmentRun", map[string]any{
			"ReplicationTaskAssessmentRunArn": "arn:aws:dms:us-east-1:123:assessment-run:nonexistent",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)
	})
}

func TestStartReplicationTaskAssessment(t *testing.T) {
	t.Parallel()

	t.Run("not_found_returns_404", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "StartReplicationTaskAssessment", map[string]any{
			"ReplicationTaskArn": "arn:aws:dms:us-east-1:123:task:nonexistent",
		})
		require.Equal(t, http.StatusNotFound, rec.Code)

		var body map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
		assert.Equal(t, "ResourceNotFoundFault", body["__type"])
	})

	t.Run("returns_task_on_success", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()

		riRec := doDMS(t, h, "CreateReplicationInstance", map[string]any{
			"ReplicationInstanceIdentifier": "assess-ri",
			"ReplicationInstanceClass":      "dms.t3.medium",
		})
		require.Equal(t, http.StatusOK, riRec.Code)
		riArn := parseJSON(t, riRec)["ReplicationInstance"].(map[string]any)["ReplicationInstanceArn"].(string)

		srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "assess-src",
			"EndpointType":       "source",
			"EngineName":         "mysql",
		})
		require.Equal(t, http.StatusOK, srcRec.Code)
		srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
			"EndpointIdentifier": "assess-tgt",
			"EndpointType":       "target",
			"EngineName":         "s3",
		})
		require.Equal(t, http.StatusOK, tgtRec.Code)
		tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

		taskRec := doDMS(t, h, "CreateReplicationTask", map[string]any{
			"ReplicationTaskIdentifier": "assess-task",
			"SourceEndpointArn":         srcArn,
			"TargetEndpointArn":         tgtArn,
			"ReplicationInstanceArn":    riArn,
			"MigrationType":             "full-load",
		})
		require.Equal(t, http.StatusOK, taskRec.Code)
		taskArn := parseJSON(t, taskRec)["ReplicationTask"].(map[string]any)["ReplicationTaskArn"].(string)

		assessRec := doDMS(t, h, "StartReplicationTaskAssessment", map[string]any{
			"ReplicationTaskArn": taskArn,
		})
		require.Equal(t, http.StatusOK, assessRec.Code)

		rt := parseJSON(t, assessRec)["ReplicationTask"].(map[string]any)
		assert.Equal(t, taskArn, rt["ReplicationTaskArn"],
			"StartReplicationTaskAssessment must return the actual task ARN")
		// Status must not be the old hardcoded "test-failed".
		assert.NotEqual(t, "test-failed", rt["Status"],
			"StartReplicationTaskAssessment must not return test-failed as initial status")
	})
}
