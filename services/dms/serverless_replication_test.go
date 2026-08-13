package dms_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test_StartStopReplication_Lifecycle exercises the DMS Serverless
// replication lifecycle: CreateReplicationConfig -> StartReplication ->
// DescribeReplications -> StopReplication. Before this fix, StartReplication
// and StopReplication were disguised no-ops (always returned an empty
// envelope without touching backend state or validating the config exists),
// and DescribeReplications always returned an empty list regardless of any
// replication ever started.
func Test_StartStopReplication_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestDMSHandler()

	srcRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "sr-src",
		"EndpointType":       "source",
		"EngineName":         "mysql",
	})
	require.Equal(t, http.StatusOK, srcRec.Code)
	srcArn := parseJSON(t, srcRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	tgtRec := doDMS(t, h, "CreateEndpoint", map[string]any{
		"EndpointIdentifier": "sr-tgt",
		"EndpointType":       "target",
		"EngineName":         "postgres",
	})
	require.Equal(t, http.StatusOK, tgtRec.Code)
	tgtArn := parseJSON(t, tgtRec)["Endpoint"].(map[string]any)["EndpointArn"].(string)

	rcRec := doDMS(t, h, "CreateReplicationConfig", map[string]any{
		"ReplicationConfigIdentifier": "sr-rc",
		"ReplicationType":             "full-load-and-cdc",
		"SourceEndpointArn":           srcArn,
		"TargetEndpointArn":           tgtArn,
		"TableMappings":               "{}",
		"ComputeConfig":               map[string]any{},
	})
	require.Equal(t, http.StatusOK, rcRec.Code)
	rcArn := parseJSON(t, rcRec)["ReplicationConfig"].(map[string]any)["ReplicationConfigArn"].(string)

	// Starting a replication for a config that doesn't exist is a NotFound.
	notFoundRec := doDMS(t, h, "StartReplication", map[string]any{
		"ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config:does-not-exist",
		"StartReplicationType": "start-replication",
	})
	assert.Equal(t, http.StatusNotFound, notFoundRec.Code)

	// An invalid StartReplicationType is a validation error.
	badTypeRec := doDMS(t, h, "StartReplication", map[string]any{
		"ReplicationConfigArn": rcArn,
		"StartReplicationType": "bogus-type",
	})
	assert.Equal(t, http.StatusBadRequest, badTypeRec.Code)

	// DescribeReplications reports the config's Replication resource as
	// "created" as soon as the config exists, even before it is ever started
	// (mirroring real AWS: CreateReplicationConfig implicitly creates the
	// associated Replication runtime resource in "created" status).
	preStartDescRec := doDMS(t, h, "DescribeReplications", map[string]any{})
	require.Equal(t, http.StatusOK, preStartDescRec.Code)
	preStart := parseJSON(t, preStartDescRec)["Replications"].([]any)
	require.Len(t, preStart, 1)
	assert.Equal(t, "created", preStart[0].(map[string]any)["Status"])

	// Start the replication.
	startRec := doDMS(t, h, "StartReplication", map[string]any{
		"ReplicationConfigArn": rcArn,
		"StartReplicationType": "start-replication",
	})
	require.Equal(t, http.StatusOK, startRec.Code)
	startedRepl := parseJSON(t, startRec)["Replication"].(map[string]any)
	assert.Equal(t, rcArn, startedRepl["ReplicationConfigArn"])
	assert.Equal(t, "sr-rc", startedRepl["ReplicationConfigIdentifier"])
	assert.Equal(t, "running", startedRepl["Status"])
	assert.Equal(t, "start-replication", startedRepl["StartReplicationType"])
	assert.Equal(t, srcArn, startedRepl["SourceEndpointArn"])
	assert.Equal(t, tgtArn, startedRepl["TargetEndpointArn"])

	// Starting an already-running replication is an invalid-state error.
	alreadyRunningRec := doDMS(t, h, "StartReplication", map[string]any{
		"ReplicationConfigArn": rcArn,
		"StartReplicationType": "start-replication",
	})
	assert.Equal(t, http.StatusBadRequest, alreadyRunningRec.Code)

	// DescribeReplications must now report the running replication -- this is
	// real backend state, not merely echoed by StartReplication.
	postStartDescRec := doDMS(t, h, "DescribeReplications", map[string]any{})
	require.Equal(t, http.StatusOK, postStartDescRec.Code)
	replications := parseJSON(t, postStartDescRec)["Replications"].([]any)
	require.Len(t, replications, 1)
	assert.Equal(t, "running", replications[0].(map[string]any)["Status"])

	// DescribeReplications filtered by replication-config-arn.
	filteredDescRec := doDMS(t, h, "DescribeReplications", map[string]any{
		"Filters": []map[string]any{
			{"Name": "replication-config-arn", "Values": []string{rcArn}},
		},
	})
	require.Equal(t, http.StatusOK, filteredDescRec.Code)
	filtered := parseJSON(t, filteredDescRec)["Replications"].([]any)
	require.Len(t, filtered, 1)

	// Stop the replication.
	stopRec := doDMS(t, h, "StopReplication", map[string]any{
		"ReplicationConfigArn": rcArn,
	})
	require.Equal(t, http.StatusOK, stopRec.Code)
	stoppedRepl := parseJSON(t, stopRec)["Replication"].(map[string]any)
	assert.Equal(t, "stopped", stoppedRepl["Status"])

	// Stopping an already-stopped replication is an invalid-state error.
	alreadyStoppedRec := doDMS(t, h, "StopReplication", map[string]any{
		"ReplicationConfigArn": rcArn,
	})
	assert.Equal(t, http.StatusBadRequest, alreadyStoppedRec.Code)

	// Stopping a replication for a nonexistent config is NotFound.
	stopNotFoundRec := doDMS(t, h, "StopReplication", map[string]any{
		"ReplicationConfigArn": "arn:aws:dms:us-east-1:123456789012:replication-config:does-not-exist",
	})
	assert.Equal(t, http.StatusNotFound, stopNotFoundRec.Code)

	// The stopped status must also be visible via DescribeReplications.
	postStopDescRec := doDMS(t, h, "DescribeReplications", map[string]any{})
	require.Equal(t, http.StatusOK, postStopDescRec.Code)
	postStop := parseJSON(t, postStopDescRec)["Replications"].([]any)
	require.Len(t, postStop, 1)
	assert.Equal(t, "stopped", postStop[0].(map[string]any)["Status"])
}

// Test_StartReplication_RequiredFields verifies ValidationException is
// returned when required input fields are missing, matching the real
// StartReplicationInput shape (ReplicationConfigArn and StartReplicationType
// are both "This member is required.").
func Test_StartReplication_RequiredFields(t *testing.T) {
	t.Parallel()

	cases := []struct {
		body map[string]any
		name string
	}{
		{name: "missing ReplicationConfigArn", body: map[string]any{"StartReplicationType": "start-replication"}},
		{name: "missing StartReplicationType", body: map[string]any{"ReplicationConfigArn": "rc-arn"}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestDMSHandler()
			rec := doDMS(t, h, "StartReplication", tc.body)
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// Test_StartRecommendations verifies that StartRecommendations, previously a
// disguised no-op that ignored its DatabaseId input and never touched
// backend state, now records a recommendation visible via
// DescribeRecommendations.
func Test_StartRecommendations(t *testing.T) {
	t.Parallel()

	t.Run("missing DatabaseId is a validation error", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()
		rec := doDMS(t, h, "StartRecommendations", map[string]any{})
		assert.Equal(t, http.StatusBadRequest, rec.Code)
	})

	t.Run("recorded recommendation is visible via DescribeRecommendations", func(t *testing.T) {
		t.Parallel()

		h := newTestDMSHandler()

		preRec := doDMS(t, h, "DescribeRecommendations", map[string]any{})
		require.Equal(t, http.StatusOK, preRec.Code)
		assert.Empty(t, parseJSON(t, preRec)["Recommendations"])

		startRec := doDMS(t, h, "StartRecommendations", map[string]any{
			"DatabaseId": "fa-db-1",
			"Settings":   map[string]any{},
		})
		require.Equal(t, http.StatusOK, startRec.Code)

		postRec := doDMS(t, h, "DescribeRecommendations", map[string]any{})
		require.Equal(t, http.StatusOK, postRec.Code)
		recs := parseJSON(t, postRec)["Recommendations"].([]any)
		require.Len(t, recs, 1)
		assert.Equal(t, "fa-db-1", recs[0].(map[string]any)["DatabaseId"])
	})
}
