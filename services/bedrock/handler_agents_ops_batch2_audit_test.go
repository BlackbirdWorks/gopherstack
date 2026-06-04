package bedrock_test

// handler_agents_ops_batch2_audit_test.go — Bedrock Agent batch-2 ops AWS-accuracy audit (go-gduq).
// Covers three gaps in the Bedrock Agent ops layer:
//
//  1. StopIngestionJob missing state guard — AWS rejects stopping jobs not in
//     STARTING state (COMPLETE, STOPPED) with ValidationException.
//
//  2. DeleteAgent with active aliases — AWS returns ConflictException (409)
//     when the agent has one or more aliases; our handler silently deleted agents.
//
//  3. StartIngestionJob concurrent conflict — AWS returns ConflictException when
//     there is already a STARTING ingestion job for the same data source.

import (
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ── 1. StopIngestionJob state guard ──────────────────────────────────────────

func TestBatch2AgentOps_StopIngestionJob_AlreadyStopped_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "second_stop_fails"},
		{name: "rapid_double_stop"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestAgentsHandler(t)
			kbID, dsID := createKBAndDS(t, h)

			startRec := doAgentRequest(t, h, http.MethodPost,
				fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
				map[string]any{})
			require.Equal(t, http.StatusAccepted, startRec.Code)

			var started map[string]any
			require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
			jobID := started["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

			// First stop: must succeed (job is STARTING).
			stopPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop",
				kbID, dsID, jobID)
			rec1 := doAgentRequest(t, h, http.MethodPost, stopPath, nil)
			assert.Equal(t, http.StatusOK, rec1.Code, "first stop should succeed")

			// Second stop: must fail — job is now STOPPED.
			rec2 := doAgentRequest(t, h, http.MethodPost, stopPath, nil)
			assert.Equal(t, http.StatusBadRequest, rec2.Code,
				"stop of already-stopped ingestion job should return 400 ValidationException")

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
			assert.Equal(t, "ValidationException", errBody["__type"],
				"error type must be ValidationException")
		})
	}
}

func TestBatch2AgentOps_StopIngestionJob_Complete_Rejected(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	startRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{})
	require.Equal(t, http.StatusAccepted, startRec.Code)

	var started map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
	jobID := started["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	// Poll until job transitions to COMPLETE (ingestionCompleteDelay is 200ms).
	var jobStatus string
	for range 20 {
		time.Sleep(20 * time.Millisecond)

		getRec := doAgentRequest(t, h, http.MethodGet,
			fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s", kbID, dsID, jobID),
			nil)
		require.Equal(t, http.StatusOK, getRec.Code)

		var out map[string]any
		require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &out))
		jobStatus = out["ingestionJob"].(map[string]any)["status"].(string)

		if jobStatus == "COMPLETE" {
			break
		}
	}

	require.Equal(t, "COMPLETE", jobStatus, "job must have transitioned to COMPLETE")

	// Try to stop a COMPLETE job — must fail.
	stopRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop", kbID, dsID, jobID),
		nil)
	assert.Equal(t, http.StatusBadRequest, stopRec.Code,
		"stopping a COMPLETE ingestion job should return 400 ValidationException")

	var errBody map[string]any
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &errBody))
	assert.Equal(t, "ValidationException", errBody["__type"])
}

func TestBatch2AgentOps_StopIngestionJob_Starting_Succeeds(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	startRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{})
	require.Equal(t, http.StatusAccepted, startRec.Code)

	var started map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &started))
	jobID := started["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	// Stop the STARTING job — must succeed.
	stopRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop", kbID, dsID, jobID),
		nil)
	assert.Equal(t, http.StatusOK, stopRec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(stopRec.Body.Bytes(), &out))
	assert.Equal(t, "STOPPED", out["ingestionJob"].(map[string]any)["status"])
}

// ── 2. DeleteAgent with active aliases ───────────────────────────────────────

func TestBatch2AgentOps_DeleteAgent_WithActiveAlias_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "one_alias"},
		{name: "named_alias"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, b := newTestAgentsHandler(t)
			agent, err := b.CreateAgent("delete-agent-"+tc.name, "model", "", "", nil)
			require.NoError(t, err)

			// Create an alias for the agent.
			aliasRec := doAgentRequest(t, h, http.MethodPost,
				fmt.Sprintf("/agents/%s/aliases", agent.AgentID),
				map[string]any{"agentAliasName": "live"})
			require.Equal(t, http.StatusAccepted, aliasRec.Code)

			// Delete agent with active alias — must fail with ConflictException.
			delRec := doAgentRequest(t, h, http.MethodDelete,
				fmt.Sprintf("/agents/%s", agent.AgentID), nil)
			assert.Equal(t, http.StatusConflict, delRec.Code,
				"deleting agent with active aliases should return 409 ConflictException")

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(delRec.Body.Bytes(), &errBody))
			assert.Equal(t, "ConflictException", errBody["__type"])

			// Agent must still exist.
			getRec := doAgentRequest(t, h, http.MethodGet,
				fmt.Sprintf("/agents/%s", agent.AgentID), nil)
			assert.Equal(t, http.StatusOK, getRec.Code, "agent must still exist after rejected delete")
		})
	}
}

func TestBatch2AgentOps_DeleteAgent_NoAliases_Succeeds(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("delete-no-alias", "model", "", "", nil)
	require.NoError(t, err)

	// Delete agent with no aliases — must succeed.
	delRec := doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s", agent.AgentID), nil)
	assert.Equal(t, http.StatusAccepted, delRec.Code)

	// Agent must be gone.
	getRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/agents/%s", agent.AgentID), nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

func TestBatch2AgentOps_DeleteAgent_AfterDeleteAlias_Succeeds(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	agent, err := b.CreateAgent("delete-after-alias-removed", "model", "", "", nil)
	require.NoError(t, err)

	// Create then delete the alias.
	aliasRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/agents/%s/aliases", agent.AgentID),
		map[string]any{"agentAliasName": "temp"})
	require.Equal(t, http.StatusAccepted, aliasRec.Code)

	var aliasBody map[string]any
	require.NoError(t, json.Unmarshal(aliasRec.Body.Bytes(), &aliasBody))
	aliasID := aliasBody["agentAlias"].(map[string]any)["agentAliasId"].(string)

	delAliasRec := doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s/aliases/%s", agent.AgentID, aliasID), nil)
	require.Equal(t, http.StatusAccepted, delAliasRec.Code)

	// Now delete the agent — must succeed.
	delRec := doAgentRequest(t, h, http.MethodDelete,
		fmt.Sprintf("/agents/%s", agent.AgentID), nil)
	assert.Equal(t, http.StatusAccepted, delRec.Code)
}

// ── 3. StartIngestionJob concurrent conflict ──────────────────────────────────

func TestBatch2AgentOps_StartIngestionJob_WhileRunning_Rejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
	}{
		{name: "immediate_second_start"},
		{name: "rapid_double_start"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h, _ := newTestAgentsHandler(t)
			kbID, dsID := createKBAndDS(t, h)

			ingestionPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID)

			// First start: must succeed.
			rec1 := doAgentRequest(t, h, http.MethodPost, ingestionPath, map[string]any{})
			assert.Equal(t, http.StatusAccepted, rec1.Code, "first ingestion job start should succeed")

			// Second start while first is still STARTING: must fail.
			rec2 := doAgentRequest(t, h, http.MethodPost, ingestionPath, map[string]any{})
			assert.Equal(t, http.StatusConflict, rec2.Code,
				"starting ingestion job while one is already running should return 409 ConflictException")

			var errBody map[string]any
			require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &errBody))
			assert.Equal(t, "ConflictException", errBody["__type"])
		})
	}
}

func TestBatch2AgentOps_StartIngestionJob_AfterStop_Succeeds(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	ingestionPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID)

	// Start a job.
	rec1 := doAgentRequest(t, h, http.MethodPost, ingestionPath, map[string]any{})
	require.Equal(t, http.StatusAccepted, rec1.Code)

	var started map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &started))
	jobID := started["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	// Stop it.
	stopPath := fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop", kbID, dsID, jobID)
	stopRec := doAgentRequest(t, h, http.MethodPost, stopPath, nil)
	require.Equal(t, http.StatusOK, stopRec.Code)

	// Now start a new job — must succeed (previous is stopped, not running).
	rec2 := doAgentRequest(t, h, http.MethodPost, ingestionPath, map[string]any{})
	assert.Equal(t, http.StatusAccepted, rec2.Code,
		"starting ingestion job after previous is stopped should succeed")
}
