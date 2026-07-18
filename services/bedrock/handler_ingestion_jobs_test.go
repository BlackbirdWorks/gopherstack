package bedrock_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAgentsHandler_IngestionJobLifecycle(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ij-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	ds, err := b.CreateDataSource(kb.KnowledgeBaseID, "ij-ds", "", nil)
	require.NoError(t, err)

	kbID := kb.KnowledgeBaseID
	dsID := ds.DataSourceID

	// Start ingestion job.
	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "test ingestion"},
	)
	assert.Equal(t, http.StatusAccepted, rec.Code)

	var startOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &startOut))
	job := startOut["ingestionJob"].(map[string]any)
	jobID := job["ingestionJobId"].(string)
	assert.NotEmpty(t, jobID)
	assert.Equal(t, "STARTING", job["status"])

	// Get ingestion job.
	rec2 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s", kbID, dsID, jobID), nil,
	)
	assert.Equal(t, http.StatusOK, rec2.Code)

	// List ingestion jobs.
	rec3 := doAgentRequest(
		t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID), nil,
	)
	assert.Equal(t, http.StatusOK, rec3.Code)

	var listOut map[string]any
	require.NoError(t, json.Unmarshal(rec3.Body.Bytes(), &listOut))
	assert.Len(t, listOut["ingestionJobSummaries"].([]any), 1)
}

func TestAgentsHandler_IngestionJob_EventuallyComplete(t *testing.T) {
	t.Parallel()

	_, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ij-complete-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	ds, err := b.CreateDataSource(kb.KnowledgeBaseID, "ij-complete-ds", "", nil)
	require.NoError(t, err)

	job, err := b.StartIngestionJob(kb.KnowledgeBaseID, ds.DataSourceID, "test")
	require.NoError(t, err)
	assert.Equal(t, "STARTING", job.Status)

	// Wait for async completion.
	time.Sleep(500 * time.Millisecond)

	got, err := b.GetIngestionJob(kb.KnowledgeBaseID, ds.DataSourceID, job.IngestionJobID)
	require.NoError(t, err)
	assert.Equal(t, "COMPLETE", got.Status)
}

func TestAgentsHandler_StartIngestionJob_InvalidJSON(t *testing.T) {
	t.Parallel()

	h, b := newTestAgentsHandler(t)
	kb, err := b.CreateKnowledgeBase("ij-json-kb", "", "", nil, nil, nil)
	require.NoError(t, err)
	ds, err := b.CreateDataSource(kb.KnowledgeBaseID, "ij-json-ds", "", nil)
	require.NoError(t, err)

	e := echo.New()
	req := httptest.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kb.KnowledgeBaseID, ds.DataSourceID),
		bytes.NewReader([]byte("bad json")),
	)
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestStopIngestionJob(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)

	kbID, dsID := createKBAndDS(t, h)

	// Start ingestion job
	startRec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{},
	)
	require.Equal(t, http.StatusAccepted, startRec.Code)

	var jb map[string]any
	require.NoError(t, json.Unmarshal(startRec.Body.Bytes(), &jb))
	jobID := jb["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	// Stop it
	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop",
			kbID, dsID, jobID),
		nil,
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	var sb map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &sb))
	assert.Equal(t, "STOPPED", sb["ingestionJob"].(map[string]any)["status"])
}

func TestAccuracy_IngestionJob_StatusTransitions(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "test ingestion"},
	)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	job := body["ingestionJob"].(map[string]any)
	jobID := job["ingestionJobId"].(string)
	assert.NotEmpty(t, jobID)

	// Status starts as STARTING
	assert.Equal(t, "STARTING", job["status"])
	assert.Equal(t, kbID, job["knowledgeBaseId"])
	assert.Equal(t, dsID, job["dataSourceId"])

	// Get returns the job
	getRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s", kbID, dsID, jobID), nil)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getBody map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getBody))
	gotJob := getBody["ingestionJob"].(map[string]any)
	assert.Equal(t, jobID, gotJob["ingestionJobId"])
}

func TestAccuracy_IngestionJob_ListContainsStartedJob(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	// AWS only allows one running job per data source; stop the first before starting the second.
	rec1 := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "first job"},
	)
	require.Equal(t, http.StatusAccepted, rec1.Code)

	var body1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &body1))
	job1ID := body1["ingestionJob"].(map[string]any)["ingestionJobId"].(string)

	stopRec := doAgentRequest(t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs/%s/stop", kbID, dsID, job1ID), nil)
	require.Equal(t, http.StatusOK, stopRec.Code)

	doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": "second job"},
	)

	listRec := doAgentRequest(t, h, http.MethodGet,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID), nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listBody map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listBody))
	jobs := listBody["ingestionJobSummaries"].([]any)
	assert.GreaterOrEqual(t, len(jobs), 2)
}

func TestAccuracy_IngestionJob_DescriptionPreserved(t *testing.T) {
	t.Parallel()

	h, _ := newTestAgentsHandler(t)
	kbID, dsID := createKBAndDS(t, h)

	const desc = "my important ingestion job"

	rec := doAgentRequest(
		t, h, http.MethodPost,
		fmt.Sprintf("/knowledgebases/%s/datasources/%s/ingestionjobs", kbID, dsID),
		map[string]any{"description": desc},
	)
	require.Equal(t, http.StatusAccepted, rec.Code)

	var body map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
	job := body["ingestionJob"].(map[string]any)
	assert.Equal(t, desc, job["description"])
}

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
