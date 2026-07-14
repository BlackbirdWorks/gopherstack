package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{
		"WorkteamName": "my-team",
		"Description":  "Test team",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["WorkteamArn"], "my-team")
}

func TestHandler_DescribeWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-1", "Description": "desc"})

	rec := doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	wt := resp["Workteam"].(map[string]any)
	assert.Equal(t, "team-1", wt["WorkteamName"])
}

func TestHandler_DeleteWorkteam(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-del"})
	rec := doSageMakerRequest(t, h, "DeleteWorkteam", map[string]any{"WorkteamName": "team-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeWorkteam", map[string]any{"WorkteamName": "team-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListWorkteams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-a"})
	doSageMakerRequest(t, h, "CreateWorkteam", map[string]any{"WorkteamName": "team-b"})

	rec := doSageMakerRequest(t, h, "ListWorkteams", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Workteams"].([]any)
	assert.Len(t, items, 2)
}

// TestCompilationJob_InputOutputConfigRoundtrip verifies that InputConfig, OutputConfig,
// and StoppingCondition provided at CreateCompilationJob are persisted and returned by
// DescribeCompilationJob. Real AWS stores and returns these fields.
