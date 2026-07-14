package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
		"Content":       "{}",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["ModelCardArn"], "my-card")
}

func TestHandler_DescribeModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-1"})
	rec := doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "card-1", resp["ModelCardName"])
}

func TestHandler_UpdateModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-upd"})
	rec := doSageMakerRequest(t, h, "UpdateModelCard", map[string]any{
		"ModelCardName": "card-upd",
		"Content":       "{\"updated\": true}",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify version incremented
	rec = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-upd"})
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.InDelta(t, float64(2), resp["ModelCardVersion"], 0)
}

func TestHandler_DeleteModelCard(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{"ModelCardName": "card-del"})
	rec := doSageMakerRequest(t, h, "DeleteModelCard", map[string]any{"ModelCardName": "card-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeModelCard", map[string]any{"ModelCardName": "card-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// OptimizationJob
// ---------------------------------------------------------------------------

func TestHandler_ListModelCards(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Empty initially
	rec := doSageMakerRequest(t, h, "ListModelCards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["ModelCardSummaries"])

	// Create one
	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName":   "my-card",
		"ModelCardStatus": "Draft",
	})

	rec = doSageMakerRequest(t, h, "ListModelCards", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	summaries := resp["ModelCardSummaries"].([]any)
	assert.Len(t, summaries, 1)

	s := summaries[0].(map[string]any)
	assert.Equal(t, "my-card", s["ModelCardName"])
	assert.Equal(t, "Draft", s["ModelCardStatus"])
	assert.EqualValues(t, 1, s["ModelCardVersion"])
}

func TestHandler_ListModelCardVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
	})

	rec := doSageMakerRequest(t, h, "ListModelCardVersions", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	versions := resp["ModelCardVersionSummaryList"].([]any)
	assert.Len(t, versions, 1)

	v := versions[0].(map[string]any)
	assert.Equal(t, "my-card", v["ModelCardName"])
	assert.EqualValues(t, 1, v["ModelCardVersion"])
}

func TestHandler_ListModelCardVersions_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListModelCardVersions", map[string]any{
		"ModelCardName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListModelCardExportJobs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateModelCard", map[string]any{
		"ModelCardName": "my-card",
	})

	rec := doSageMakerRequest(t, h, "ListModelCardExportJobs", map[string]any{
		"ModelCardName": "my-card",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	jobs := resp["ModelCardExportJobSummaries"].([]any)
	assert.Empty(t, jobs)
}

// ---------------------------------------------------------------------------
// UpdateModelPackage tests
// ---------------------------------------------------------------------------
