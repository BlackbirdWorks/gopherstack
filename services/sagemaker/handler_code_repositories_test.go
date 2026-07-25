package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{
		"CodeRepositoryName": "my-repo",
		"GitConfig":          map[string]string{"RepositoryUrl": "https://github.com/test/repo"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CodeRepositoryArn"], "my-repo")
}

func TestHandler_DescribeCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-1"})

	rec := doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "repo-1", resp["CodeRepositoryName"])

	// The awsjson1.1 protocol encodes timestamps as epoch-seconds numbers, not
	// RFC3339 strings — assert the wire type directly rather than trusting Go's
	// default time.Time JSON marshaling (which would silently emit a string).
	_, isNumber := resp["CreationTime"].(float64)
	assert.True(t, isNumber, "CreationTime must be a JSON number (epoch seconds), got %T", resp["CreationTime"])
	_, isNumber = resp["LastModifiedTime"].(float64)
	assert.True(t, isNumber, "LastModifiedTime must be a JSON number (epoch seconds), got %T", resp["LastModifiedTime"])
}

func TestHandler_UpdateCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-upd"})
	rec := doSageMakerRequest(t, h, "UpdateCodeRepository", map[string]any{
		"CodeRepositoryName": "repo-upd",
		"GitConfig":          map[string]string{"Branch": "main"},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["CodeRepositoryArn"], "repo-upd")
}

func TestHandler_DeleteCodeRepository(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	rec := doSageMakerRequest(t, h, "DeleteCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeCodeRepository", map[string]any{"CodeRepositoryName": "repo-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListCodeRepositories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-x"})
	doSageMakerRequest(t, h, "CreateCodeRepository", map[string]any{"CodeRepositoryName": "repo-y"})

	rec := doSageMakerRequest(t, h, "ListCodeRepositories", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["CodeRepositorySummaryList"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Project
// ---------------------------------------------------------------------------
