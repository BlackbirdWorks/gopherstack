package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{
		"AppImageConfigName": "my-config",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AppImageConfigArn"], "my-config")
}

func TestHandler_DescribeAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-1"})
	rec := doSageMakerRequest(t, h, "DescribeAppImageConfig", map[string]any{"AppImageConfigName": "aic-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "aic-1", resp["AppImageConfigName"])
}

func TestHandler_DeleteAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	rec := doSageMakerRequest(t, h, "DeleteAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeAppImageConfig", map[string]any{"AppImageConfigName": "aic-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_UpdateAppImageConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{"AppImageConfigName": "aic-upd"})
	rec := doSageMakerRequest(t, h, "UpdateAppImageConfig", map[string]any{"AppImageConfigName": "aic-upd"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["AppImageConfigArn"], "aic-upd")
}

// ---------------------------------------------------------------------------
// InferenceExperiment
// ---------------------------------------------------------------------------

func TestHandler_ListAppImageConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListAppImageConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["AppImageConfigs"])

	doSageMakerRequest(t, h, "CreateAppImageConfig", map[string]any{
		"AppImageConfigName": "my-config",
	})

	rec = doSageMakerRequest(t, h, "ListAppImageConfigs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs := resp["AppImageConfigs"].([]any)
	assert.Len(t, configs, 1)
	c := configs[0].(map[string]any)
	assert.Equal(t, "my-config", c["AppImageConfigName"])
}

// ---------------------------------------------------------------------------
// ListTrainingJobsForHyperParameterTuningJob tests
// ---------------------------------------------------------------------------
