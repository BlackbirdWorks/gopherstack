package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
		"Name": "my-exp",
		"Type": "ShadowMode",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["InferenceExperimentArn"], "my-exp")
}

func TestHandler_DescribeInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "exp-1"})
	rec := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "exp-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "exp-1", resp["Name"])
}

func TestHandler_StopInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "exp-stop"})
	rec := doSageMakerRequest(t, h, "StopInferenceExperiment", map[string]any{"Name": "exp-stop"})
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_DeleteInferenceExperiment(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "exp-del"})
	rec := doSageMakerRequest(t, h, "DeleteInferenceExperiment", map[string]any{"Name": "exp-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "exp-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// MlflowTrackingServer
// ---------------------------------------------------------------------------

func TestHandler_InferenceExperiment_StartAndUpdate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{"Name": "inf-exp"})

	recStart := doSageMakerRequest(t, h, "StartInferenceExperiment", map[string]any{"Name": "inf-exp"})
	require.Equal(t, http.StatusOK, recStart.Code)

	recUpdate := doSageMakerRequest(t, h, "UpdateInferenceExperiment", map[string]any{
		"Name": "inf-exp", "Description": "updated",
	})
	require.Equal(t, http.StatusOK, recUpdate.Code)

	recDescribe := doSageMakerRequest(t, h, "DescribeInferenceExperiment", map[string]any{"Name": "inf-exp"})
	var out map[string]any
	require.NoError(t, json.Unmarshal(recDescribe.Body.Bytes(), &out))
	assert.Equal(t, "Running", out["Status"])
	assert.Equal(t, "updated", out["Description"])
}

func TestHandler_InferenceExperiment_StartAndUpdate_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, op := range []string{"StartInferenceExperiment", "UpdateInferenceExperiment"} {
		t.Run(op, func(t *testing.T) {
			t.Parallel()

			rec := doSageMakerRequest(t, h, op, map[string]any{"Name": "no-such-experiment"})
			assert.Equal(t, http.StatusBadRequest, rec.Code)
		})
	}
}

// ---------------------------------------------------------------------------
// HubContent Update/UpdateReference
// ---------------------------------------------------------------------------

func TestHandler_ListInferenceExperiments(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["InferenceExperiments"])

	doSageMakerRequest(t, h, "CreateInferenceExperiment", map[string]any{
		"Name":    "my-exp",
		"Type":    "ShadowMode",
		"RoleArn": "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListInferenceExperiments", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	exps := resp["InferenceExperiments"].([]any)
	assert.Len(t, exps, 1)
	e := exps[0].(map[string]any)
	assert.Equal(t, "my-exp", e["Name"])
}
