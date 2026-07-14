package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_InferenceComponentLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rec := doSageMakerRequest(t, h, "CreateInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
		"EndpointName":           "my-endpoint",
		"VariantName":            "variant-1",
		"RuntimeConfig":          map[string]any{"CopyCount": 2},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))
	assert.Contains(t, createResp["InferenceComponentArn"], "my-component")

	// Describe
	rec = doSageMakerRequest(t, h, "DescribeInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &descResp))
	assert.Equal(t, "my-component", descResp["InferenceComponentName"])
	assert.Equal(t, "my-endpoint", descResp["EndpointName"])
	assert.Equal(t, "Creating", descResp["InferenceComponentStatus"])

	// List
	rec = doSageMakerRequest(t, h, "ListInferenceComponents", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &listResp))
	components := listResp["InferenceComponents"].([]any)
	assert.Len(t, components, 1)

	// Update runtime config
	rec = doSageMakerRequest(t, h, "UpdateInferenceComponentRuntimeConfig", map[string]any{
		"InferenceComponentName": "my-component",
		"DesiredRuntimeConfig":   map[string]any{"CopyCount": 4},
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var updateResp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &updateResp))
	assert.Contains(t, updateResp["InferenceComponentArn"], "my-component")

	// Update component
	rec = doSageMakerRequest(t, h, "UpdateInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
		"VariantName":            "variant-2",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = doSageMakerRequest(t, h, "DeleteInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	// Verify deleted
	rec = doSageMakerRequest(t, h, "DescribeInferenceComponent", map[string]any{
		"InferenceComponentName": "my-component",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InferenceComponent_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "DescribeInferenceComponent", map[string]any{
		"InferenceComponentName": "nonexistent",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_InferenceComponent_Duplicate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	body := map[string]any{
		"InferenceComponentName": "dup-component",
		"EndpointName":           "ep",
	}
	doSageMakerRequest(t, h, "CreateInferenceComponent", body)

	rec := doSageMakerRequest(t, h, "CreateInferenceComponent", body)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListInferenceComponents_EndpointFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateInferenceComponent", map[string]any{
		"InferenceComponentName": "comp-a",
		"EndpointName":           "ep-1",
	})
	doSageMakerRequest(t, h, "CreateInferenceComponent", map[string]any{
		"InferenceComponentName": "comp-b",
		"EndpointName":           "ep-2",
	})

	rec := doSageMakerRequest(t, h, "ListInferenceComponents", map[string]any{
		"EndpointNameEquals": "ep-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	components := resp["InferenceComponents"].([]any)
	assert.Len(t, components, 1)
}

// ---------------------------------------------------------------------------
// ClusterSchedulerConfig tests
// ---------------------------------------------------------------------------
