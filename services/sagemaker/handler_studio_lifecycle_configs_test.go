package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateStudioLifecycleConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "my-lc",
		"StudioLifecycleConfigAppType": "JupyterServer",
		"StudioLifecycleConfigContent": "IyEvYmluL2Jhc2g=",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["StudioLifecycleConfigArn"], "my-lc")
}

func TestHandler_DescribeStudioLifecycleConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "lc-1",
		"StudioLifecycleConfigAppType": "JupyterServer",
		"StudioLifecycleConfigContent": "IyEvYmluL2Jhc2g=",
	})
	rec := doSageMakerRequest(t, h, "DescribeStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-1",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "lc-1", resp["StudioLifecycleConfigName"])
}

func TestHandler_CreateStudioLifecycleConfig_Content(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "lc-content",
		"StudioLifecycleConfigAppType": "JupyterServer",
		"StudioLifecycleConfigContent": "IyEvYmluL2Jhc2g=",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-content",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "IyEvYmluL2Jhc2g=", resp["StudioLifecycleConfigContent"])
}

func TestHandler_DeleteStudioLifecycleConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "lc-del",
		"StudioLifecycleConfigAppType": "JupyterServer",
		"StudioLifecycleConfigContent": "IyEvYmluL2Jhc2g=",
	})
	rec := doSageMakerRequest(t, h, "DeleteStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-del",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName": "lc-del",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// PartnerApp
// ---------------------------------------------------------------------------

func TestHandler_ListStudioLifecycleConfigs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListStudioLifecycleConfigs", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["StudioLifecycleConfigs"])

	doSageMakerRequest(t, h, "CreateStudioLifecycleConfig", map[string]any{
		"StudioLifecycleConfigName":    "my-config",
		"StudioLifecycleConfigAppType": "JupyterServer",
		"StudioLifecycleConfigContent": "IyEvYmluL2Jhc2g=",
	})

	rec = doSageMakerRequest(t, h, "ListStudioLifecycleConfigs", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	configs := resp["StudioLifecycleConfigs"].([]any)
	assert.Len(t, configs, 1)
	c := configs[0].(map[string]any)
	assert.Equal(t, "my-config", c["StudioLifecycleConfigName"])
}
