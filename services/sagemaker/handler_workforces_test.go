package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{
		"WorkforceName": "my-workforce",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["WorkforceArn"], "my-workforce")
}

func TestHandler_DescribeWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-1"})
	rec := doSageMakerRequest(t, h, "DescribeWorkforce", map[string]any{"WorkforceName": "wf-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Workforce"])
}

func TestHandler_UpdateWorkforce(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateWorkforce", map[string]any{"WorkforceName": "wf-upd"})
	rec := doSageMakerRequest(t, h, "UpdateWorkforce", map[string]any{"WorkforceName": "wf-upd"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotNil(t, resp["Workforce"])
}

// ---------------------------------------------------------------------------
// FlowDefinition
// ---------------------------------------------------------------------------
