package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{
		"FlowDefinitionName": "my-flow",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["FlowDefinitionArn"], "my-flow")
}

func TestHandler_DescribeFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{"FlowDefinitionName": "flow-1"})
	rec := doSageMakerRequest(t, h, "DescribeFlowDefinition", map[string]any{"FlowDefinitionName": "flow-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "flow-1", resp["FlowDefinitionName"])
}

func TestHandler_DeleteFlowDefinition(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	rec := doSageMakerRequest(t, h, "DeleteFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeFlowDefinition", map[string]any{"FlowDefinitionName": "flow-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// AppImageConfig
// ---------------------------------------------------------------------------

func TestHandler_ListFlowDefinitions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListFlowDefinitions", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["FlowDefinitionSummaries"])

	doSageMakerRequest(t, h, "CreateFlowDefinition", map[string]any{
		"FlowDefinitionName": "my-flow",
		"RoleArn":            "arn:aws:iam::000000000000:role/TestRole",
	})

	rec = doSageMakerRequest(t, h, "ListFlowDefinitions", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	defs := resp["FlowDefinitionSummaries"].([]any)
	assert.Len(t, defs, 1)
	d := defs[0].(map[string]any)
	assert.Equal(t, "my-flow", d["FlowDefinitionName"])
}
