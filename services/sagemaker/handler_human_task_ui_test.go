package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "my-ui",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["HumanTaskUiArn"], "my-ui")
}

func TestHandler_DescribeHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	rec := doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "ui-1", resp["HumanTaskUiName"])
}

func TestHandler_DeleteHumanTaskUI(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	rec := doSageMakerRequest(t, h, "DeleteHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeHumanTaskUi", map[string]any{"HumanTaskUiName": "ui-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// Workforce
// ---------------------------------------------------------------------------

func TestHandler_ListHumanTaskUIs(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "ListHumanTaskUis", map[string]any{})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["HumanTaskUiSummaries"])

	doSageMakerRequest(t, h, "CreateHumanTaskUi", map[string]any{
		"HumanTaskUiName": "my-ui",
	})

	rec = doSageMakerRequest(t, h, "ListHumanTaskUis", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	uis := resp["HumanTaskUiSummaries"].([]any)
	assert.Len(t, uis, 1)
	u := uis[0].(map[string]any)
	assert.Equal(t, "my-ui", u["HumanTaskUiName"])
}
