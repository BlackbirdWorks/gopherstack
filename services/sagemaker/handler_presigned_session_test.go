package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_RenderUiTemplate(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "RenderUiTemplate", map[string]any{
		"RoleArn": "arn:aws:iam::000000000000:role/test",
		"Task":    map[string]any{"Input": `{"text":"hello world"}`},
		"UiTemplate": map[string]any{
			"Content": "<p>{{ task.input.text }}</p>",
		},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.Equal(t, "<p>hello world</p>", out["RenderedContent"])
	assert.Empty(t, out["Errors"])
}

func TestHandler_RenderUiTemplate_MissingRoleArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "RenderUiTemplate", map[string]any{
		"Task":       map[string]any{"Input": `{}`},
		"UiTemplate": map[string]any{"Content": "<p></p>"},
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_StartSession(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "StartSession", map[string]any{
		"ResourceIdentifier": "arn:aws:sagemaker:us-east-1:000000000000:space/my-domain/my-space",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["SessionId"])
	assert.NotEmpty(t, out["StreamUrl"])
	assert.NotEmpty(t, out["TokenValue"])
}

func TestHandler_StartSession_MissingResourceIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "StartSession", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// DeleteProcessingJob
// ---------------------------------------------------------------------------
