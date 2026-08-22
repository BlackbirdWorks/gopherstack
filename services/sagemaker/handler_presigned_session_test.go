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

// TestHandler_RenderUiTemplate_MissingTaskInput verifies that RenderUiTemplate
// rejects a request with no Task.Input. Task is "This member is required" on
// RenderUiTemplateInput, and RenderableTask.Input is itself "This member is
// required" (types/types.go:19548) — previously accepted silently, rendering
// the template unchanged instead of rejecting the request.
func TestHandler_RenderUiTemplate_MissingTaskInput(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "RenderUiTemplate", map[string]any{
		"RoleArn":    "arn:aws:iam::000000000000:role/test",
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

func TestHandler_CreatePresignedDomainUrl(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	recDomain := doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName":          "my-domain2",
		"DefaultUserSettings": map[string]any{},
	})
	var domainOut map[string]any
	require.NoError(t, json.Unmarshal(recDomain.Body.Bytes(), &domainOut))
	domainID, _ := domainOut["DomainId"].(string)
	require.NotEmpty(t, domainID)

	doSageMakerRequest(t, h, "CreateUserProfile", map[string]any{
		"DomainId": domainID, "UserProfileName": "my-user",
	})

	rec := doSageMakerRequest(t, h, "CreatePresignedDomainUrl", map[string]any{
		"DomainId": domainID, "UserProfileName": "my-user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &out))
	assert.NotEmpty(t, out["AuthorizedUrl"])
}

func TestHandler_CreatePresignedDomainUrl_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreatePresignedDomainUrl", map[string]any{
		"DomainId": "no-such-domain", "UserProfileName": "no-such-user",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// DeleteProcessingJob
// ---------------------------------------------------------------------------
