package appconfig_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

func TestHandler_TagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create an application to have a tagged resource ARN.
	rec := doRequest(t, h, http.MethodPost, "/applications", []byte(`{"name":"tag-app"}`))
	require.Equal(t, http.StatusCreated, rec.Code)

	var app appconfig.Application
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &app))

	resourceArn := "arn:aws:appconfig:us-east-1:123456789012:application/" + app.ID

	// List tags — initially empty.
	result := doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	assert.Equal(t, http.StatusOK, result.Code)

	// Tag the resource.
	result = doRequest(t, h, http.MethodPost, "/tags/"+resourceArn,
		[]byte(`{"Tags":{"env":"prod","owner":"team"}}`))
	assert.Equal(t, http.StatusNoContent, result.Code)

	// List tags — should be present.
	result = doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	assert.Equal(t, http.StatusOK, result.Code)

	// Untag.
	result = doRequest(t, h, http.MethodDelete, "/tags/"+resourceArn+"?tagKeys=env", nil)
	assert.Equal(t, http.StatusNoContent, result.Code)
}

func TestHandler_TagResource_VerifyTags(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	resourceArn := "arn:aws:appconfig:us-east-1:123456789012:application/app-123"

	// Tag resource.
	rec := doRequest(t, h, http.MethodPost, "/tags/"+resourceArn,
		[]byte(`{"Tags":{"env":"prod","version":"1.0"}}`))
	require.Equal(t, http.StatusNoContent, rec.Code)

	// List tags - should have 2.
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "prod", resp["Tags"]["env"])
	assert.Equal(t, "1.0", resp["Tags"]["version"])

	// Remove one tag.
	rec = doRequest(t, h, http.MethodDelete, "/tags/"+resourceArn+"?tagKeys=env", nil)
	require.Equal(t, http.StatusNoContent, rec.Code)

	// List tags - should have 1.
	rec = doRequest(t, h, http.MethodGet, "/tags/"+resourceArn, nil)
	require.Equal(t, http.StatusOK, rec.Code)

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Len(t, resp["Tags"], 1)
	assert.Equal(t, "1.0", resp["Tags"]["version"])
}
