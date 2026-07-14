package sagemaker_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_CreateSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateSpace", map[string]any{
		"DomainId":  "d-abc123",
		"SpaceName": "my-space",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Contains(t, resp["SpaceArn"], "my-space")
}

func TestHandler_DescribeSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-1"})

	rec := doSageMakerRequest(t, h, "DescribeSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "space-1", resp["SpaceName"])
	assert.Equal(t, "InService", resp["SpaceStatus"])
}

func TestHandler_DeleteSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-del"})
	rec := doSageMakerRequest(t, h, "DeleteSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-del"})
	assert.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeSpace", map[string]any{"DomainId": "d-1", "SpaceName": "space-del"})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_ListSpaces(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "sp-a"})
	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-1", "SpaceName": "sp-b"})
	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-2", "SpaceName": "sp-c"})

	rec := doSageMakerRequest(t, h, "ListSpaces", map[string]any{"DomainIdEquals": "d-1"})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items := resp["Spaces"].([]any)
	assert.Len(t, items, 2)
}

// ---------------------------------------------------------------------------
// Image
// ---------------------------------------------------------------------------

func TestHandler_UpdateSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName": "my-domain",
		"AuthMode":   "SSO",
	})

	var domainResp map[string]any
	rec := doSageMakerRequest(t, h, "ListDomains", map[string]any{})
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &domainResp))
	domains := domainResp["Domains"].([]any)
	require.Len(t, domains, 1)
	domainID := domains[0].(map[string]any)["DomainId"].(string)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "my-space",
	})

	rec = doSageMakerRequest(t, h, "UpdateSpace", map[string]any{
		"DomainId":  domainID,
		"SpaceName": "my-space",
	})
	assert.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.NotEmpty(t, resp["SpaceArn"])
}

func TestHandler_UpdateSpace_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "UpdateSpace", map[string]any{
		"DomainId":  "d-nonexistent",
		"SpaceName": "no-space",
	})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// ---------------------------------------------------------------------------
// UpdateUserProfile tests
// ---------------------------------------------------------------------------
