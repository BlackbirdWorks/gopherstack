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
	assert.Equal(t, "InService", resp["Status"])
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

func TestHandler_ListSpaces_NameContainsAndMaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-mr", "SpaceName": "alpha-space"})
	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-mr", "SpaceName": "beta-space"})
	doSageMakerRequest(t, h, "CreateSpace", map[string]any{"DomainId": "d-mr", "SpaceName": "alpha-other"})

	t.Run("spaceNameContains narrows the result set", func(t *testing.T) {
		t.Parallel()

		rec := doSageMakerRequest(t, h, "ListSpaces", map[string]any{
			"DomainIdEquals":    "d-mr",
			"SpaceNameContains": "alpha",
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp["Spaces"].([]any), 2)
	})

	t.Run("maxResults caps the page", func(t *testing.T) {
		t.Parallel()

		rec := doSageMakerRequest(t, h, "ListSpaces", map[string]any{
			"DomainIdEquals": "d-mr",
			"MaxResults":     1,
		})
		require.Equal(t, http.StatusOK, rec.Code)

		var resp map[string]any
		require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
		assert.Len(t, resp["Spaces"].([]any), 1, "MaxResults must cap the page, not just be parsed and ignored")
		assert.NotEmpty(t, resp["NextToken"])
	})
}

func TestHandler_CreateSpace_DisplayNameRoundTrips(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doSageMakerRequest(t, h, "CreateSpace", map[string]any{
		"DomainId":         "d-1",
		"SpaceName":        "display-space",
		"SpaceDisplayName": "My Display Name",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	rec = doSageMakerRequest(t, h, "DescribeSpace", map[string]any{"DomainId": "d-1", "SpaceName": "display-space"})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Equal(t, "My Display Name", resp["SpaceDisplayName"])
}

// ---------------------------------------------------------------------------
// Image
// ---------------------------------------------------------------------------

func TestHandler_UpdateSpace(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doSageMakerRequest(t, h, "CreateDomain", map[string]any{
		"DomainName":          "my-domain",
		"AuthMode":            "SSO",
		"DefaultUserSettings": map[string]any{},
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
