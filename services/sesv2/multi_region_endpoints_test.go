package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateAndGetMultiRegionEndpoint tests CreateMultiRegionEndpoint followed by GetMultiRegionEndpoint.
func TestCreateAndGetMultiRegionEndpoint(t *testing.T) {
	t.Parallel()

	h := newHandler()

	createRec := doRequest(
		t,
		h,
		http.MethodPost,
		"/v2/email/multi-region-endpoints",
		map[string]any{
			"EndpointName": "TestEndpoint",
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	rec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints/TestEndpoint", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestListMultiRegionEndpoints tests the ListMultiRegionEndpoints operation.
func TestListMultiRegionEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteMultiRegionEndpoint tests the DeleteMultiRegionEndpoint operation.
func TestDeleteMultiRegionEndpoint(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", map[string]any{
		"EndpointName": "DelEndpoint",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/multi-region-endpoints/DelEndpoint", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestMultiRegionEndpointCRUD verifies Create persists, Get retrieves, List lists.
func TestMultiRegionEndpointCRUD(t *testing.T) {
	t.Parallel()

	h := newHandler()

	rec := doReqQuery(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", nil,
		map[string]any{"EndpointName": "my-endpoint", "Details": map[string]any{}})
	require.Equal(t, http.StatusOK, rec.Code, "CreateMultiRegionEndpoint: %s", rec.Body)

	rec2 := doReqQuery(t, h, http.MethodGet, "/v2/email/multi-region-endpoints/my-endpoint", nil, nil)
	require.Equal(t, http.StatusOK, rec2.Code, "GetMultiRegionEndpoint: %s", rec2.Body)

	resp2 := decodeJSON(t, rec2)
	assert.Equal(t, "READY", resp2["Status"])

	rec3 := doReqQuery(t, h, http.MethodGet, "/v2/email/multi-region-endpoints", nil, nil)
	require.Equal(t, http.StatusOK, rec3.Code, "ListMultiRegionEndpoints: %s", rec3.Body)
}
