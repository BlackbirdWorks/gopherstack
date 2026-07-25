package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCreateAndGetMultiRegionEndpoint tests CreateMultiRegionEndpoint followed
// by GetMultiRegionEndpoint, and field-checks the response against
// GetMultiRegionEndpointOutput (CreatedTimestamp/EndpointId/EndpointName/
// LastUpdatedTimestamp/Routes/Status).
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
			"Details": map[string]any{
				"RoutesDetails": []map[string]any{
					{"Region": "us-west-2"},
				},
			},
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	createResp := decodeJSON(t, createRec)
	assert.NotEmpty(t, createResp["EndpointId"])
	assert.Equal(t, "READY", createResp["Status"])

	rec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints/TestEndpoint", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := decodeJSON(t, rec)
	assert.Equal(t, "TestEndpoint", resp["EndpointName"])
	assert.NotEmpty(t, resp["EndpointId"])
	assert.Equal(t, "READY", resp["Status"])
	assert.NotZero(t, resp["CreatedTimestamp"])
	assert.NotZero(t, resp["LastUpdatedTimestamp"])

	routes, ok := resp["Routes"].([]any)
	require.True(t, ok, "response missing Routes: %v", resp)
	require.Len(t, routes, 2, "expected primary + one secondary region")

	seen := map[string]bool{}

	for _, r := range routes {
		route, isMap := r.(map[string]any)
		require.True(t, isMap)

		region, _ := route["Region"].(string)
		seen[region] = true
	}

	assert.True(t, seen["us-west-2"], "secondary region missing from Routes: %v", routes)
}

// TestCreateMultiRegionEndpoint_AlreadyExists verifies AlreadyExistsException semantics.
func TestCreateMultiRegionEndpoint_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newHandler()
	body := map[string]any{"EndpointName": "dup", "Details": map[string]any{}}

	doRequest(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", body)
	rec := doRequest(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", body)

	assert.Equal(t, http.StatusConflict, rec.Code)
}

// TestListMultiRegionEndpoints tests the ListMultiRegionEndpoints operation.
func TestListMultiRegionEndpoints(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
}

// TestDeleteMultiRegionEndpoint tests the DeleteMultiRegionEndpoint
// operation, including that it reports Status DELETING (the status
// documented as returned "right after the delete request") and that a
// subsequent Get 404s.
func TestDeleteMultiRegionEndpoint(t *testing.T) {
	t.Parallel()

	h := newHandler()

	doRequest(t, h, http.MethodPost, "/v2/email/multi-region-endpoints", map[string]any{
		"EndpointName": "DelEndpoint",
	})

	rec := doRequest(t, h, http.MethodDelete, "/v2/email/multi-region-endpoints/DelEndpoint", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	resp := decodeJSON(t, rec)
	assert.Equal(t, "DELETING", resp["Status"])

	getRec := doRequest(t, h, http.MethodGet, "/v2/email/multi-region-endpoints/DelEndpoint", nil)
	assert.Equal(t, http.StatusNotFound, getRec.Code)
}

// TestDeleteMultiRegionEndpoint_NotFound verifies NotFoundException semantics.
func TestDeleteMultiRegionEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	h := newHandler()
	rec := doRequest(t, h, http.MethodDelete, "/v2/email/multi-region-endpoints/ghost", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
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
