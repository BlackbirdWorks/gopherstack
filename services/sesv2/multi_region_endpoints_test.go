package sesv2_test

import (
	"net/http"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	sesv2sdk "github.com/aws/aws-sdk-go-v2/service/sesv2"
	sesv2types "github.com/aws/aws-sdk-go-v2/service/sesv2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMultiRegionEndpointSDKRoundTrip drives Create/Get/List
// MultiRegionEndpoint through the real aws-sdk-go-v2 sesv2 client, verifying
// the createMultiRegionEndpointOutput/multiRegionEndpointOutput/
// multiRegionEndpointSummaryOutput typed-DTO conversions (wire_output.go)
// against the genuine SDK deserializer rather than a decoded-map assertion.
func TestMultiRegionEndpointSDKRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(
			t *testing.T,
			create *sesv2sdk.CreateMultiRegionEndpointOutput,
			get *sesv2sdk.GetMultiRegionEndpointOutput,
			list []sesv2types.MultiRegionEndpoint,
		)
		name string
	}{
		{
			name: "create_output_shape",
			check: func(
				t *testing.T,
				create *sesv2sdk.CreateMultiRegionEndpointOutput,
				_ *sesv2sdk.GetMultiRegionEndpointOutput,
				_ []sesv2types.MultiRegionEndpoint,
			) {
				t.Helper()
				assert.NotEmpty(t, aws.ToString(create.EndpointId))
				assert.Equal(t, sesv2types.StatusReady, create.Status)
			},
		},
		{
			name: "get_output_routes",
			check: func(
				t *testing.T,
				_ *sesv2sdk.CreateMultiRegionEndpointOutput,
				get *sesv2sdk.GetMultiRegionEndpointOutput,
				_ []sesv2types.MultiRegionEndpoint,
			) {
				t.Helper()
				assert.Equal(t, "sdk-mre", aws.ToString(get.EndpointName))
				assert.Equal(t, sesv2types.StatusReady, get.Status)
				assert.NotNil(t, get.CreatedTimestamp)
				assert.NotNil(t, get.LastUpdatedTimestamp)

				regions := make([]string, 0, len(get.Routes))
				for _, r := range get.Routes {
					regions = append(regions, aws.ToString(r.Region))
				}

				assert.Contains(t, regions, "us-west-2")
			},
		},
		{
			name: "list_output_regions_not_routes",
			check: func(
				t *testing.T,
				_ *sesv2sdk.CreateMultiRegionEndpointOutput,
				_ *sesv2sdk.GetMultiRegionEndpointOutput,
				list []sesv2types.MultiRegionEndpoint,
			) {
				t.Helper()
				require.Len(t, list, 1)
				assert.Equal(t, "sdk-mre", aws.ToString(list[0].EndpointName))
				assert.Contains(t, list[0].Regions, "us-west-2")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newHandler()
			client := newSESv2SDKClient(t, h)

			createOut, err := client.CreateMultiRegionEndpoint(t.Context(), &sesv2sdk.CreateMultiRegionEndpointInput{
				EndpointName: aws.String("sdk-mre"),
				Details: &sesv2types.Details{
					RoutesDetails: []sesv2types.RouteDetails{{Region: aws.String("us-west-2")}},
				},
			})
			require.NoError(t, err)

			getOut, err := client.GetMultiRegionEndpoint(t.Context(), &sesv2sdk.GetMultiRegionEndpointInput{
				EndpointName: aws.String("sdk-mre"),
			})
			require.NoError(t, err)

			listOut, err := client.ListMultiRegionEndpoints(t.Context(), &sesv2sdk.ListMultiRegionEndpointsInput{})
			require.NoError(t, err)

			tt.check(t, createOut, getOut, listOut.MultiRegionEndpoints)
		})
	}
}

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
