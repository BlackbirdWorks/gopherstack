package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestManagedPolicies_SeededAtConstruction verifies that every real-AWS managed
// cache/origin-request/response-headers policy is present as soon as a backend is
// constructed, matching real CloudFront accounts where these policies always exist
// without any explicit Create call. IDs are spot-checked against the values
// published on the "Use managed cache/origin-request/response-headers policies"
// AWS documentation pages (permanent, identical across every account/region).
func TestManagedPolicies_SeededAtConstruction(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")

	cp, err := b.GetCachePolicy("658327ea-f89d-4fab-a63d-7e88639e58f6")
	require.NoError(t, err)
	assert.Equal(t, "Managed-CachingOptimized", cp.Name)
	assert.True(t, cp.Managed)
	require.NotNil(t, cp.Params)
	assert.True(t, cp.Params.EnableAcceptEncodingGzip)

	orp, err := b.GetOriginRequestPolicy("216adef6-5c7f-47e4-b989-5492eafa07d3")
	require.NoError(t, err)
	assert.Equal(t, "Managed-AllViewer", orp.Name)
	assert.True(t, orp.Managed)

	rhp, err := b.GetResponseHeadersPolicy("60669652-455b-4ae9-85a4-c4c02393f86c")
	require.NoError(t, err)
	assert.Equal(t, "Managed-SimpleCORS", rhp.Name)
	assert.True(t, rhp.Managed)
	require.NotNil(t, rhp.CorsConfig)
	assert.Equal(t, []string{"*"}, rhp.CorsConfig.AccessControlAllowOrigins)

	// Every seeded policy across the three families must be marked Managed.
	for _, p := range b.ListCachePolicies() {
		assert.True(t, p.Managed, "cache policy %s (%s) should be managed", p.ID, p.Name)
	}

	for _, p := range b.ListOriginRequestPolicies() {
		assert.True(t, p.Managed, "origin request policy %s (%s) should be managed", p.ID, p.Name)
	}

	for _, p := range b.ListResponseHeadersPolicies() {
		assert.True(t, p.Managed, "response headers policy %s (%s) should be managed", p.ID, p.Name)
	}
}

// TestManagedPolicies_SurviveResetAndRestore verifies managed policies still exist
// after Reset() and after a Restore() from a snapshot that predates seeding (or from
// any snapshot at all) -- real AWS managed policies always exist regardless of
// what an account's persisted state happens to capture.
func TestManagedPolicies_SurviveResetAndRestore(t *testing.T) {
	t.Parallel()

	b := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	b.Reset()

	_, err := b.GetCachePolicy("658327ea-f89d-4fab-a63d-7e88639e58f6")
	require.NoError(t, err, "managed cache policy must survive Reset")

	snap := b.Snapshot(t.Context())
	b2 := cloudfront.NewInMemoryBackend(t.Context(), "123456789012", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	_, err = b2.GetCachePolicy("658327ea-f89d-4fab-a63d-7e88639e58f6")
	require.NoError(t, err, "managed cache policy must survive Restore")
	_, err = b2.GetOriginRequestPolicy("216adef6-5c7f-47e4-b989-5492eafa07d3")
	require.NoError(t, err, "managed origin request policy must survive Restore")
	_, err = b2.GetResponseHeadersPolicy("60669652-455b-4ae9-85a4-c4c02393f86c")
	require.NoError(t, err, "managed response headers policy must survive Restore")
}

// TestManagedPolicies_ListTypeFilter verifies ListCachePolicies/ListOriginRequestPolicies/
// ListResponseHeadersPolicies honor the real ListXPoliciesInput.Type=managed|custom query
// filter, and that each summary carries the correct <Type> element (gopherstack-a9t).
func TestManagedPolicies_ListTypeFilter(t *testing.T) {
	t.Parallel()

	const prefix = "/2020-05-31/"
	h := newCFHandler(t)

	// Create one custom cache policy alongside the seeded managed ones.
	createRR := cfRequest(t, h, http.MethodPost, prefix+"cache-policy",
		`<CachePolicyConfig><Name>my-custom-cp</Name>`+
			`<DefaultTTL>0</DefaultTTL><MaxTTL>1</MaxTTL><MinTTL>0</MinTTL></CachePolicyConfig>`)
	require.Equal(t, http.StatusCreated, createRR.Code, createRR.Body.String())

	allRR := cfRequest(t, h, http.MethodGet, prefix+"cache-policy", "")
	require.Equal(t, http.StatusOK, allRR.Code)
	assert.Contains(t, allRR.Body.String(), "Managed-CachingOptimized")
	assert.Contains(t, allRR.Body.String(), "my-custom-cp")

	managedRR := cfRequest(t, h, http.MethodGet, prefix+"cache-policy?Type=managed", "")
	require.Equal(t, http.StatusOK, managedRR.Code)
	assert.Contains(t, managedRR.Body.String(), "Managed-CachingOptimized")
	assert.NotContains(t, managedRR.Body.String(), "my-custom-cp")
	assert.Contains(t, managedRR.Body.String(), "<Type>managed</Type>")

	customRR := cfRequest(t, h, http.MethodGet, prefix+"cache-policy?Type=custom", "")
	require.Equal(t, http.StatusOK, customRR.Code)
	assert.NotContains(t, customRR.Body.String(), "Managed-CachingOptimized")
	assert.Contains(t, customRR.Body.String(), "my-custom-cp")
	assert.Contains(t, customRR.Body.String(), "<Type>custom</Type>")
}

// TestManagedPolicies_ReadOnly verifies UpdateXPolicy/DeleteXPolicy on a managed
// policy ID returns IllegalUpdate/IllegalDelete (400) rather than mutating or
// removing it, for each of the three policy families.
func TestManagedPolicies_ReadOnly(t *testing.T) {
	t.Parallel()

	cases := []struct {
		resourcePath string
		updateBody   string
		managedID    string
	}{
		{
			resourcePath: "cache-policy",
			managedID:    "658327ea-f89d-4fab-a63d-7e88639e58f6",
			updateBody: `<CachePolicyConfig><Name>hijacked</Name>` +
				`<DefaultTTL>0</DefaultTTL><MaxTTL>1</MaxTTL><MinTTL>0</MinTTL></CachePolicyConfig>`,
		},
		{
			resourcePath: "origin-request-policy",
			managedID:    "216adef6-5c7f-47e4-b989-5492eafa07d3",
			updateBody:   `<OriginRequestPolicyConfig><Name>hijacked</Name></OriginRequestPolicyConfig>`,
		},
		{
			resourcePath: "response-headers-policy",
			managedID:    "60669652-455b-4ae9-85a4-c4c02393f86c",
			updateBody:   `<ResponseHeadersPolicyConfig><Name>hijacked</Name></ResponseHeadersPolicyConfig>`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.resourcePath, func(t *testing.T) {
			t.Parallel()

			const prefix = "/2020-05-31/"
			h := newCFHandler(t)

			getRR := cfRequest(t, h, http.MethodGet, prefix+tc.resourcePath+"/"+tc.managedID, "")
			require.Equal(t, http.StatusOK, getRR.Code, getRR.Body.String())
			etag := getRR.Header().Get("ETag")
			require.NotEmpty(t, etag)

			updateRR := cfRequestWithBodyHeaders(
				t, h, http.MethodPut, prefix+tc.resourcePath+"/"+tc.managedID, tc.updateBody,
				map[string]string{"If-Match": etag},
			)
			assert.Equal(t, http.StatusBadRequest, updateRR.Code, updateRR.Body.String())
			assert.Contains(t, updateRR.Body.String(), "IllegalUpdate")

			deleteRR := cfRequestWithHeader(
				t, h, http.MethodDelete, prefix+tc.resourcePath+"/"+tc.managedID,
				map[string]string{"If-Match": etag},
			)
			assert.Equal(t, http.StatusBadRequest, deleteRR.Code, deleteRR.Body.String())
			assert.Contains(t, deleteRR.Body.String(), "IllegalDelete")

			// The managed policy must still exist afterward, untouched.
			stillThereRR := cfRequest(t, h, http.MethodGet, prefix+tc.resourcePath+"/"+tc.managedID, "")
			require.Equal(t, http.StatusOK, stillThereRR.Code)
			assert.NotContains(t, stillThereRR.Body.String(), "hijacked")
		})
	}
}
