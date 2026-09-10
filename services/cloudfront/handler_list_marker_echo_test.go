package cloudfront_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/cloudfront"
)

// TestListOperations_MarkerEcho_RealHTTP is a regression test for gopherstack-xd0y: every
// classic cloudfront List response (Items/Quantity/IsTruncated/Marker/NextMarker) must echo
// the request's Marker (cloudfront@v1.67.4 types/types.go, "This member is required" on
// Marker for each type named below), even when it is empty. Each case exercises one of the
// edited code paths directly over raw HTTP so the assertion is on the actual serialized XML,
// not just the Go struct.
func TestListOperations_MarkerEcho_RealHTTP(t *testing.T) {
	t.Parallel()

	cases := []struct {
		request func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder
		name    string
	}{
		{
			name: "list_distributions",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet, "/2020-05-31/distribution?Marker="+marker, "")
			},
		},
		{
			// marshalDistributionList / writeDistributionList (query-bound DistributionList path).
			name: "list_distributions_by_web_acl_id",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet,
					"/2020-05-31/distributionsByWebACLId/marker-echo-web-acl?Marker="+marker, "")
			},
		},
		{
			// writeDistributionList's body-bound Marker path (Marker travels in the XML body, not
			// the query string).
			name: "list_distributions_by_realtime_log_config",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				body := `<ListDistributionsByRealtimeLogConfigRequest>` +
					`<RealtimeLogConfigArn>arn:aws:cloudfront::123456789012:realtime-log-config/marker-echo</RealtimeLogConfigArn>` +
					`<Marker>` + marker + `</Marker>` +
					`</ListDistributionsByRealtimeLogConfigRequest>`

				return cfRequest(t, h, http.MethodPost, "/2020-05-31/distributionsByRealtimeLogConfig", body)
			},
		},
		{
			name: "list_distributions_by_cache_policy_id",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet,
					"/2020-05-31/distributionsByCachePolicyId/marker-echo-policy?Marker="+marker, "")
			},
		},
		{
			name: "list_distributions_by_owned_resource",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet,
					"/2020-05-31/distributionsByOwnedResource/"+
						"arn:aws:cloudfront::123456789012:distribution/ABCDEF123456?Marker="+marker, "")
			},
		},
		{
			name: "list_invalidations",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				created := cfOK(
					t,
					h,
					http.MethodPost,
					"/2020-05-31/distribution",
					`<DistributionConfig><CallerReference>marker-echo</CallerReference><Enabled>true</Enabled></DistributionConfig>`,
				)
				id := extractXMLID(t, created)

				return cfRequest(
					t,
					h,
					http.MethodGet,
					"/2020-05-31/distribution/"+id+"/invalidation?Marker="+marker,
					"",
				)
			},
		},
		{
			name: "list_invalidations_for_tenant",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet,
					"/2020-05-31/distribution-tenant/marker-echo-tenant/invalidation?Marker="+marker, "")
			},
		},
		{
			name: "list_anycast_ip_lists",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet, "/2020-05-31/anycast-ip-list?Marker="+marker, "")
			},
		},
		{
			name: "list_cloudfront_origin_access_identities",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(
					t,
					h,
					http.MethodGet,
					"/2020-05-31/origin-access-identity/cloudfront?Marker="+marker,
					"",
				)
			},
		},
		{
			name: "list_origin_access_controls",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet, "/2020-05-31/origin-access-control?Marker="+marker, "")
			},
		},
		{
			name: "list_realtime_log_configs",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet, "/2020-05-31/realtime-log-config?Marker="+marker, "")
			},
		},
		{
			name: "list_streaming_distributions",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet, "/2020-05-31/streaming-distribution?Marker="+marker, "")
			},
		},
		{
			name: "list_vpc_origins",
			request: func(t *testing.T, h *cloudfront.Handler, marker string) *httptest.ResponseRecorder {
				t.Helper()

				return cfRequest(t, h, http.MethodGet, "/2020-05-31/vpc-origin?Marker="+marker, "")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			for _, marker := range []string{"", "probe-marker-xyz"} {
				markerName := "empty"
				if marker != "" {
					markerName = "nonempty"
				}

				t.Run(markerName, func(t *testing.T) {
					t.Parallel()

					h := newCFHandler(t)
					rr := tc.request(t, h, marker)
					require.Equal(t, http.StatusOK, rr.Code, "body: %s", rr.Body.String())
					assert.Contains(t, rr.Body.String(), "<Marker>"+marker+"</Marker>")
				})
			}
		})
	}
}
