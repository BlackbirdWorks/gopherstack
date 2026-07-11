package cloudfront_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
)

// Test_InconsistentQuantities_EndToEnd proves that a caller-supplied Quantity that
// disagrees with the actual number of Items in a config payload is rejected with
// AWS's InconsistentQuantities error, across several op families that carry the
// pervasive <X><Quantity>N</Quantity><Items>...</Items></X> pattern. Real CloudFront
// validates this everywhere; because the emulator historically parsed configs by
// slice (whose length is authoritative regardless of the caller's stated Quantity),
// a bad Quantity was silently accepted.
func Test_InconsistentQuantities_EndToEnd(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		method     string
		path       func(h *testHarness) string
		body       []byte
		wantStatus int
		wantErr    bool
	}{
		{
			name:   "CreateDistribution: Origins Quantity overstates Items",
			method: http.MethodPost,
			path:   func(*testHarness) string { return "/2020-05-31/distribution" },
			body: []byte(`<DistributionConfig>` +
				`<CallerReference>ref-iq-1</CallerReference>` +
				`<Comment>c</Comment>` +
				`<Enabled>true</Enabled>` +
				`<Origins><Quantity>2</Quantity><Items><Origin><Id>o1</Id></Origin></Items></Origins>` +
				`</DistributionConfig>`),
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "CreateDistribution: Origins Quantity matches Items (control)",
			method: http.MethodPost,
			path:   func(*testHarness) string { return "/2020-05-31/distribution" },
			body: []byte(`<DistributionConfig>` +
				`<CallerReference>ref-iq-2</CallerReference>` +
				`<Comment>c</Comment>` +
				`<Enabled>true</Enabled>` +
				`<Origins><Quantity>1</Quantity><Items><Origin><Id>o1</Id></Origin></Items></Origins>` +
				`</DistributionConfig>`),
			wantStatus: http.StatusCreated,
			wantErr:    false,
		},
		{
			name:   "CreateCachePolicy: HeadersConfig Quantity understates Items",
			method: http.MethodPost,
			path:   func(*testHarness) string { return "/2020-05-31/cache-policy" },
			body: []byte(`<CachePolicyConfig>` +
				`<Name>iq-cp-1</Name>` +
				`<DefaultTTL>86400</DefaultTTL>` +
				`<MaxTTL>31536000</MaxTTL>` +
				`<MinTTL>0</MinTTL>` +
				`<ParametersInCacheKeyAndForwardedToOrigin>` +
				`<CookiesConfig><CookieBehavior>none</CookieBehavior></CookiesConfig>` +
				`<HeadersConfig><HeaderBehavior>whitelist</HeaderBehavior>` +
				`<Headers><Quantity>1</Quantity><Items><Name>X-A</Name><Name>X-B</Name></Items></Headers>` +
				`</HeadersConfig>` +
				`<QueryStringsConfig><QueryStringBehavior>none</QueryStringBehavior></QueryStringsConfig>` +
				`<EnableAcceptEncodingGzip>false</EnableAcceptEncodingGzip>` +
				`</ParametersInCacheKeyAndForwardedToOrigin>` +
				`</CachePolicyConfig>`),
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "CreateInvalidation: Paths Quantity overstates Items",
			method: http.MethodPost,
			path: func(h *testHarness) string {
				return "/2020-05-31/distribution/" + h.distID + "/invalidation"
			},
			body: []byte(`<InvalidationBatch>` +
				`<CallerReference>iq-inval-1</CallerReference>` +
				`<Paths><Quantity>5</Quantity><Items><Path>/a</Path></Items></Paths>` +
				`</InvalidationBatch>`),
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "CreateDistribution: nested AllowedMethods Quantity mismatch inside CacheBehaviors",
			method: http.MethodPost,
			path:   func(*testHarness) string { return "/2020-05-31/distribution" },
			body: []byte(`<DistributionConfig>` +
				`<CallerReference>ref-iq-3</CallerReference>` +
				`<Comment>c</Comment>` +
				`<Enabled>true</Enabled>` +
				`<Origins><Quantity>1</Quantity><Items><Origin><Id>o1</Id></Origin></Items></Origins>` +
				`<CacheBehaviors><Quantity>1</Quantity><Items><CacheBehavior>` +
				`<AllowedMethods><Quantity>3</Quantity>` +
				`<Items><Method>GET</Method><Method>HEAD</Method></Items>` +
				`</AllowedMethods></CacheBehavior></Items></CacheBehaviors>` +
				`</DistributionConfig>`),
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
		{
			name:   "CreateResponseHeadersPolicy: CustomHeadersConfig Quantity understates Items",
			method: http.MethodPost,
			path:   func(*testHarness) string { return "/2020-05-31/response-headers-policy" },
			body: []byte(`<ResponseHeadersPolicyConfig>` +
				`<Name>iq-rhp-1</Name>` +
				`<CustomHeadersConfig><Quantity>1</Quantity><Items>` +
				`<ResponseHeadersPolicyCustomHeader>` +
				`<Header>X-A</Header><Value>a</Value><Override>false</Override>` +
				`</ResponseHeadersPolicyCustomHeader>` +
				`<ResponseHeadersPolicyCustomHeader>` +
				`<Header>X-B</Header><Value>b</Value><Override>false</Override>` +
				`</ResponseHeadersPolicyCustomHeader>` +
				`</Items></CustomHeadersConfig>` +
				`</ResponseHeadersPolicyConfig>`),
			wantStatus: http.StatusBadRequest,
			wantErr:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler()
			harness := &testHarness{}

			d, err := h.Backend.CreateDistribution(
				"iq-harness-ref-"+tc.name, "harness", true,
				minimalDistConfig("iq-harness-ref-"+tc.name, "harness", true),
			)
			if err == nil {
				harness.distID = d.ID
			}

			rec := doXML(t, h, tc.method, tc.path(harness), tc.body)

			assert.Equal(t, tc.wantStatus, rec.Code, "status code; body=%s", rec.Body.String())

			if tc.wantErr {
				assert.Contains(t, rec.Body.String(), "InconsistentQuantities")
			} else {
				assert.NotContains(t, rec.Body.String(), "InconsistentQuantities")
			}
		})
	}
}

// testHarness carries per-subtest fixture state (e.g. a pre-created distribution ID
// for invalidation tests) without leaking it across the table-driven subtests, each
// of which gets its own fresh *cloudfront.Handler / backend.
type testHarness struct {
	distID string
}
