package mediapackage_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real MediaPackage
// (v1) operation, extracted from mediapackage@v1.42.4 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER
// stands in for a {Id}/{IngestEndpointId}/{ResourceArn} URI label --
// classifyPath (handler.go) never validates identifier shape, so the
// literal value doesn't matter here, only path depth and static segments.
// This service's paths are unversioned (bare "/channels", "/origin_endpoints",
// "/harvest_jobs", "/tags/{arn}") -- unlike sibling services such as mq
// ("/v1/...") or appmesh ("/v20190125/...") -- and several of those bare
// prefixes are shared with other services (IoT Analytics/MediaTailor on
// "/channels", FIS on "/tags/{arn}"); RouteMatcher disambiguates by SigV4
// service name or ARN substring before a request ever reaches
// ExtractOperation/Handler(), so that disambiguation is out of scope for
// this table -- it drives the handler directly, the same way every other
// route-table test in this campaign bypasses RouteMatcher. 19 real ops
// here, matching mediapackage's real op count exactly (also matches
// GetSupportedOperations's own 19 entries one-for-one).
//
// A systematic check for a shared method+path across all 19 ops found zero
// collisions: DescribeChannel/UpdateChannel/DeleteChannel share
// "/channels/{Id}" and DescribeOriginEndpoint/UpdateOriginEndpoint/
// DeleteOriginEndpoint share "/origin_endpoints/{Id}", but each group is
// disambiguated by method (GET/PUT/DELETE), which classifyChannelRootOp and
// classifyOriginEndpointPath already switch on -- so no *required dynamic*
// (non-template) member -- the s3/glacier vacuity-trap class -- was needed
// to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"ConfigureLogs", "PUT", "/channels/PLACEHOLDER/configure_logs"},
		{"CreateChannel", "POST", "/channels"},
		{"CreateHarvestJob", "POST", "/harvest_jobs"},
		{"CreateOriginEndpoint", "POST", "/origin_endpoints"},
		{"DeleteChannel", "DELETE", "/channels/PLACEHOLDER"},
		{"DeleteOriginEndpoint", "DELETE", "/origin_endpoints/PLACEHOLDER"},
		{"DescribeChannel", "GET", "/channels/PLACEHOLDER"},
		{"DescribeHarvestJob", "GET", "/harvest_jobs/PLACEHOLDER"},
		{"DescribeOriginEndpoint", "GET", "/origin_endpoints/PLACEHOLDER"},
		{"ListChannels", "GET", "/channels"},
		{"ListHarvestJobs", "GET", "/harvest_jobs"},
		{"ListOriginEndpoints", "GET", "/origin_endpoints"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"RotateChannelCredentials", "PUT", "/channels/PLACEHOLDER/credentials"},
		{"RotateIngestEndpointCredentials", "PUT", "/channels/PLACEHOLDER/ingest_endpoints/PLACEHOLDER/credentials"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateChannel", "PUT", "/channels/PLACEHOLDER"},
		{"UpdateOriginEndpoint", "PUT", "/origin_endpoints/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MediaPackage (v1)
// op's authoritative method+path (see sdkRouteCases) through
// ExtractOperation and asserts classifyPath (handler.go) resolves it to the
// right op, all 19 ops against mediapackage's real op count. It then drives
// the same request through the real Handler() and asserts the response does
// not contain the exact literal "unknown operation" that handleREST's
// terminal fallback (handler.go) emits via
// c.JSON(http.StatusNotFound, map[string]any{keyMessage: "unknown operation"})
// when the handlers map has no entry for the classified op -- this
// service's only dispatch-miss mode, grepped across every non-test .go file
// in this package and confirmed to appear nowhere else (every domain error
// instead carries a dynamic err.Error() message via jsonError/
// jsonErrorTyped, never this literal).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
