package mediatailor_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real
// MediaTailor operation, extracted from mediatailor@v1.63.4 serializers.go:
// each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {Param} URI label -- the router does not validate ID shape, so the
// literal value doesn't matter here, only that the path matches Op.
//
// ListPrefetchSchedules is a real AWS API quirk: it alone is POST (not GET)
// on the bare "/prefetchSchedule/{PlaybackConfigurationName}" path, while
// Create/Get/DeletePrefetchSchedule add a trailing "/{Name}" segment --
// verified directly in serializers.go, not an extraction artifact.
//
// The three tag ops' "/tags/{ResourceArn}" entries use a realistic
// "arn:aws:mediatailor:...:channel/PLACEHOLDER" ARN rather than a bare
// PLACEHOLDER: ExtractOperation requires the ARN to contain ":mediatailor:"
// to disambiguate this path from FIS's identically-shaped "/tags/{arn}" --
// a real client's ARN always satisfies this, but an opaque PLACEHOLDER
// string does not.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"ConfigureLogsForChannel", "PUT", "/configureLogs/channel"},
		{"ConfigureLogsForPlaybackConfiguration", "PUT", "/configureLogs/playbackConfiguration"},
		{"CreateChannel", "POST", "/channel/PLACEHOLDER"},
		{"CreateLiveSource", "POST", "/sourceLocation/PLACEHOLDER/liveSource/PLACEHOLDER"},
		{"CreatePrefetchSchedule", "POST", "/prefetchSchedule/PLACEHOLDER/PLACEHOLDER"},
		{"CreateProgram", "POST", "/channel/PLACEHOLDER/program/PLACEHOLDER"},
		{"CreateSourceLocation", "POST", "/sourceLocation/PLACEHOLDER"},
		{"CreateVodSource", "POST", "/sourceLocation/PLACEHOLDER/vodSource/PLACEHOLDER"},
		{"DeleteChannel", "DELETE", "/channel/PLACEHOLDER"},
		{"DeleteChannelPolicy", "DELETE", "/channel/PLACEHOLDER/policy"},
		{"DeleteFunction", "DELETE", "/function/PLACEHOLDER"},
		{"DeleteLiveSource", "DELETE", "/sourceLocation/PLACEHOLDER/liveSource/PLACEHOLDER"},
		{"DeletePlaybackConfiguration", "DELETE", "/playbackConfiguration/PLACEHOLDER"},
		{"DeletePrefetchSchedule", "DELETE", "/prefetchSchedule/PLACEHOLDER/PLACEHOLDER"},
		{"DeleteProgram", "DELETE", "/channel/PLACEHOLDER/program/PLACEHOLDER"},
		{"DeleteSourceLocation", "DELETE", "/sourceLocation/PLACEHOLDER"},
		{"DeleteVodSource", "DELETE", "/sourceLocation/PLACEHOLDER/vodSource/PLACEHOLDER"},
		{"DescribeChannel", "GET", "/channel/PLACEHOLDER"},
		{"DescribeLiveSource", "GET", "/sourceLocation/PLACEHOLDER/liveSource/PLACEHOLDER"},
		{"DescribeProgram", "GET", "/channel/PLACEHOLDER/program/PLACEHOLDER"},
		{"DescribeSourceLocation", "GET", "/sourceLocation/PLACEHOLDER"},
		{"DescribeVodSource", "GET", "/sourceLocation/PLACEHOLDER/vodSource/PLACEHOLDER"},
		{"GetChannelPolicy", "GET", "/channel/PLACEHOLDER/policy"},
		{"GetChannelSchedule", "GET", "/channel/PLACEHOLDER/schedule"},
		{"GetFunction", "GET", "/function/PLACEHOLDER"},
		{"GetPlaybackConfiguration", "GET", "/playbackConfiguration/PLACEHOLDER"},
		{"GetPrefetchSchedule", "GET", "/prefetchSchedule/PLACEHOLDER/PLACEHOLDER"},
		{"ListAlerts", "GET", "/alerts"},
		{"ListChannels", "GET", "/channels"},
		{"ListFunctions", "GET", "/functions"},
		{"ListLiveSources", "GET", "/sourceLocation/PLACEHOLDER/liveSources"},
		{"ListPlaybackConfigurations", "GET", "/playbackConfigurations"},
		{"ListPrefetchSchedules", "POST", "/prefetchSchedule/PLACEHOLDER"},
		{"ListSourceLocations", "GET", "/sourceLocations"},
		{"ListTagsForResource", "GET", "/tags/arn:aws:mediatailor:us-east-1:000000000000:channel/PLACEHOLDER"},
		{"ListVodSources", "GET", "/sourceLocation/PLACEHOLDER/vodSources"},
		{"PutChannelPolicy", "PUT", "/channel/PLACEHOLDER/policy"},
		{"PutFunction", "PUT", "/function/PLACEHOLDER"},
		{"PutPlaybackConfiguration", "PUT", "/playbackConfiguration"},
		{"StartChannel", "PUT", "/channel/PLACEHOLDER/start"},
		{"StopChannel", "PUT", "/channel/PLACEHOLDER/stop"},
		{"TagResource", "POST", "/tags/arn:aws:mediatailor:us-east-1:000000000000:channel/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/arn:aws:mediatailor:us-east-1:000000000000:channel/PLACEHOLDER"},
		{"UpdateChannel", "PUT", "/channel/PLACEHOLDER"},
		{"UpdateLiveSource", "PUT", "/sourceLocation/PLACEHOLDER/liveSource/PLACEHOLDER"},
		{"UpdateProgram", "PUT", "/channel/PLACEHOLDER/program/PLACEHOLDER"},
		{"UpdateSourceLocation", "PUT", "/sourceLocation/PLACEHOLDER"},
		{"UpdateVodSource", "PUT", "/sourceLocation/PLACEHOLDER/vodSource/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real MediaTailor op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts the route table resolves it to the right op. gopherstack-jqh2 pass
// 3: re-extracted all 48 mediatailor ops from the pinned SDK and confirmed
// the existing route table already correct, including the
// ListPrefetchSchedules POST quirk (already handled with a doc comment
// before this pass) and the several same-path/different-method collisions
// this service's routing depends on (/channel/{name}, /function/{id}).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			c := e.NewContext(req, httptest.NewRecorder())

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)
		})
	}
}
