package resiliencehub_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/resiliencehub"
)

// sdkRouteCases is the authoritative method+path for every real Resilience
// Hub operation, extracted from resiliencehub@v1.38.3 serializers.go: each
// entry's "request.Method" and the string passed to httpbinding.SplitURI in
// that op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands
// in for the {resourceArn} URI label on the /tags/ trio -- every other op
// has a fixed, literal, kebab-case path with NO path parameters at all
// (confirmed directly in the SDK source and already documented on
// RouteMatcher's doc comment in handler.go). 63 real ops here, matching
// Resilience Hub's real op count exactly (also matches
// GetSupportedOperations's own 63 entries one-for-one, per its own doc
// comment).
//
// A systematic check for a shared method+path across all 63 ops found zero
// collisions -- every op's kebab-case action segment is unique, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptResourceGroupingRecommendations", "POST", "/accept-resource-grouping-recommendations"},
		{"AddDraftAppVersionResourceMappings", "POST", "/add-draft-app-version-resource-mappings"},
		{"BatchUpdateRecommendationStatus", "POST", "/batch-update-recommendation-status"},
		{"CreateApp", "POST", "/create-app"},
		{"CreateAppVersionAppComponent", "POST", "/create-app-version-app-component"},
		{"CreateAppVersionResource", "POST", "/create-app-version-resource"},
		{"CreateRecommendationTemplate", "POST", "/create-recommendation-template"},
		{"CreateResiliencyPolicy", "POST", "/create-resiliency-policy"},
		{"DeleteApp", "POST", "/delete-app"},
		{"DeleteAppAssessment", "POST", "/delete-app-assessment"},
		{"DeleteAppInputSource", "POST", "/delete-app-input-source"},
		{"DeleteAppVersionAppComponent", "POST", "/delete-app-version-app-component"},
		{"DeleteAppVersionResource", "POST", "/delete-app-version-resource"},
		{"DeleteRecommendationTemplate", "POST", "/delete-recommendation-template"},
		{"DeleteResiliencyPolicy", "POST", "/delete-resiliency-policy"},
		{"DescribeApp", "POST", "/describe-app"},
		{"DescribeAppAssessment", "POST", "/describe-app-assessment"},
		{"DescribeAppVersion", "POST", "/describe-app-version"},
		{"DescribeAppVersionAppComponent", "POST", "/describe-app-version-app-component"},
		{"DescribeAppVersionResource", "POST", "/describe-app-version-resource"},
		{"DescribeAppVersionResourcesResolutionStatus", "POST", "/describe-app-version-resources-resolution-status"},
		{"DescribeAppVersionTemplate", "POST", "/describe-app-version-template"},
		{"DescribeDraftAppVersionResourcesImportStatus", "POST", "/describe-draft-app-version-resources-import-status"},
		{"DescribeMetricsExport", "POST", "/describe-metrics-export"},
		{"DescribeResiliencyPolicy", "POST", "/describe-resiliency-policy"},
		{"DescribeResourceGroupingRecommendationTask", "POST", "/describe-resource-grouping-recommendation-task"},
		{"ImportResourcesToDraftAppVersion", "POST", "/import-resources-to-draft-app-version"},
		{"ListAlarmRecommendations", "POST", "/list-alarm-recommendations"},
		{"ListAppAssessmentComplianceDrifts", "POST", "/list-app-assessment-compliance-drifts"},
		{"ListAppAssessmentResourceDrifts", "POST", "/list-app-assessment-resource-drifts"},
		{"ListAppAssessments", "GET", "/list-app-assessments"},
		{"ListAppComponentCompliances", "POST", "/list-app-component-compliances"},
		{"ListAppComponentRecommendations", "POST", "/list-app-component-recommendations"},
		{"ListAppInputSources", "POST", "/list-app-input-sources"},
		{"ListApps", "GET", "/list-apps"},
		{"ListAppVersionAppComponents", "POST", "/list-app-version-app-components"},
		{"ListAppVersionResourceMappings", "POST", "/list-app-version-resource-mappings"},
		{"ListAppVersionResources", "POST", "/list-app-version-resources"},
		{"ListAppVersions", "POST", "/list-app-versions"},
		{"ListMetrics", "POST", "/list-metrics"},
		{"ListRecommendationTemplates", "GET", "/list-recommendation-templates"},
		{"ListResiliencyPolicies", "GET", "/list-resiliency-policies"},
		{"ListResourceGroupingRecommendations", "GET", "/list-resource-grouping-recommendations"},
		{"ListSopRecommendations", "POST", "/list-sop-recommendations"},
		{"ListSuggestedResiliencyPolicies", "GET", "/list-suggested-resiliency-policies"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListTestRecommendations", "POST", "/list-test-recommendations"},
		{"ListUnsupportedAppVersionResources", "POST", "/list-unsupported-app-version-resources"},
		{"PublishAppVersion", "POST", "/publish-app-version"},
		{"PutDraftAppVersionTemplate", "POST", "/put-draft-app-version-template"},
		{"RejectResourceGroupingRecommendations", "POST", "/reject-resource-grouping-recommendations"},
		{"RemoveDraftAppVersionResourceMappings", "POST", "/remove-draft-app-version-resource-mappings"},
		{"ResolveAppVersionResources", "POST", "/resolve-app-version-resources"},
		{"StartAppAssessment", "POST", "/start-app-assessment"},
		{"StartMetricsExport", "POST", "/start-metrics-export"},
		{"StartResourceGroupingRecommendationTask", "POST", "/start-resource-grouping-recommendation-task"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateApp", "POST", "/update-app"},
		{"UpdateAppVersion", "POST", "/update-app-version"},
		{"UpdateAppVersionAppComponent", "POST", "/update-app-version-app-component"},
		{"UpdateAppVersionResource", "POST", "/update-app-version-resource"},
		{"UpdateResiliencyPolicy", "POST", "/update-resiliency-policy"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Resilience Hub op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts h.dispatch/h.routes() (handler.go, handler_routes.go) resolves it
// to the right op, all 63 ops against Resilience Hub's real op count. It
// then drives the same request through the real Handler() and asserts the
// response does not contain the literal "unknown path" that Handler() emits
// via handleError(fmt.Errorf("%w: %s %s", errUnknownPath, method, path))
// when h.dispatch's routes() lookup misses.
//
// The miss message is dynamic (it embeds the request's own method and
// path), so it cannot be compared for exact equality the way a static
// sentinel can -- instead, "unknown path" (errUnknownPath's own text) was
// grepped across every non-test .go file in this package and found nowhere
// else: the package's other not-found sentinel, errNotFoundSentinel, is the
// distinct literal "resource not found", so a substring check on "unknown
// path" cannot collide with any legitimate domain response.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := resiliencehub.NewInMemoryBackend(t.Context(), "000000000000", "us-east-1")
			h := resiliencehub.NewHandler(backend)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown path",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default", tc.method, tc.path, tc.op)
		})
	}
}
