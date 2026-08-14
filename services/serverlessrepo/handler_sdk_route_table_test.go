package serverlessrepo_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Serverless
// Application Repository operation, extracted from
// serverlessapplicationrepository@v1.33.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for a {ApplicationId}/{SemanticVersion}/{TemplateId} URI label --
// ExtractOperation (handler.go) never validates identifier shape, so the
// literal value doesn't matter here, only path depth and static segments.
// 14 real ops here, matching serverlessrepo's real op count exactly (also
// matches GetSupportedOperations's own 14 entries one-for-one).
//
// A systematic check for a shared method+path across all 14 ops found zero
// collisions: GetApplication/UpdateApplication/DeleteApplication share
// "/applications/{ApplicationId}" but are disambiguated by method
// (GET/PATCH/DELETE) -- notably PATCH for update, not PUT -- and
// GetApplicationPolicy/PutApplicationPolicy share
// "/applications/{ApplicationId}/policy", disambiguated by GET/PUT. Both
// distinctions extractSingleSegOp/extractPolicyOp already switch on, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateApplication", "POST", "/applications"},
		{"CreateApplicationVersion", "PUT", "/applications/PLACEHOLDER/versions/PLACEHOLDER"},
		{"CreateCloudFormationChangeSet", "POST", "/applications/PLACEHOLDER/changesets"},
		{"CreateCloudFormationTemplate", "POST", "/applications/PLACEHOLDER/templates"},
		{"DeleteApplication", "DELETE", "/applications/PLACEHOLDER"},
		{"GetApplication", "GET", "/applications/PLACEHOLDER"},
		{"GetApplicationPolicy", "GET", "/applications/PLACEHOLDER/policy"},
		{"GetCloudFormationTemplate", "GET", "/applications/PLACEHOLDER/templates/PLACEHOLDER"},
		{"ListApplicationDependencies", "GET", "/applications/PLACEHOLDER/dependencies"},
		{"ListApplications", "GET", "/applications"},
		{"ListApplicationVersions", "GET", "/applications/PLACEHOLDER/versions"},
		{"PutApplicationPolicy", "PUT", "/applications/PLACEHOLDER/policy"},
		{"UnshareApplication", "POST", "/applications/PLACEHOLDER/unshare"},
		{"UpdateApplication", "PATCH", "/applications/PLACEHOLDER"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real serverlessrepo op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 14 ops against serverlessrepo's
// real op count. It then drives the same request through the real Handler()
// and asserts the response does not contain the exact literal
// "unknown action" that dispatch's terminal default case (handler.go)
// emits wrapping errUnknownAction when the ExtractOperation result matches
// no case -- this service's only dispatch-miss mode, grepped across every
// non-test .go file in this package and confirmed to appear nowhere else
// (every domain error instead carries NotFoundException/
// ConflictException/BadRequestException/InternalServerErrorException built
// from a dynamic err.Error(), never this literal).
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
			assert.NotContains(t, rec.Body.String(), "unknown action",
				"method=%s path=%s op=%s: dispatched to the unmatched-action default", tc.method, tc.path, tc.op)
		})
	}
}
