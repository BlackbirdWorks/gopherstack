package amplify_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Amplify
// operation, extracted from amplify@v1.41.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for any {appId}/{branchName}/{jobId}/{...} URI label -- parseAmplifyOperation
// and routeApps (handler.go) do not validate ID shape, so the literal value
// doesn't matter here, only that the path matches Op. 37 real ops here,
// matching amplify's real op count exactly.
//
// A systematic check for a shared method+path across all 37 ops found zero
// collisions, so no *required dynamic* (non-template) member -- the
// s3/glacier vacuity-trap class -- was needed to disambiguate any route in
// this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"CreateApp", "POST", "/apps"},
		{"CreateBackendEnvironment", "POST", "/apps/PLACEHOLDER/backendenvironments"},
		{"CreateBranch", "POST", "/apps/PLACEHOLDER/branches"},
		{"CreateDeployment", "POST", "/apps/PLACEHOLDER/branches/PLACEHOLDER/deployments"},
		{"CreateDomainAssociation", "POST", "/apps/PLACEHOLDER/domains"},
		{"CreateWebhook", "POST", "/apps/PLACEHOLDER/webhooks"},
		{"DeleteApp", "DELETE", "/apps/PLACEHOLDER"},
		{"DeleteBackendEnvironment", "DELETE", "/apps/PLACEHOLDER/backendenvironments/PLACEHOLDER"},
		{"DeleteBranch", "DELETE", "/apps/PLACEHOLDER/branches/PLACEHOLDER"},
		{"DeleteDomainAssociation", "DELETE", "/apps/PLACEHOLDER/domains/PLACEHOLDER"},
		{"DeleteJob", "DELETE", "/apps/PLACEHOLDER/branches/PLACEHOLDER/jobs/PLACEHOLDER"},
		{"DeleteWebhook", "DELETE", "/webhooks/PLACEHOLDER"},
		{"GenerateAccessLogs", "POST", "/apps/PLACEHOLDER/accesslogs"},
		{"GetApp", "GET", "/apps/PLACEHOLDER"},
		{"GetArtifactUrl", "GET", "/artifacts/PLACEHOLDER"},
		{"GetBackendEnvironment", "GET", "/apps/PLACEHOLDER/backendenvironments/PLACEHOLDER"},
		{"GetBranch", "GET", "/apps/PLACEHOLDER/branches/PLACEHOLDER"},
		{"GetDomainAssociation", "GET", "/apps/PLACEHOLDER/domains/PLACEHOLDER"},
		{"GetJob", "GET", "/apps/PLACEHOLDER/branches/PLACEHOLDER/jobs/PLACEHOLDER"},
		{"GetWebhook", "GET", "/webhooks/PLACEHOLDER"},
		{"ListApps", "GET", "/apps"},
		{"ListArtifacts", "GET", "/apps/PLACEHOLDER/branches/PLACEHOLDER/jobs/PLACEHOLDER/artifacts"},
		{"ListBackendEnvironments", "GET", "/apps/PLACEHOLDER/backendenvironments"},
		{"ListBranches", "GET", "/apps/PLACEHOLDER/branches"},
		{"ListDomainAssociations", "GET", "/apps/PLACEHOLDER/domains"},
		{"ListJobs", "GET", "/apps/PLACEHOLDER/branches/PLACEHOLDER/jobs"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListWebhooks", "GET", "/apps/PLACEHOLDER/webhooks"},
		{"StartDeployment", "POST", "/apps/PLACEHOLDER/branches/PLACEHOLDER/deployments/start"},
		{"StartJob", "POST", "/apps/PLACEHOLDER/branches/PLACEHOLDER/jobs"},
		{"StopJob", "DELETE", "/apps/PLACEHOLDER/branches/PLACEHOLDER/jobs/PLACEHOLDER/stop"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateApp", "POST", "/apps/PLACEHOLDER"},
		{"UpdateBranch", "POST", "/apps/PLACEHOLDER/branches/PLACEHOLDER"},
		{"UpdateDomainAssociation", "POST", "/apps/PLACEHOLDER/domains/PLACEHOLDER"},
		{"UpdateWebhook", "POST", "/webhooks/PLACEHOLDER"},
	}
}

// amplifyTestARN is a stand-in resource ARN routed through the /tags/{arn}
// prefix. RouteMatcher only claims that prefix when the ARN contains
// ":amplify" (handler.go), and ExtractOperation's tags case doesn't inspect
// the ARN at all -- so a literal ARN with the right substring is enough for
// the three TagResource/UntagResource/ListTagsForResource cases above,
// consistent with how PLACEHOLDER stands in for every other dynamic label.
const amplifyTestARN = "arn:aws:amplify:us-east-1:123456789012:apps/PLACEHOLDER"

// TestExtractOperation_SDKRouteTable drives every real Amplify op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts parseAmplifyOperation resolves it to the right op, all 37 ops
// against amplify's real op count. It then drives the same request through
// the real Handler() and asserts the response is neither of this service's
// two distinct dispatch-miss modes: routeApps/routeAppSub/routeAppItem/
// routeAppBranchSub/routeAppBranchItem/routeJobAction/routeWebhooks/
// routeArtifacts's shared "not found" default (an unmatched path shape) and
// handler_apps.go/handler_branches.go/handler_webhooks.go/handler_jobs.go/
// handler_domains.go/handler_environments.go/handler_artifacts.go/
// handler_deployments.go/handler_tags.go's shared "method not allowed"
// default (a recognised path with no case for this method).
//
// The first mode cannot be caught with a plain substring check on "not
// found": amplify's own NotFoundException messages (e.g. "NotFoundException:
// app xyz not found") also contain that phrase, so the check below matches
// the exact quoted JSON fragment `"message":"not found"` instead, which the
// miss default emits verbatim and no domain error does (every domain not-
// found message names the missing resource). "method not allowed" has no
// such collision -- grepped across every non-test .go file in this package,
// it appears only in the nine method-mismatch defaults -- so it is checked
// as a plain substring.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			h, _ := newTestHandler()

			path := tc.path
			if strings.HasPrefix(path, "/tags/") {
				path = "/tags/" + amplifyTestARN
			}

			e := echo.New()
			req := httptest.NewRequest(tc.method, path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, path)

			require.NoError(t, h.Handler()(c))
			body := rec.Body.String()
			assert.NotContains(t, body, `"message":"not found"`,
				"method=%s path=%s op=%s: dispatched to the unmatched-path-shape default", tc.method, path, tc.op)
			assert.NotContains(t, body, "method not allowed",
				"method=%s path=%s op=%s: dispatched to the method-mismatch default", tc.method, path, tc.op)
		})
	}
}
