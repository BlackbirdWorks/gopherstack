package grafana_test

import (
	"context"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/grafana"
)

// sdkRouteCases is the authoritative method+path for every real Grafana
// (Amazon Managed Grafana) operation, extracted from grafana@v1.38.4
// serializers.go: each entry's "request.Method" and the string passed to
// httpbinding.SplitURI in that op's
// awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in for
// any {workspaceId}/{keyName}/{serviceAccountId}/{tokenId}/{licenseType}/
// {resourceArn} URI label -- routeRequest (handler.go) does not validate ID
// shape, so the literal value doesn't matter here, only that the path
// matches Op. 25 real ops here, matching grafana's real op count exactly
// (see GetSupportedOperations's own doc comment citing
// sdk_completeness_test.go).
//
// A systematic check for a shared method+path across all 25 ops found zero
// collisions, so no *required dynamic* (non-template) member -- the
// s3/glacier vacuity-trap class -- was needed to disambiguate any route in
// this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AssociateLicense", "POST", "/workspaces/PLACEHOLDER/licenses/PLACEHOLDER"},
		{"CreateWorkspace", "POST", "/workspaces"},
		{"CreateWorkspaceApiKey", "POST", "/workspaces/PLACEHOLDER/apikeys"},
		{"CreateWorkspaceServiceAccount", "POST", "/workspaces/PLACEHOLDER/serviceaccounts"},
		{
			"CreateWorkspaceServiceAccountToken",
			"POST",
			"/workspaces/PLACEHOLDER/serviceaccounts/PLACEHOLDER/tokens",
		},
		{"DeleteWorkspace", "DELETE", "/workspaces/PLACEHOLDER"},
		{"DeleteWorkspaceApiKey", "DELETE", "/workspaces/PLACEHOLDER/apikeys/PLACEHOLDER"},
		{
			"DeleteWorkspaceServiceAccount",
			"DELETE",
			"/workspaces/PLACEHOLDER/serviceaccounts/PLACEHOLDER",
		},
		{
			"DeleteWorkspaceServiceAccountToken",
			"DELETE",
			"/workspaces/PLACEHOLDER/serviceaccounts/PLACEHOLDER/tokens/PLACEHOLDER",
		},
		{"DescribeWorkspace", "GET", "/workspaces/PLACEHOLDER"},
		{"DescribeWorkspaceAuthentication", "GET", "/workspaces/PLACEHOLDER/authentication"},
		{"DescribeWorkspaceConfiguration", "GET", "/workspaces/PLACEHOLDER/configuration"},
		{"DisassociateLicense", "DELETE", "/workspaces/PLACEHOLDER/licenses/PLACEHOLDER"},
		{"ListPermissions", "GET", "/workspaces/PLACEHOLDER/permissions"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"ListVersions", "GET", "/versions"},
		{
			"ListWorkspaceServiceAccountTokens",
			"GET",
			"/workspaces/PLACEHOLDER/serviceaccounts/PLACEHOLDER/tokens",
		},
		{"ListWorkspaceServiceAccounts", "GET", "/workspaces/PLACEHOLDER/serviceaccounts"},
		{"ListWorkspaces", "GET", "/workspaces"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdatePermissions", "PATCH", "/workspaces/PLACEHOLDER/permissions"},
		{"UpdateWorkspace", "PUT", "/workspaces/PLACEHOLDER"},
		{"UpdateWorkspaceAuthentication", "POST", "/workspaces/PLACEHOLDER/authentication"},
		{"UpdateWorkspaceConfiguration", "PUT", "/workspaces/PLACEHOLDER/configuration"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Grafana op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts routeRequest resolves it to the right op, all 25 ops against
// grafana's real op count. It then drives the same request through the real
// Handler() and asserts the response did not fall through to
// routeRequest's nil-dispatchFn miss, surfaced by handleError as the
// "unknown path: " prefix under ResourceNotFoundException (handler.go) --
// this is this service's only dispatch-miss mode (routeRequest has a single
// terminal `return "", nil` per branch, not several distinct messages like
// bedrockagent's), and grepping "unknown path"/errUnknownPath across every
// non-test .go file in this package confirms it is used nowhere else, so a
// plain substring check is safe against every domain error this service
// writes (ResourceNotFoundException/ConflictException/ValidationException/
// ServiceQuotaExceededException, all built from apiError's own message,
// never this literal).
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			b := grafana.NewInMemoryBackend(context.Background(), "123456789012", "us-east-1")
			h := grafana.NewHandler(b)

			e := echo.New()
			req := httptest.NewRequest(tc.method, tc.path, nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			require.Equal(t, tc.op, got, "method=%s path=%s", tc.method, tc.path)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(
				t,
				rec.Body.String(),
				"unknown path",
				"method=%s path=%s op=%s: dispatched to the unmatched-route default",
				tc.method,
				tc.path,
				tc.op,
			)
		})
	}
}
