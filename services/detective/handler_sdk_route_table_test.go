package detective_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real Detective
// operation, extracted from detective@v1.41.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. PLACEHOLDER stands in
// for the {ResourceArn} URI label on the /tags/ trio -- classifyTagPath
// (handler.go) dispatches on HTTP method alone and never validates the ARN
// shape, so the literal value doesn't matter here, only that a segment
// follows the "/tags/" prefix. 29 real ops here, matching detective's real
// op count exactly (also matches GetSupportedOperations's own 29 entries
// one-for-one).
//
// A systematic check for a shared method+path across all 29 ops found zero
// collisions -- every op has its own unique (method, path) pair, so no
// *required dynamic* (non-template) member -- the s3/glacier vacuity-trap
// class -- was needed to disambiguate any route in this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptInvitation", "PUT", "/invitation"},
		{"BatchGetGraphMemberDatasources", "POST", "/graph/datasources/get"},
		{"BatchGetMembershipDatasources", "POST", "/membership/datasources/get"},
		{"CreateGraph", "POST", "/graph"},
		{"CreateMembers", "POST", "/graph/members"},
		{"DeleteGraph", "POST", "/graph/removal"},
		{"DeleteMembers", "POST", "/graph/members/removal"},
		{"DescribeOrganizationConfiguration", "POST", "/orgs/describeOrganizationConfiguration"},
		{"DisableOrganizationAdminAccount", "POST", "/orgs/disableAdminAccount"},
		{"DisassociateMembership", "POST", "/membership/removal"},
		{"EnableOrganizationAdminAccount", "POST", "/orgs/enableAdminAccount"},
		{"GetInvestigation", "POST", "/investigations/getInvestigation"},
		{"GetMembers", "POST", "/graph/members/get"},
		{"ListDatasourcePackages", "POST", "/graph/datasources/list"},
		{"ListGraphs", "POST", "/graphs/list"},
		{"ListIndicators", "POST", "/investigations/listIndicators"},
		{"ListInvestigations", "POST", "/investigations/listInvestigations"},
		{"ListInvitations", "POST", "/invitations/list"},
		{"ListMembers", "POST", "/graph/members/list"},
		{"ListOrganizationAdminAccounts", "POST", "/orgs/adminAccountslist"},
		{"ListTagsForResource", "GET", "/tags/PLACEHOLDER"},
		{"RejectInvitation", "POST", "/invitation/removal"},
		{"StartInvestigation", "POST", "/investigations/startInvestigation"},
		{"StartMonitoringMember", "POST", "/graph/member/monitoringstate"},
		{"TagResource", "POST", "/tags/PLACEHOLDER"},
		{"UntagResource", "DELETE", "/tags/PLACEHOLDER"},
		{"UpdateDatasourcePackages", "POST", "/graph/datasources/update"},
		{"UpdateInvestigationState", "POST", "/investigations/updateInvestigationState"},
		{"UpdateOrganizationConfiguration", "POST", "/orgs/updateOrganizationConfiguration"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real Detective op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts classifyPath (handler.go) resolves it to the right op, all 29 ops
// against detective's real op count. It then drives the same request
// through the real Handler() and asserts the response does not contain the
// exact literal "unknown operation" that handleREST's dispatch-miss branch
// (handler.go:259) emits under InvalidInputException with HTTP 400 -- not
// 404, unlike most sibling services -- when classifyPath returns opUnknown.
//
// "unknown operation" was grepped across every non-test .go file in this
// package and found nowhere else: every domain error instead routes through
// mapError, whose messages are built from err.Error() on the package's
// awserr-based ErrNotFound/ErrAlreadyExists/ErrInvalidParameter sentinels,
// none of which contain that two-word literal.
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
