package ram_test

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// sdkRouteCases is the authoritative method+path for every real RAM
// operation, extracted from ram@v1.39.4 serializers.go: each entry's
// "request.Method" and the string passed to httpbinding.SplitURI in that
// op's awsRestjson1_serializeOp<Op>.HandleSerialize. Every real RAM path is
// a fixed, ID-free literal (RAM passes resource identifiers via query
// string or JSON body, never the URL path), so there is no PLACEHOLDER
// substitution needed here unlike every other table in this campaign. 35
// real ops here, matching ram's real op count exactly. This table
// deliberately excludes the handler's own internal opListTagsForResource
// ("/listtagsforresource"): per its doc comment in handler.go, it is not a
// real AWS RAM SDK operation (RAM has no ListTagsForResource action --
// verified against botocore's ram service-2.json; tags are read back via
// GetResourceShares) and is unreachable by any real client.
//
// A systematic check for a shared method+path across all 35 ops found zero
// collisions, so no *required dynamic* (non-template) member -- the
// s3/glacier vacuity-trap class -- was needed to disambiguate any route in
// this table.
//
// Regenerate by grepping serializers.go for every
// "func (m *awsRestjson1_serializeOp<Op>) HandleSerialize" and pulling
// "request.Method" and the httpbinding.SplitURI(...) argument from its body.
func sdkRouteCases() []struct{ op, method, path string } {
	return []struct{ op, method, path string }{
		{"AcceptResourceShareInvitation", "POST", "/acceptresourceshareinvitation"},
		{"AssociateResourceShare", "POST", "/associateresourceshare"},
		{"AssociateResourceSharePermission", "POST", "/associateresourcesharepermission"},
		{"CreatePermission", "POST", "/createpermission"},
		{"CreatePermissionVersion", "POST", "/createpermissionversion"},
		{"CreateResourceShare", "POST", "/createresourceshare"},
		{"DeletePermission", "DELETE", "/deletepermission"},
		{"DeletePermissionVersion", "DELETE", "/deletepermissionversion"},
		{"DeleteResourceShare", "DELETE", "/deleteresourceshare"},
		{"DisassociateResourceShare", "POST", "/disassociateresourceshare"},
		{"DisassociateResourceSharePermission", "POST", "/disassociateresourcesharepermission"},
		{"EnableSharingWithAwsOrganization", "POST", "/enablesharingwithawsorganization"},
		{"GetPermission", "POST", "/getpermission"},
		{"GetResourcePolicies", "POST", "/getresourcepolicies"},
		{"GetResourceShareAssociations", "POST", "/getresourceshareassociations"},
		{"GetResourceShareInvitations", "POST", "/getresourceshareinvitations"},
		{"GetResourceShares", "POST", "/getresourceshares"},
		{"ListPendingInvitationResources", "POST", "/listpendinginvitationresources"},
		{"ListPermissionAssociations", "POST", "/listpermissionassociations"},
		{"ListPermissionVersions", "POST", "/listpermissionversions"},
		{"ListPermissions", "POST", "/listpermissions"},
		{"ListPrincipals", "POST", "/listprincipals"},
		{"ListReplacePermissionAssociationsWork", "POST", "/listreplacepermissionassociationswork"},
		{"ListResourceSharePermissions", "POST", "/listresourcesharepermissions"},
		{"ListResourceTypes", "POST", "/listresourcetypes"},
		{"ListResources", "POST", "/listresources"},
		{"ListSourceAssociations", "POST", "/listsourceassociations"},
		{"PromotePermissionCreatedFromPolicy", "POST", "/promotepermissioncreatedfrompolicy"},
		{"PromoteResourceShareCreatedFromPolicy", "POST", "/promoteresourcesharecreatedfrompolicy"},
		{"RejectResourceShareInvitation", "POST", "/rejectresourceshareinvitation"},
		{"ReplacePermissionAssociations", "POST", "/replacepermissionassociations"},
		{"SetDefaultPermissionVersion", "POST", "/setdefaultpermissionversion"},
		{"TagResource", "POST", "/tagresource"},
		{"UntagResource", "POST", "/untagresource"},
		{"UpdateResourceShare", "POST", "/updateresourceshare"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real RAM op's
// authoritative method+path (see sdkRouteCases) through ExtractOperation and
// asserts it resolves to the right op, all 35 ops against ram's real op
// count. It also exercises the prefix-collision hazard this service's route
// tables (ramGetListRoutes, extractCreateDeleteOp) explicitly call out in
// their own doc comments -- e.g. "/listresourcesharepermissions" sharing the
// literal prefix "/listresources" with ListResources, and
// "/createpermissionversion"/"/deletepermissionversion" sharing prefixes
// with CreatePermission/DeletePermission -- by driving the longer, more
// specific path for each such pair and asserting it does not misclassify as
// the shorter one. It then drives the same request through the real
// Handler() and asserts the response does not contain the exact literal
// "unknown action" that dispatch's terminal fallthrough (handler.go) emits
// wrapping errUnknownAction when no dispatch* function claims the op --
// this service's only dispatch-miss mode -- grepped across every non-test
// .go file in this package and confirmed to appear nowhere else (every
// domain error instead carries one of RAM's many named exception types via
// handleError's type switch, e.g. UnknownResourceException/
// InvalidParameterException, built from a dynamic err.Error(), never this
// literal).
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
