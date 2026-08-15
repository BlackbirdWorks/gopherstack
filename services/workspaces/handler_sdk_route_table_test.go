package workspaces_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// sdkRouteCases is the authoritative X-Amz-Target for every real Amazon
// WorkSpaces operation, extracted from workspaces@v1.73.1 serializers.go:
// each op's awsAwsjson11_serializeOp<Op>.HandleSerialize sets
// httpBindingEncoder.SetHeader("X-Amz-Target").String("WorkspacesService.<Op>")
// and always request.Request.Method = "POST" against path "/" -- WorkSpaces
// is JSON-RPC 1.1 (services/_PROTOCOLS.md), so unlike a REST-family service
// there is no path template to get wrong: dispatch is entirely by this one
// header. ExtractOperation and the shared pkgs/service.HandleTarget both
// derive the action the same way (split on "."), so the class of bug this
// table can catch is a dispatch-table key that doesn't exactly match the
// real op name (typo, wrong case -- WorkSpaces is case-sensitive JSON-RPC),
// not a route-template mismatch.
//
// This table covers all 91 real WorkSpaces ops -- confirmed by diffing the
// actual buildOps() dispatch map (14 op-family builders merged via
// maps.Copy, handler.go:117-136) against this exact list: zero mismatches
// in either direction, no dead or excluded keys. GetSupportedOperations()
// is derived directly from h.ops's map keys (handler.go:45-52), so it is
// correct by construction and cannot itself drift from the dispatch table
// -- the only drift risk here is buildOps() vs the SDK, which this table
// checks directly.
//
// Regenerate by grepping serializers.go for every
// `SetHeader("X-Amz-Target").String("WorkspacesService.` and pulling the
// suffix after the last dot.
func sdkRouteCases() []struct{ op, target string } {
	return []struct{ op, target string }{
		{"AcceptAccountLinkInvitation", "WorkspacesService.AcceptAccountLinkInvitation"},
		{"AssociateConnectionAlias", "WorkspacesService.AssociateConnectionAlias"},
		{"AssociateIpGroups", "WorkspacesService.AssociateIpGroups"},
		{"AssociateWorkspaceApplication", "WorkspacesService.AssociateWorkspaceApplication"},
		{"AuthorizeIpRules", "WorkspacesService.AuthorizeIpRules"},
		{"CopyWorkspaceImage", "WorkspacesService.CopyWorkspaceImage"},
		{"CreateAccountLinkInvitation", "WorkspacesService.CreateAccountLinkInvitation"},
		{"CreateConnectClientAddIn", "WorkspacesService.CreateConnectClientAddIn"},
		{"CreateConnectionAlias", "WorkspacesService.CreateConnectionAlias"},
		{"CreateIpGroup", "WorkspacesService.CreateIpGroup"},
		{"CreateStandbyWorkspaces", "WorkspacesService.CreateStandbyWorkspaces"},
		{"CreateTags", "WorkspacesService.CreateTags"},
		{"CreateUpdatedWorkspaceImage", "WorkspacesService.CreateUpdatedWorkspaceImage"},
		{"CreateWorkspaceBundle", "WorkspacesService.CreateWorkspaceBundle"},
		{"CreateWorkspaceImage", "WorkspacesService.CreateWorkspaceImage"},
		{"CreateWorkspaces", "WorkspacesService.CreateWorkspaces"},
		{"CreateWorkspacesPool", "WorkspacesService.CreateWorkspacesPool"},
		{"DeleteAccountLinkInvitation", "WorkspacesService.DeleteAccountLinkInvitation"},
		{"DeleteClientBranding", "WorkspacesService.DeleteClientBranding"},
		{"DeleteConnectClientAddIn", "WorkspacesService.DeleteConnectClientAddIn"},
		{"DeleteConnectionAlias", "WorkspacesService.DeleteConnectionAlias"},
		{"DeleteIpGroup", "WorkspacesService.DeleteIpGroup"},
		{"DeleteTags", "WorkspacesService.DeleteTags"},
		{"DeleteWorkspaceBundle", "WorkspacesService.DeleteWorkspaceBundle"},
		{"DeleteWorkspaceImage", "WorkspacesService.DeleteWorkspaceImage"},
		{"DeployWorkspaceApplications", "WorkspacesService.DeployWorkspaceApplications"},
		{"DeregisterWorkspaceDirectory", "WorkspacesService.DeregisterWorkspaceDirectory"},
		{"DescribeAccount", "WorkspacesService.DescribeAccount"},
		{"DescribeAccountModifications", "WorkspacesService.DescribeAccountModifications"},
		{"DescribeApplicationAssociations", "WorkspacesService.DescribeApplicationAssociations"},
		{"DescribeApplications", "WorkspacesService.DescribeApplications"},
		{"DescribeBundleAssociations", "WorkspacesService.DescribeBundleAssociations"},
		{"DescribeClientBranding", "WorkspacesService.DescribeClientBranding"},
		{"DescribeClientProperties", "WorkspacesService.DescribeClientProperties"},
		{"DescribeConnectClientAddIns", "WorkspacesService.DescribeConnectClientAddIns"},
		{"DescribeConnectionAliases", "WorkspacesService.DescribeConnectionAliases"},
		{"DescribeConnectionAliasPermissions", "WorkspacesService.DescribeConnectionAliasPermissions"},
		{"DescribeCustomWorkspaceImageImport", "WorkspacesService.DescribeCustomWorkspaceImageImport"},
		{"DescribeImageAssociations", "WorkspacesService.DescribeImageAssociations"},
		{"DescribeIpGroups", "WorkspacesService.DescribeIpGroups"},
		{"DescribeTags", "WorkspacesService.DescribeTags"},
		{"DescribeWorkspaceAssociations", "WorkspacesService.DescribeWorkspaceAssociations"},
		{"DescribeWorkspaceBundles", "WorkspacesService.DescribeWorkspaceBundles"},
		{"DescribeWorkspaceDirectories", "WorkspacesService.DescribeWorkspaceDirectories"},
		{"DescribeWorkspaceImagePermissions", "WorkspacesService.DescribeWorkspaceImagePermissions"},
		{"DescribeWorkspaceImages", "WorkspacesService.DescribeWorkspaceImages"},
		{"DescribeWorkspaces", "WorkspacesService.DescribeWorkspaces"},
		{"DescribeWorkspacesConnectionStatus", "WorkspacesService.DescribeWorkspacesConnectionStatus"},
		{"DescribeWorkspaceSnapshots", "WorkspacesService.DescribeWorkspaceSnapshots"},
		{"DescribeWorkspacesPools", "WorkspacesService.DescribeWorkspacesPools"},
		{"DescribeWorkspacesPoolSessions", "WorkspacesService.DescribeWorkspacesPoolSessions"},
		{"DisassociateConnectionAlias", "WorkspacesService.DisassociateConnectionAlias"},
		{"DisassociateIpGroups", "WorkspacesService.DisassociateIpGroups"},
		{"DisassociateWorkspaceApplication", "WorkspacesService.DisassociateWorkspaceApplication"},
		{"GetAccountLink", "WorkspacesService.GetAccountLink"},
		{"ImportClientBranding", "WorkspacesService.ImportClientBranding"},
		{"ImportCustomWorkspaceImage", "WorkspacesService.ImportCustomWorkspaceImage"},
		{"ImportWorkspaceImage", "WorkspacesService.ImportWorkspaceImage"},
		{"ListAccountLinks", "WorkspacesService.ListAccountLinks"},
		{"ListAvailableManagementCidrRanges", "WorkspacesService.ListAvailableManagementCidrRanges"},
		{"MigrateWorkspace", "WorkspacesService.MigrateWorkspace"},
		{"ModifyAccount", "WorkspacesService.ModifyAccount"},
		{"ModifyCertificateBasedAuthProperties", "WorkspacesService.ModifyCertificateBasedAuthProperties"},
		{"ModifyClientProperties", "WorkspacesService.ModifyClientProperties"},
		{"ModifyEndpointEncryptionMode", "WorkspacesService.ModifyEndpointEncryptionMode"},
		{"ModifySamlProperties", "WorkspacesService.ModifySamlProperties"},
		{"ModifySelfservicePermissions", "WorkspacesService.ModifySelfservicePermissions"},
		{"ModifyStreamingProperties", "WorkspacesService.ModifyStreamingProperties"},
		{"ModifyWorkspaceAccessProperties", "WorkspacesService.ModifyWorkspaceAccessProperties"},
		{"ModifyWorkspaceCreationProperties", "WorkspacesService.ModifyWorkspaceCreationProperties"},
		{"ModifyWorkspaceProperties", "WorkspacesService.ModifyWorkspaceProperties"},
		{"ModifyWorkspaceState", "WorkspacesService.ModifyWorkspaceState"},
		{"RebootWorkspaces", "WorkspacesService.RebootWorkspaces"},
		{"RebuildWorkspaces", "WorkspacesService.RebuildWorkspaces"},
		{"RegisterWorkspaceDirectory", "WorkspacesService.RegisterWorkspaceDirectory"},
		{"RejectAccountLinkInvitation", "WorkspacesService.RejectAccountLinkInvitation"},
		{"RestoreWorkspace", "WorkspacesService.RestoreWorkspace"},
		{"RevokeIpRules", "WorkspacesService.RevokeIpRules"},
		{"StartWorkspaces", "WorkspacesService.StartWorkspaces"},
		{"StartWorkspacesPool", "WorkspacesService.StartWorkspacesPool"},
		{"StopWorkspaces", "WorkspacesService.StopWorkspaces"},
		{"StopWorkspacesPool", "WorkspacesService.StopWorkspacesPool"},
		{"TerminateWorkspaces", "WorkspacesService.TerminateWorkspaces"},
		{"TerminateWorkspacesPool", "WorkspacesService.TerminateWorkspacesPool"},
		{"TerminateWorkspacesPoolSession", "WorkspacesService.TerminateWorkspacesPoolSession"},
		{"UpdateConnectClientAddIn", "WorkspacesService.UpdateConnectClientAddIn"},
		{"UpdateConnectionAliasPermission", "WorkspacesService.UpdateConnectionAliasPermission"},
		{"UpdateRulesOfIpGroup", "WorkspacesService.UpdateRulesOfIpGroup"},
		{"UpdateWorkspaceBundle", "WorkspacesService.UpdateWorkspaceBundle"},
		{"UpdateWorkspaceImagePermission", "WorkspacesService.UpdateWorkspaceImagePermission"},
		{"UpdateWorkspacesPool", "WorkspacesService.UpdateWorkspacesPool"},
	}
}

// TestExtractOperation_SDKRouteTable drives every real WorkSpaces
// operation's authoritative X-Amz-Target through ExtractOperation and
// Handler(), asserting the header resolves to the right op name and that
// Handler() does not fall through to the dispatch-miss sentinel a
// dispatch-table key mismatch would produce.
//
// WorkSpaces's dispatch-miss sentinel (unknownOpError, constructed only by
// newUnknownOpError at its single production call site, handler.go:87) is
// NOT wire-typed at all -- it falls through handleError's default branch to
// the generic "InternalServerException", which every unclassified real
// internal error also produces, so asserting on the wire type would be
// unsafe (the iam trap, in its wire-code-shared form). This test asserts on
// unknownOpError's own message text ("unknown operation: ") instead, which
// is unique in the package (grepped) and only ever produced by the dispatch
// miss.
func TestExtractOperation_SDKRouteTable(t *testing.T) {
	t.Parallel()

	for _, tc := range sdkRouteCases() {
		t.Run(strings.ToLower(tc.op), func(t *testing.T) {
			t.Parallel()

			backend := workspaces.NewInMemoryBackend("111122223333", "us-east-1")
			h := workspaces.NewHandler(backend)
			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/", strings.NewReader("{}"))
			req.Header.Set("X-Amz-Target", tc.target)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			got := h.ExtractOperation(c)
			assert.Equal(t, tc.op, got)

			require.NoError(t, h.Handler()(c))
			assert.NotContains(t, rec.Body.String(), "unknown operation:",
				"target=%s op=%s: dispatched to the unmatched-route handler", tc.target, tc.op)
		})
	}
}
