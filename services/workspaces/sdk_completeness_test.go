package workspaces_test

import (
	"testing"

	workspacessdk "github.com/aws/aws-sdk-go-v2/service/workspaces"

	"github.com/blackbirdworks/gopherstack/pkgs/sdkcheck"
	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

// TestSDKCompleteness verifies that every operation exposed by the AWS SDK v2
// workspaces client is either listed in GetSupportedOperations() or explicitly
// acknowledged in the notImplemented slice.
func TestSDKCompleteness(t *testing.T) {
	t.Parallel()

	backend := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
	h := workspaces.NewHandler(backend)

	// 76 of 91 WorkSpaces operations are not yet implemented.
	notImplemented := []string{
		"AcceptAccountLinkInvitation",
		"AssociateConnectionAlias",
		"AssociateIpGroups",
		"AssociateWorkspaceApplication",
		"AuthorizeIpRules",
		"CopyWorkspaceImage",
		"CreateAccountLinkInvitation",
		"CreateConnectClientAddIn",
		"CreateConnectionAlias",
		"CreateIpGroup",
		"CreateStandbyWorkspaces",
		"CreateUpdatedWorkspaceImage",
		"CreateWorkspaceBundle",
		"CreateWorkspaceImage",
		"CreateWorkspacesPool",
		"DeleteAccountLinkInvitation",
		"DeleteClientBranding",
		"DeleteConnectClientAddIn",
		"DeleteConnectionAlias",
		"DeleteIpGroup",
		"DeleteWorkspaceBundle",
		"DeleteWorkspaceImage",
		"DeployWorkspaceApplications",
		"DeregisterWorkspaceDirectory",
		"DescribeAccount",
		"DescribeAccountModifications",
		"DescribeApplicationAssociations",
		"DescribeApplications",
		"DescribeBundleAssociations",
		"DescribeClientBranding",
		"DescribeClientProperties",
		"DescribeConnectClientAddIns",
		"DescribeConnectionAliasPermissions",
		"DescribeConnectionAliases",
		"DescribeCustomWorkspaceImageImport",
		"DescribeImageAssociations",
		"DescribeIpGroups",
		"DescribeWorkspaceAssociations",
		"DescribeWorkspaceImagePermissions",
		"DescribeWorkspaceImages",
		"DescribeWorkspaceSnapshots",
		"DescribeWorkspacesPools",
		"DescribeWorkspacesPoolSessions",
		"DisassociateConnectionAlias",
		"DisassociateIpGroups",
		"DisassociateWorkspaceApplication",
		"GetAccountLink",
		"ImportClientBranding",
		"ImportCustomWorkspaceImage",
		"ImportWorkspaceImage",
		"ListAccountLinks",
		"ListAvailableManagementCidrRanges",
		"MigrateWorkspace",
		"ModifyAccount",
		"ModifyCertificateBasedAuthProperties",
		"ModifyClientProperties",
		"ModifyEndpointEncryptionMode",
		"ModifySamlProperties",
		"ModifySelfservicePermissions",
		"ModifyStreamingProperties",
		"ModifyWorkspaceAccessProperties",
		"ModifyWorkspaceCreationProperties",
		"RegisterWorkspaceDirectory",
		"RejectAccountLinkInvitation",
		"RestoreWorkspace",
		"RevokeIpRules",
		"StartWorkspacesPool",
		"StopWorkspacesPool",
		"TerminateWorkspacesPool",
		"TerminateWorkspacesPoolSession",
		"UpdateConnectClientAddIn",
		"UpdateConnectionAliasPermission",
		"UpdateRulesOfIpGroup",
		"UpdateWorkspaceBundle",
		"UpdateWorkspaceImagePermission",
		"UpdateWorkspacesPool",
	}

	sdkcheck.CheckCompleteness(t, &workspacessdk.Client{}, h.GetSupportedOperations(), notImplemented)
}
