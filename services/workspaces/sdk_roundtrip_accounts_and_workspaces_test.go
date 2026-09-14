package workspaces_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	wstypes "github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestRealClient_AccountsAndWorkspaces drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage). newTestHandlerAndClient (sdk_roundtrip_helper_test.go)
// stands up a fresh backend/handler/client triple per call.
func TestRealClient_AccountsAndWorkspaces(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "account_links",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				toAccept, err := client.CreateAccountLinkInvitation(ctx, &wssdk.CreateAccountLinkInvitationInput{
					TargetAccountId: aws.String("111111111111"),
				})
				require.NoError(t, err)
				require.NotNil(t, toAccept.AccountLink)
				linkID := aws.ToString(toAccept.AccountLink.AccountLinkId)

				accepted, err := client.AcceptAccountLinkInvitation(ctx, &wssdk.AcceptAccountLinkInvitationInput{
					LinkId: aws.String(linkID),
				})
				require.NoError(t, err)
				require.NotNil(t, accepted.AccountLink)
				assert.Equal(t, wstypes.AccountLinkStatusEnumLinked, accepted.AccountLink.AccountLinkStatus)

				got, err := client.GetAccountLink(ctx, &wssdk.GetAccountLinkInput{LinkId: aws.String(linkID)})
				require.NoError(t, err)
				require.NotNil(t, got.AccountLink)
				assert.Equal(t, linkID, aws.ToString(got.AccountLink.AccountLinkId))

				toReject, err := client.CreateAccountLinkInvitation(ctx, &wssdk.CreateAccountLinkInvitationInput{
					TargetAccountId: aws.String("222222222222"),
				})
				require.NoError(t, err)
				rejectLinkID := aws.ToString(toReject.AccountLink.AccountLinkId)

				rejected, err := client.RejectAccountLinkInvitation(ctx, &wssdk.RejectAccountLinkInvitationInput{
					LinkId: aws.String(rejectLinkID),
				})
				require.NoError(t, err)
				require.NotNil(t, rejected.AccountLink)
				assert.Equal(t, wstypes.AccountLinkStatusEnumRejected, rejected.AccountLink.AccountLinkStatus)
			},
		},
		{
			name: "ip_groups",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				created, err := client.CreateIpGroup(ctx, &wssdk.CreateIpGroupInput{
					GroupName: aws.String("slice18-ipgroup"),
					UserRules: []wstypes.IpRuleItem{
						{IpRule: aws.String("10.0.0.0/24"), RuleDesc: aws.String("office")},
					},
				})
				require.NoError(t, err)
				groupID := aws.ToString(created.GroupId)

				_, err = client.AuthorizeIpRules(ctx, &wssdk.AuthorizeIpRulesInput{
					GroupId: aws.String(groupID),
					UserRules: []wstypes.IpRuleItem{
						{IpRule: aws.String("10.0.1.0/24"), RuleDesc: aws.String("branch")},
					},
				})
				require.NoError(t, err)

				desc, err := client.DescribeIpGroups(ctx, &wssdk.DescribeIpGroupsInput{GroupIds: []string{groupID}})
				require.NoError(t, err)
				require.Len(t, desc.Result, 1)
				assert.Len(t, desc.Result[0].UserRules, 2)

				_, err = client.RevokeIpRules(ctx, &wssdk.RevokeIpRulesInput{
					GroupId:   aws.String(groupID),
					UserRules: []string{"10.0.1.0/24"},
				})
				require.NoError(t, err)

				_, err = client.UpdateRulesOfIpGroup(ctx, &wssdk.UpdateRulesOfIpGroupInput{
					GroupId: aws.String(groupID),
					UserRules: []wstypes.IpRuleItem{
						{IpRule: aws.String("10.0.2.0/24"), RuleDesc: aws.String("replaced")},
					},
				})
				require.NoError(t, err)

				afterUpdate, err := client.DescribeIpGroups(
					ctx,
					&wssdk.DescribeIpGroupsInput{GroupIds: []string{groupID}},
				)
				require.NoError(t, err)
				require.Len(t, afterUpdate.Result, 1)
				require.Len(t, afterUpdate.Result[0].UserRules, 1)
				assert.Equal(t, "10.0.2.0/24", aws.ToString(afterUpdate.Result[0].UserRules[0].IpRule))

				_, err = client.AssociateIpGroups(ctx, &wssdk.AssociateIpGroupsInput{
					DirectoryId: aws.String("d-slice18"),
					GroupIds:    []string{groupID},
				})
				require.NoError(t, err)

				_, err = client.DisassociateIpGroups(ctx, &wssdk.DisassociateIpGroupsInput{
					DirectoryId: aws.String("d-slice18"),
					GroupIds:    []string{groupID},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.CreateTags(ctx, &wssdk.CreateTagsInput{
					ResourceId: aws.String("ws-slice18tags"),
					Tags:       []wstypes.Tag{{Key: aws.String("owner"), Value: aws.String("slice18")}},
				})
				require.NoError(t, err)

				desc, err := client.DescribeTags(
					ctx,
					&wssdk.DescribeTagsInput{ResourceId: aws.String("ws-slice18tags")},
				)
				require.NoError(t, err)
				require.Len(t, desc.TagList, 1)
				assert.Equal(t, "owner", aws.ToString(desc.TagList[0].Key))

				_, err = client.DeleteTags(ctx, &wssdk.DeleteTagsInput{
					ResourceId: aws.String("ws-slice18tags"),
					TagKeys:    []string{"owner"},
				})
				require.NoError(t, err)

				afterDelete, err := client.DescribeTags(
					ctx,
					&wssdk.DescribeTagsInput{ResourceId: aws.String("ws-slice18tags")},
				)
				require.NoError(t, err)
				assert.Empty(t, afterDelete.TagList)
			},
		},
		{
			name: "connect_client_addins",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				created, err := client.CreateConnectClientAddIn(ctx, &wssdk.CreateConnectClientAddInInput{
					Name:       aws.String("slice18-addin"),
					ResourceId: aws.String("d-slice18addin"),
					URL:        aws.String("https://example.com/addin"),
				})
				require.NoError(t, err)
				addInID := aws.ToString(created.AddInId)

				_, err = client.UpdateConnectClientAddIn(ctx, &wssdk.UpdateConnectClientAddInInput{
					AddInId:    aws.String(addInID),
					ResourceId: aws.String("d-slice18addin"),
					Name:       aws.String("slice18-addin-renamed"),
					URL:        aws.String("https://example.com/addin2"),
				})
				require.NoError(t, err)

				desc, err := client.DescribeConnectClientAddIns(ctx, &wssdk.DescribeConnectClientAddInsInput{
					ResourceId: aws.String("d-slice18addin"),
				})
				require.NoError(t, err)
				require.Len(t, desc.AddIns, 1)
				assert.Equal(t, "slice18-addin-renamed", aws.ToString(desc.AddIns[0].Name))

				_, err = client.DeleteConnectClientAddIn(ctx, &wssdk.DeleteConnectClientAddInInput{
					AddInId:    aws.String(addInID),
					ResourceId: aws.String("d-slice18addin"),
				})
				require.NoError(t, err)

				afterDelete, err := client.DescribeConnectClientAddIns(ctx, &wssdk.DescribeConnectClientAddInsInput{
					ResourceId: aws.String("d-slice18addin"),
				})
				require.NoError(t, err)
				assert.Empty(t, afterDelete.AddIns)
			},
		},
		{
			name: "connection_aliases",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				created, err := client.CreateConnectionAlias(ctx, &wssdk.CreateConnectionAliasInput{
					ConnectionString: aws.String("slice18.example.com"),
				})
				require.NoError(t, err)
				aliasID := aws.ToString(created.AliasId)

				assoc, err := client.AssociateConnectionAlias(ctx, &wssdk.AssociateConnectionAliasInput{
					AliasId:    aws.String(aliasID),
					ResourceId: aws.String("d-slice18alias"),
				})
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(assoc.ConnectionIdentifier))

				_, err = client.DisassociateConnectionAlias(ctx, &wssdk.DisassociateConnectionAliasInput{
					AliasId: aws.String(aliasID),
				})
				require.NoError(t, err)
			},
		},
		{
			name: "account",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				desc, err := client.DescribeAccount(ctx, &wssdk.DescribeAccountInput{})
				require.NoError(t, err)
				assert.NotEmpty(t, desc.DedicatedTenancySupport)

				mods, err := client.DescribeAccountModifications(ctx, &wssdk.DescribeAccountModificationsInput{})
				require.NoError(t, err)
				assert.NotNil(t, mods.AccountModifications)

				_, err = client.ModifyAccount(ctx, &wssdk.ModifyAccountInput{
					DedicatedTenancySupport: wstypes.DedicatedTenancySupportEnumEnabled,
				})
				require.NoError(t, err)

				cidrs, err := client.ListAvailableManagementCidrRanges(
					ctx,
					&wssdk.ListAvailableManagementCidrRangesInput{
						ManagementCidrRangeConstraint: aws.String("10.0.0.0/16"),
					},
				)
				require.NoError(t, err)
				assert.NotNil(t, cidrs.ManagementCidrRanges)
			},
		},
		{
			name: "client_branding",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.ImportClientBranding(ctx, &wssdk.ImportClientBrandingInput{
					ResourceId: aws.String("d-slice18branding"),
					DeviceTypeWeb: &wstypes.DefaultImportClientBrandingAttributes{
						SupportEmail: aws.String("support@example.com"),
					},
				})
				require.NoError(t, err)

				desc, err := client.DescribeClientBranding(ctx, &wssdk.DescribeClientBrandingInput{
					ResourceId: aws.String("d-slice18branding"),
				})
				require.NoError(t, err)
				require.NotNil(t, desc.DeviceTypeWeb)
				assert.Equal(t, "support@example.com", aws.ToString(desc.DeviceTypeWeb.SupportEmail))

				_, err = client.DeleteClientBranding(ctx, &wssdk.DeleteClientBrandingInput{
					ResourceId: aws.String("d-slice18branding"),
					Platforms:  []wstypes.ClientDeviceType{wstypes.ClientDeviceTypeDeviceTypeWeb},
				})
				require.NoError(t, err)

				afterDelete, err := client.DescribeClientBranding(ctx, &wssdk.DescribeClientBrandingInput{
					ResourceId: aws.String("d-slice18branding"),
				})
				require.NoError(t, err)
				assert.Nil(t, afterDelete.DeviceTypeWeb)

				_, err = client.ModifyClientProperties(ctx, &wssdk.ModifyClientPropertiesInput{
					ResourceId: aws.String("d-slice18props"),
					ClientProperties: &wstypes.ClientProperties{
						ReconnectEnabled: wstypes.ReconnectEnumEnabled,
						LogUploadEnabled: wstypes.LogUploadEnumEnabled,
					},
				})
				require.NoError(t, err)

				props, err := client.DescribeClientProperties(ctx, &wssdk.DescribeClientPropertiesInput{
					ResourceIds: []string{"d-slice18props"},
				})
				require.NoError(t, err)
				require.Len(t, props.ClientPropertiesList, 1)
				require.NotNil(t, props.ClientPropertiesList[0].ClientProperties)
				assert.Equal(
					t,
					wstypes.ReconnectEnumEnabled,
					props.ClientPropertiesList[0].ClientProperties.ReconnectEnabled,
				)
			},
		},
		{
			name: "workspace_lifecycle",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-slice18ws"),
				})
				require.NoError(t, err)

				created, err := client.CreateWorkspaces(ctx, &wssdk.CreateWorkspacesInput{
					Workspaces: []wstypes.WorkspaceRequest{
						{
							DirectoryId: aws.String("d-slice18ws"),
							UserName:    aws.String("alice"),
							BundleId:    aws.String("wsb-slice18"),
							WorkspaceProperties: &wstypes.WorkspaceProperties{
								RunningMode: wstypes.RunningModeAutoStop,
							},
						},
					},
				})
				require.NoError(t, err)
				require.Len(t, created.PendingRequests, 1)
				workspaceID := aws.ToString(created.PendingRequests[0].WorkspaceId)

				_, err = client.RebootWorkspaces(ctx, &wssdk.RebootWorkspacesInput{
					RebootWorkspaceRequests: []wstypes.RebootRequest{{WorkspaceId: aws.String(workspaceID)}},
				})
				require.NoError(t, err)

				connStatus, err := client.DescribeWorkspacesConnectionStatus(
					ctx,
					&wssdk.DescribeWorkspacesConnectionStatusInput{WorkspaceIds: []string{workspaceID}},
				)
				require.NoError(t, err)
				require.Len(t, connStatus.WorkspacesConnectionStatus, 1)

				stopped, err := client.StopWorkspaces(ctx, &wssdk.StopWorkspacesInput{
					StopWorkspaceRequests: []wstypes.StopRequest{{WorkspaceId: aws.String(workspaceID)}},
				})
				require.NoError(t, err)
				assert.Empty(t, stopped.FailedRequests, "AUTO_STOP workspace from AVAILABLE should stop cleanly")

				started, err := client.StartWorkspaces(ctx, &wssdk.StartWorkspacesInput{
					StartWorkspaceRequests: []wstypes.StartRequest{{WorkspaceId: aws.String(workspaceID)}},
				})
				require.NoError(t, err)
				assert.Empty(t, started.FailedRequests, "AUTO_STOP workspace from STOPPED should start cleanly")

				_, err = client.ModifyWorkspaceState(ctx, &wssdk.ModifyWorkspaceStateInput{
					WorkspaceId:    aws.String(workspaceID),
					WorkspaceState: wstypes.TargetWorkspaceStateAdminMaintenance,
				})
				require.NoError(t, err)

				_, err = client.ModifyWorkspaceState(ctx, &wssdk.ModifyWorkspaceStateInput{
					WorkspaceId:    aws.String(workspaceID),
					WorkspaceState: wstypes.TargetWorkspaceStateAvailable,
				})
				require.NoError(t, err)

				_, err = client.RebuildWorkspaces(ctx, &wssdk.RebuildWorkspacesInput{
					RebuildWorkspaceRequests: []wstypes.RebuildRequest{{WorkspaceId: aws.String(workspaceID)}},
				})
				require.NoError(t, err)

				_, err = client.RestoreWorkspace(
					ctx,
					&wssdk.RestoreWorkspaceInput{WorkspaceId: aws.String(workspaceID)},
				)
				require.NoError(t, err)

				migrated, err := client.MigrateWorkspace(ctx, &wssdk.MigrateWorkspaceInput{
					SourceWorkspaceId: aws.String(workspaceID),
					BundleId:          aws.String("wsb-slice18-target"),
				})
				require.NoError(t, err)
				assert.Equal(t, workspaceID, aws.ToString(migrated.SourceWorkspaceId))
				assert.NotEmpty(t, aws.ToString(migrated.TargetWorkspaceId))

				terminated, err := client.TerminateWorkspaces(ctx, &wssdk.TerminateWorkspacesInput{
					TerminateWorkspaceRequests: []wstypes.TerminateRequest{
						{WorkspaceId: aws.String(aws.ToString(migrated.TargetWorkspaceId))},
					},
				})
				require.NoError(t, err)
				assert.Empty(t, terminated.FailedRequests)
			},
		},
		{
			name: "bundles",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-slice18bundle"),
				})
				require.NoError(t, err)

				createdWS, err := client.CreateWorkspaces(ctx, &wssdk.CreateWorkspacesInput{
					Workspaces: []wstypes.WorkspaceRequest{
						{
							DirectoryId: aws.String("d-slice18bundle"),
							UserName:    aws.String("dana"),
							BundleId:    aws.String("wsb-slice18bundlesrc"),
						},
					},
				})
				require.NoError(t, err)
				require.Len(t, createdWS.PendingRequests, 1)

				sourceImg, err := client.CreateWorkspaceImage(ctx, &wssdk.CreateWorkspaceImageInput{
					Name:        aws.String("slice18-bundle-image"),
					Description: aws.String("source for bundle"),
					WorkspaceId: createdWS.PendingRequests[0].WorkspaceId,
				})
				require.NoError(t, err)

				created, err := client.CreateWorkspaceBundle(ctx, &wssdk.CreateWorkspaceBundleInput{
					BundleName:        aws.String("slice18-bundle"),
					BundleDescription: aws.String("slice18 bundle"),
					ImageId:           sourceImg.ImageId,
					ComputeType:       &wstypes.ComputeType{Name: wstypes.ComputeValue},
					UserStorage:       &wstypes.UserStorage{Capacity: aws.String("50")},
				})
				require.NoError(t, err)
				require.NotNil(t, created.WorkspaceBundle)
				bundleID := aws.ToString(created.WorkspaceBundle.BundleId)

				desc, err := client.DescribeWorkspaceBundles(ctx, &wssdk.DescribeWorkspaceBundlesInput{
					BundleIds: []string{bundleID},
				})
				require.NoError(t, err)
				require.Len(t, desc.Bundles, 1)
				assert.Equal(t, "slice18-bundle", aws.ToString(desc.Bundles[0].Name))

				bundleAssoc, err := client.DescribeBundleAssociations(ctx, &wssdk.DescribeBundleAssociationsInput{
					BundleId: aws.String(bundleID),
					AssociatedResourceTypes: []wstypes.BundleAssociatedResourceType{
						wstypes.BundleAssociatedResourceTypeApplication,
					},
				})
				require.NoError(t, err)
				assert.NotNil(t, bundleAssoc.Associations)

				_, err = client.UpdateWorkspaceBundle(ctx, &wssdk.UpdateWorkspaceBundleInput{
					BundleId: aws.String(bundleID),
					ImageId:  sourceImg.ImageId,
				})
				require.NoError(t, err)

				_, err = client.DeleteWorkspaceBundle(
					ctx,
					&wssdk.DeleteWorkspaceBundleInput{BundleId: aws.String(bundleID)},
				)
				require.NoError(t, err)

				afterDelete, err := client.DescribeWorkspaceBundles(ctx, &wssdk.DescribeWorkspaceBundlesInput{
					BundleIds: []string{bundleID},
				})
				require.NoError(t, err)
				assert.Empty(t, afterDelete.Bundles)
			},
		},
		{
			name: "images",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-slice18img"),
				})
				require.NoError(t, err)

				createdWS, err := client.CreateWorkspaces(ctx, &wssdk.CreateWorkspacesInput{
					Workspaces: []wstypes.WorkspaceRequest{
						{
							DirectoryId: aws.String("d-slice18img"),
							UserName:    aws.String("bob"),
							BundleId:    aws.String("wsb-slice18img"),
						},
					},
				})
				require.NoError(t, err)
				require.Len(t, createdWS.PendingRequests, 1)
				workspaceID := aws.ToString(createdWS.PendingRequests[0].WorkspaceId)

				sourceImg, err := client.CreateWorkspaceImage(ctx, &wssdk.CreateWorkspaceImageInput{
					Name:        aws.String("slice18-source-image"),
					Description: aws.String("source"),
					WorkspaceId: aws.String(workspaceID),
				})
				require.NoError(t, err)
				sourceImageID := aws.ToString(sourceImg.ImageId)

				updated, err := client.CreateUpdatedWorkspaceImage(ctx, &wssdk.CreateUpdatedWorkspaceImageInput{
					SourceImageId: aws.String(sourceImageID),
					Name:          aws.String("slice18-updated-image"),
					Description:   aws.String("updated"),
				})
				require.NoError(t, err)
				updatedImageID := aws.ToString(updated.ImageId)
				require.NotEmpty(t, updatedImageID)

				assoc, err := client.DescribeImageAssociations(ctx, &wssdk.DescribeImageAssociationsInput{
					ImageId: aws.String(sourceImageID),
					AssociatedResourceTypes: []wstypes.ImageAssociatedResourceType{
						wstypes.ImageAssociatedResourceTypeApplication,
					},
				})
				require.NoError(t, err)
				assert.NotNil(t, assoc.Associations)

				_, err = client.DeleteWorkspaceImage(
					ctx,
					&wssdk.DeleteWorkspaceImageInput{ImageId: aws.String(updatedImageID)},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "application_associations",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-slice18app"),
				})
				require.NoError(t, err)

				createdWS, err := client.CreateWorkspaces(ctx, &wssdk.CreateWorkspacesInput{
					Workspaces: []wstypes.WorkspaceRequest{
						{
							DirectoryId: aws.String("d-slice18app"),
							UserName:    aws.String("carol"),
							BundleId:    aws.String("wsb-slice18app"),
						},
					},
				})
				require.NoError(t, err)
				require.Len(t, createdWS.PendingRequests, 1)
				workspaceID := aws.ToString(createdWS.PendingRequests[0].WorkspaceId)

				_, err = client.AssociateWorkspaceApplication(ctx, &wssdk.AssociateWorkspaceApplicationInput{
					WorkspaceId:   aws.String(workspaceID),
					ApplicationId: aws.String("wsa-slice18"),
				})
				require.NoError(t, err)

				apps, err := client.DescribeApplications(ctx, &wssdk.DescribeApplicationsInput{})
				require.NoError(t, err)
				assert.NotNil(t, apps.Applications)

				wsAssoc, err := client.DescribeWorkspaceAssociations(ctx, &wssdk.DescribeWorkspaceAssociationsInput{
					WorkspaceId: aws.String(workspaceID),
					AssociatedResourceTypes: []wstypes.WorkSpaceAssociatedResourceType{
						wstypes.WorkSpaceAssociatedResourceTypeApplication,
					},
				})
				require.NoError(t, err)
				require.Len(t, wsAssoc.Associations, 1)
				assert.Equal(t, "wsa-slice18", aws.ToString(wsAssoc.Associations[0].AssociatedResourceId))

				deployed, err := client.DeployWorkspaceApplications(ctx, &wssdk.DeployWorkspaceApplicationsInput{
					WorkspaceId: aws.String(workspaceID),
				})
				require.NoError(t, err)
				assert.NotNil(t, deployed.Deployment)

				disassoc, err := client.DisassociateWorkspaceApplication(
					ctx,
					&wssdk.DisassociateWorkspaceApplicationInput{
						WorkspaceId:   aws.String(workspaceID),
						ApplicationId: aws.String("wsa-slice18"),
					},
				)
				require.NoError(t, err)
				require.NotNil(t, disassoc.Association)
				assert.Equal(t, wstypes.AssociationStateRemoved, disassoc.Association.State)
			},
		},
		{
			name: "pools",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				created, err := client.CreateWorkspacesPool(ctx, &wssdk.CreateWorkspacesPoolInput{
					PoolName:    aws.String("slice18-pool"),
					Description: aws.String("slice18 pool"),
					BundleId:    aws.String("wsb-slice18pool"),
					DirectoryId: aws.String("d-slice18pool"),
					RunningMode: wstypes.PoolsRunningModeAlwaysOn,
					Capacity:    &wstypes.Capacity{DesiredUserSessions: aws.Int32(3)},
				})
				require.NoError(t, err)
				require.NotNil(t, created.WorkspacesPool)
				poolID := aws.ToString(created.WorkspacesPool.PoolId)

				_, err = client.StartWorkspacesPool(ctx, &wssdk.StartWorkspacesPoolInput{PoolId: aws.String(poolID)})
				require.NoError(t, err)

				updated, err := client.UpdateWorkspacesPool(ctx, &wssdk.UpdateWorkspacesPoolInput{
					PoolId:      aws.String(poolID),
					Description: aws.String("updated description"),
					Capacity:    &wstypes.Capacity{DesiredUserSessions: aws.Int32(5)},
				})
				require.NoError(t, err)
				require.NotNil(t, updated.WorkspacesPool)
				assert.Equal(t, "updated description", aws.ToString(updated.WorkspacesPool.Description))

				sessions, err := client.DescribeWorkspacesPoolSessions(ctx, &wssdk.DescribeWorkspacesPoolSessionsInput{
					PoolId: aws.String(poolID),
				})
				require.NoError(t, err)
				assert.Empty(t, sessions.Sessions)

				// This backend never synthesizes a live pool session (no simulated
				// user-connects-to-pool path exists), so TerminateWorkspacesPoolSession
				// can only ever be exercised against a nonexistent session here --
				// still a real wire round trip (request encode + error decode)
				// through the typed client.
				_, err = client.TerminateWorkspacesPoolSession(ctx, &wssdk.TerminateWorkspacesPoolSessionInput{
					SessionId: aws.String("slice18-nonexistent-session"),
				})
				require.Error(t, err)

				_, err = client.StopWorkspacesPool(ctx, &wssdk.StopWorkspacesPoolInput{PoolId: aws.String(poolID)})
				require.NoError(t, err)

				_, err = client.TerminateWorkspacesPool(
					ctx,
					&wssdk.TerminateWorkspacesPoolInput{PoolId: aws.String(poolID)},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "snapshots",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				desc, err := client.DescribeWorkspaceSnapshots(ctx, &wssdk.DescribeWorkspaceSnapshotsInput{
					WorkspaceId: aws.String("ws-slice18snap"),
				})
				require.NoError(t, err)
				assert.NotNil(t, desc.RebuildSnapshots)
				assert.NotNil(t, desc.RestoreSnapshots)
			},
		},
		{
			name: "directories_and_streaming",
			run: func(t *testing.T) {
				t.Helper()

				ctx := t.Context()
				client := newTestHandlerAndClient(t)

				_, err := client.RegisterWorkspaceDirectory(ctx, &wssdk.RegisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-slice18stream"),
				})
				require.NoError(t, err)

				_, err = client.ModifyStreamingProperties(ctx, &wssdk.ModifyStreamingPropertiesInput{
					ResourceId: aws.String("d-slice18stream"),
					StreamingProperties: &wstypes.StreamingProperties{
						StreamingExperiencePreferredProtocol: wstypes.StreamingExperiencePreferredProtocolEnumTcp,
					},
				})
				require.NoError(t, err)

				_, err = client.DeregisterWorkspaceDirectory(ctx, &wssdk.DeregisterWorkspaceDirectoryInput{
					DirectoryId: aws.String("d-slice18stream"),
				})
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}
