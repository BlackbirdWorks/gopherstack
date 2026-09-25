package terraform_test

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	accountsvc "github.com/aws/aws-sdk-go-v2/service/account"
	directconnectsvc "github.com/aws/aws-sdk-go-v2/service/directconnect"
	grafanasvc "github.com/aws/aws-sdk-go-v2/service/grafana"
	identitystoresvc "github.com/aws/aws-sdk-go-v2/service/identitystore"
	opsworkssvc "github.com/aws/aws-sdk-go-v2/service/opsworks"
	ssoadminsvc "github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestTerraform_AccountDirectconnectOpsworksAndGrafana provisions Account, Direct Connect gateway/LAG,
// OpsWorks stack/layer/app/user-profile/permission, and Grafana workspace
// API key/role association resources via Terraform and verifies each
// through its own SDK client's Describe/List/Get path.
func TestTerraform_AccountDirectconnectOpsworksAndGrafana(t *testing.T) {
	t.Parallel()

	tests := []tfTestCase{
		{
			name:    "success",
			fixture: "account-directconnect-opsworks-and-grafana",
			setup: func(t *testing.T, _ string) map[string]any {
				t.Helper()

				return map[string]any{}
			},
			verify: func(t *testing.T, ctx context.Context, _ map[string]any) {
				t.Helper()
				cfg := megaConfig(t)

				acctClient := accountsvc.NewFromConfig(cfg, func(o *accountsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})
				contactOut, err := acctClient.GetContactInformation(ctx, &accountsvc.GetContactInformationInput{})
				require.NoError(t, err, "GetContactInformation should succeed")
				require.NotNil(t, contactOut.ContactInformation)
				assert.Equal(t, "Adog", aws.ToString(contactOut.ContactInformation.FullName))

				regionOut, err := acctClient.GetRegionOptStatus(ctx, &accountsvc.GetRegionOptStatusInput{
					RegionName: aws.String("af-south-1"),
				})
				require.NoError(t, err, "GetRegionOptStatus should succeed")
				assert.Equal(t, "ENABLED", string(regionOut.RegionOptStatus))

				dxClient := directconnectsvc.NewFromConfig(cfg, func(o *directconnectsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				gwOut, err := dxClient.DescribeDirectConnectGateways(
					ctx,
					&directconnectsvc.DescribeDirectConnectGatewaysInput{},
				)
				require.NoError(t, err, "DescribeDirectConnectGateways should succeed")

				var dxGatewayID string

				for _, gw := range gwOut.DirectConnectGateways {
					if aws.ToString(gw.DirectConnectGatewayName) == "adog-dxgw" {
						dxGatewayID = aws.ToString(gw.DirectConnectGatewayId)
					}
				}

				require.NotEmpty(t, dxGatewayID, "direct connect gateway should be listed")

				require.Eventually(t, func() bool {
					assocOut, assocErr := dxClient.DescribeDirectConnectGatewayAssociations(
						ctx,
						&directconnectsvc.DescribeDirectConnectGatewayAssociationsInput{
							DirectConnectGatewayId: aws.String(dxGatewayID),
						},
					)
					if assocErr != nil || len(assocOut.DirectConnectGatewayAssociations) != 1 {
						return false
					}

					return assocOut.DirectConnectGatewayAssociations[0].AssociationState == "associated"
				}, 5*time.Second, 50*time.Millisecond, "gateway association should reach associated state")

				lagsOut, err := dxClient.DescribeLags(ctx, &directconnectsvc.DescribeLagsInput{})
				require.NoError(t, err, "DescribeLags should succeed")

				var foundLag bool

				for _, lag := range lagsOut.Lags {
					if aws.ToString(lag.LagName) == "adog-lag" {
						foundLag = true

						assert.Equal(t, "1Gbps", aws.ToString(lag.ConnectionsBandwidth))
					}
				}

				assert.True(t, foundLag, "lag adog-lag should be listed")

				opsClient := opsworkssvc.NewFromConfig(cfg, func(o *opsworkssvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				stacksOut, err := opsClient.DescribeStacks(ctx, &opsworkssvc.DescribeStacksInput{})
				require.NoError(t, err, "DescribeStacks should succeed")

				var stackID string

				for _, s := range stacksOut.Stacks {
					if aws.ToString(s.Name) == "adog-stack" {
						stackID = aws.ToString(s.StackId)
					}
				}

				require.NotEmpty(t, stackID, "opsworks stack should be listed")

				layersOut, err := opsClient.DescribeLayers(
					ctx,
					&opsworkssvc.DescribeLayersInput{StackId: aws.String(stackID)},
				)
				require.NoError(t, err, "DescribeLayers should succeed")
				require.Len(t, layersOut.Layers, 1)
				assert.Equal(t, "adog-layer", aws.ToString(layersOut.Layers[0].Name))
				assert.Equal(t, "adoglayer", aws.ToString(layersOut.Layers[0].Shortname))

				appsOut, err := opsClient.DescribeApps(
					ctx,
					&opsworkssvc.DescribeAppsInput{StackId: aws.String(stackID)},
				)
				require.NoError(t, err, "DescribeApps should succeed")
				require.Len(t, appsOut.Apps, 1)
				assert.Equal(t, "adog-app", aws.ToString(appsOut.Apps[0].Name))

				const userArn = "arn:aws:iam::000000000000:user/adog-user"

				profilesOut, err := opsClient.DescribeUserProfiles(ctx, &opsworkssvc.DescribeUserProfilesInput{
					IamUserArns: []string{userArn},
				})
				require.NoError(t, err, "DescribeUserProfiles should succeed")
				require.Len(t, profilesOut.UserProfiles, 1)
				assert.Equal(t, "adog-user", aws.ToString(profilesOut.UserProfiles[0].SshUsername))

				permsOut, err := opsClient.DescribePermissions(
					ctx,
					&opsworkssvc.DescribePermissionsInput{StackId: aws.String(stackID)},
				)
				require.NoError(t, err, "DescribePermissions should succeed")

				var foundPermission bool

				for _, p := range permsOut.Permissions {
					if aws.ToString(p.IamUserArn) == userArn {
						foundPermission = true

						assert.Equal(t, "deploy", aws.ToString(p.Level))
					}
				}

				assert.True(t, foundPermission, "opsworks permission for the user should be listed")

				grafanaClient := grafanasvc.NewFromConfig(cfg, func(o *grafanasvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				workspacesOut, err := grafanaClient.ListWorkspaces(ctx, &grafanasvc.ListWorkspacesInput{})
				require.NoError(t, err, "ListWorkspaces should succeed")

				var workspaceID string

				for _, w := range workspacesOut.Workspaces {
					if aws.ToString(w.Name) == "adog-grafana" {
						workspaceID = aws.ToString(w.Id)
					}
				}

				require.NotEmpty(t, workspaceID, "grafana workspace should be listed")

				ssoClient := ssoadminsvc.NewFromConfig(cfg, func(o *ssoadminsvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				instancesOut, err := ssoClient.ListInstances(ctx, &ssoadminsvc.ListInstancesInput{})
				require.NoError(t, err, "ListInstances should succeed")
				require.NotEmpty(t, instancesOut.Instances, "an IAM Identity Center instance should exist")

				identityStoreID := aws.ToString(instancesOut.Instances[0].IdentityStoreId)

				isClient := identitystoresvc.NewFromConfig(cfg, func(o *identitystoresvc.Options) {
					o.BaseEndpoint = aws.String(endpoint)
				})

				usersOut, err := isClient.ListUsers(ctx, &identitystoresvc.ListUsersInput{
					IdentityStoreId: aws.String(identityStoreID),
				})
				require.NoError(t, err, "ListUsers should succeed")

				var userID string

				for _, u := range usersOut.Users {
					if aws.ToString(u.UserName) == "adog-user" {
						userID = aws.ToString(u.UserId)
					}
				}

				require.NotEmpty(t, userID, "identity store user should be listed")

				permissionsOut, err := grafanaClient.ListPermissions(ctx, &grafanasvc.ListPermissionsInput{
					WorkspaceId: aws.String(workspaceID),
				})
				require.NoError(t, err, "ListPermissions should succeed")

				var foundRoleAssociation bool

				for _, p := range permissionsOut.Permissions {
					if p.User != nil && aws.ToString(p.User.Id) == userID {
						foundRoleAssociation = true

						assert.Equal(t, "ADMIN", string(p.Role))
					}
				}

				assert.True(t, foundRoleAssociation, "grafana role association should be listed")
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runTFTest(t, tc)
		})
	}
}
