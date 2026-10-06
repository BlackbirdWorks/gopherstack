package workspaces_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	wssdk "github.com/aws/aws-sdk-go-v2/service/workspaces"
	"github.com/aws/aws-sdk-go-v2/service/workspaces/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workspaces"
)

func describeDir(t *testing.T, c *wssdk.Client, id string) types.WorkspaceDirectory {
	t.Helper()

	out, err := c.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
		DirectoryIds: []string{id},
	})
	require.NoError(t, err)
	require.Len(t, out.Directories, 1)

	return out.Directories[0]
}

func TestRegisterWorkspaceDirectory_RegistrationMembersRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in       *wssdk.RegisterWorkspaceDirectoryInput
		check    func(t *testing.T, d types.WorkspaceDirectory)
		name     string
		wantCode string
	}{
		{
			name: "defaults",
			in:   &wssdk.RegisterWorkspaceDirectoryInput{DirectoryId: aws.String("d-1")},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				assert.Equal(t, types.WorkspaceTypePersonal, d.WorkspaceType)
				assert.Equal(t, types.TenancyShared, d.Tenancy)
				assert.Equal(t, types.UserIdentityTypeAwsDirectoryService, d.UserIdentityType)
			},
		},
		{
			name: "explicit members",
			in: &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId:                   aws.String("d-1"),
				WorkspaceType:                 types.WorkspaceTypePools,
				Tenancy:                       types.TenancyDedicated,
				UserIdentityType:              types.UserIdentityTypeCustomerManaged,
				WorkspaceDirectoryName:        aws.String("named"),
				WorkspaceDirectoryDescription: aws.String("described"),
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				assert.Equal(t, types.WorkspaceTypePools, d.WorkspaceType)
				assert.Equal(t, types.TenancyDedicated, d.Tenancy)
				assert.Equal(t, types.UserIdentityTypeCustomerManaged, d.UserIdentityType)
				assert.Equal(t, "named", aws.ToString(d.WorkspaceDirectoryName))
				assert.Equal(t, "described", aws.ToString(d.WorkspaceDirectoryDescription))
			},
		},
		{
			name: "active directory config",
			in: &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId: aws.String("d-1"),
				ActiveDirectoryConfig: &types.ActiveDirectoryConfig{
					DomainName:              aws.String("corp.example.com"),
					ServiceAccountSecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:s"),
				},
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				require.NotNil(t, d.ActiveDirectoryConfig)
				assert.Equal(t, "corp.example.com", aws.ToString(d.ActiveDirectoryConfig.DomainName))
				assert.Equal(t, types.UserIdentityTypeCustomerManaged, d.UserIdentityType)
			},
		},
		{
			name: "entra config without directory id",
			in: &wssdk.RegisterWorkspaceDirectoryInput{
				MicrosoftEntraConfig: &types.MicrosoftEntraConfig{
					TenantId:                   aws.String("tenant-1"),
					ApplicationConfigSecretArn: aws.String("arn:aws:secretsmanager:us-east-1:000000000000:secret:e"),
				},
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				require.NotNil(t, d.MicrosoftEntraConfig)
				assert.Equal(t, "tenant-1", aws.ToString(d.MicrosoftEntraConfig.TenantId))
			},
		},
		{
			name: "identity center without directory id",
			in: &wssdk.RegisterWorkspaceDirectoryInput{
				IdcInstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1"),
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				require.NotNil(t, d.IDCConfig)
				assert.Equal(t, "arn:aws:sso:::instance/ssoins-1", aws.ToString(d.IDCConfig.InstanceArn))
				assert.Equal(t, types.UserIdentityTypeAwsIamIdentityCenter, d.UserIdentityType)
				assert.Equal(t, types.WorkspaceDirectoryTypeAwsIamIdentityCenter, d.DirectoryType)
			},
		},
		{
			name:     "invalid workspace type",
			in:       &wssdk.RegisterWorkspaceDirectoryInput{DirectoryId: aws.String("d-1"), WorkspaceType: "BOGUS"},
			wantCode: "InvalidParameterValuesException",
		},
		{
			name:     "missing directory id",
			in:       &wssdk.RegisterWorkspaceDirectoryInput{},
			wantCode: "InvalidParameterValuesException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			out, err := c.RegisterWorkspaceDirectory(t.Context(), tc.in)

			if tc.wantCode != "" {
				require.ErrorContains(t, err, tc.wantCode)

				return
			}

			require.NoError(t, err)
			require.NotEmpty(t, aws.ToString(out.DirectoryId))
			tc.check(t, describeDir(t, c, aws.ToString(out.DirectoryId)))
		})
	}
}

func TestDescribeWorkspaceDirectories_Filters(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name     string
		wantCode string
		filters  []types.DescribeWorkspaceDirectoriesFilter
		wantIDs  []string
	}{
		{
			name: "workspace type pools",
			filters: []types.DescribeWorkspaceDirectoriesFilter{
				{Name: types.DescribeWorkspaceDirectoriesFilterNameWorkspaceType, Values: []string{"POOLS"}},
			},
			wantIDs: []string{"d-pools"},
		},
		{
			name: "workspace type personal includes default",
			filters: []types.DescribeWorkspaceDirectoriesFilter{
				{Name: types.DescribeWorkspaceDirectoriesFilterNameWorkspaceType, Values: []string{"PERSONAL"}},
			},
			wantIDs: []string{"d-default", "d-managed"},
		},
		{
			name: "and across filters",
			filters: []types.DescribeWorkspaceDirectoriesFilter{
				{Name: types.DescribeWorkspaceDirectoriesFilterNameWorkspaceType, Values: []string{"PERSONAL"}},
				{
					Name:   types.DescribeWorkspaceDirectoriesFilterNameUserIdentityType,
					Values: []string{"CUSTOMER_MANAGED"},
				},
			},
			wantIDs: []string{"d-managed"},
		},
		{
			name: "or within values",
			filters: []types.DescribeWorkspaceDirectoriesFilter{
				{
					Name:   types.DescribeWorkspaceDirectoriesFilterNameUserIdentityType,
					Values: []string{"CUSTOMER_MANAGED", "AWS_DIRECTORY_SERVICE"},
				},
			},
			wantIDs: []string{"d-default", "d-managed", "d-pools"},
		},
		{
			name:     "bad filter name",
			filters:  []types.DescribeWorkspaceDirectoriesFilter{{Name: "BOGUS", Values: []string{"x"}}},
			wantCode: "InvalidParameterValuesException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)

			for _, in := range []*wssdk.RegisterWorkspaceDirectoryInput{
				{DirectoryId: aws.String("d-default")},
				{DirectoryId: aws.String("d-pools"), WorkspaceType: types.WorkspaceTypePools},
				{DirectoryId: aws.String("d-managed"), UserIdentityType: types.UserIdentityTypeCustomerManaged},
			} {
				_, err := c.RegisterWorkspaceDirectory(t.Context(), in)
				require.NoError(t, err)
			}

			out, err := c.DescribeWorkspaceDirectories(t.Context(), &wssdk.DescribeWorkspaceDirectoriesInput{
				Filters: tc.filters,
			})

			if tc.wantCode != "" {
				require.ErrorContains(t, err, tc.wantCode)

				return
			}

			require.NoError(t, err)

			got := make([]string, 0, len(out.Directories))
			for _, d := range out.Directories {
				got = append(got, aws.ToString(d.DirectoryId))
			}

			assert.Equal(t, tc.wantIDs, got)
		})
	}
}

func TestModifyDirectoryProperties_PartialKeepsOmittedMembers(t *testing.T) {
	t.Parallel()

	cases := []struct {
		modify func(t *testing.T, c *wssdk.Client)
		check  func(t *testing.T, d types.WorkspaceDirectory)
		name   string
	}{
		{
			name: "saml status only",
			modify: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				_, err := c.ModifySamlProperties(t.Context(), &wssdk.ModifySamlPropertiesInput{
					ResourceId:     aws.String("d-1"),
					SamlProperties: &types.SamlProperties{Status: types.SamlStatusEnumDisabled},
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				assert.Equal(t, types.SamlStatusEnumDisabled, d.SamlProperties.Status)
				assert.Equal(t, "https://idp.example.com", aws.ToString(d.SamlProperties.UserAccessUrl))
			},
		},
		{
			name: "saml properties to delete",
			modify: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				_, err := c.ModifySamlProperties(t.Context(), &wssdk.ModifySamlPropertiesInput{
					ResourceId: aws.String("d-1"),
					PropertiesToDelete: []types.DeletableSamlProperty{
						types.DeletableSamlPropertySamlPropertiesUserAccessUrl,
					},
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				assert.Equal(t, types.SamlStatusEnumEnabled, d.SamlProperties.Status)
				assert.Empty(t, aws.ToString(d.SamlProperties.UserAccessUrl))
			},
		},
		{
			name: "selfservice single member",
			modify: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				_, err := c.ModifySelfservicePermissions(t.Context(), &wssdk.ModifySelfservicePermissionsInput{
					ResourceId:             aws.String("d-1"),
					SelfservicePermissions: &types.SelfservicePermissions{RestartWorkspace: types.ReconnectEnumEnabled},
				})
				require.NoError(t, err)
				_, err = c.ModifySelfservicePermissions(t.Context(), &wssdk.ModifySelfservicePermissionsInput{
					ResourceId:             aws.String("d-1"),
					SelfservicePermissions: &types.SelfservicePermissions{RebuildWorkspace: types.ReconnectEnumEnabled},
				})
				require.NoError(t, err)
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				assert.Equal(t, types.ReconnectEnumEnabled, d.SelfservicePermissions.RestartWorkspace)
				assert.Equal(t, types.ReconnectEnumEnabled, d.SelfservicePermissions.RebuildWorkspace)
			},
		},
		{
			name: "creation properties bool only",
			modify: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				for _, props := range []*types.WorkspaceCreationProperties{
					{EnableInternetAccess: aws.Bool(true), DefaultOu: aws.String("OU=x")},
					{EnableMaintenanceMode: aws.Bool(false)},
				} {
					_, err := c.ModifyWorkspaceCreationProperties(
						t.Context(), &wssdk.ModifyWorkspaceCreationPropertiesInput{
							ResourceId: aws.String("d-1"), WorkspaceCreationProperties: props,
						})
					require.NoError(t, err)
				}
			},
			check: func(t *testing.T, d types.WorkspaceDirectory) {
				t.Helper()
				require.NotNil(t, d.WorkspaceCreationProperties.EnableInternetAccess)
				assert.True(t, *d.WorkspaceCreationProperties.EnableInternetAccess)
				assert.Equal(t, "OU=x", aws.ToString(d.WorkspaceCreationProperties.DefaultOu))
				require.NotNil(t, d.WorkspaceCreationProperties.EnableMaintenanceMode)
				assert.False(t, *d.WorkspaceCreationProperties.EnableMaintenanceMode)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			_, err := c.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId: aws.String("d-1"),
			})
			require.NoError(t, err)

			_, err = c.ModifySamlProperties(t.Context(), &wssdk.ModifySamlPropertiesInput{
				ResourceId: aws.String("d-1"),
				SamlProperties: &types.SamlProperties{
					Status:        types.SamlStatusEnumEnabled,
					UserAccessUrl: aws.String("https://idp.example.com"),
				},
			})
			require.NoError(t, err)

			tc.modify(t, c)
			tc.check(t, describeDir(t, c, "d-1"))
		})
	}
}

func TestModifyStreamingProperties_AllMembersRoundTrip(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name   string
		first  types.StreamingProperties
		second types.StreamingProperties
	}{
		{
			name: "user settings survive protocol-only update",
			first: types.StreamingProperties{
				UserSettings: []types.UserSetting{
					{
						Action:        types.UserSettingActionEnumClipboardCopyToLocalDevice,
						Permission:    types.UserSettingPermissionEnumEnabled,
						MaximumLength: aws.Int32(100),
					},
				},
				StorageConnectors: []types.StorageConnector{
					{
						ConnectorType: types.StorageConnectorTypeEnumHomeFolder,
						Status:        types.StorageConnectorStatusEnumEnabled,
					},
				},
				GlobalAccelerator: &types.GlobalAcceleratorForDirectory{Mode: types.AGAModeForDirectoryEnumEnabledAuto},
			},
			second: types.StreamingProperties{
				StreamingExperiencePreferredProtocol: types.StreamingExperiencePreferredProtocolEnumUdp,
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			_, err := c.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId: aws.String("d-1"),
			})
			require.NoError(t, err)

			require.Nil(t, describeDir(t, c, "d-1").StreamingProperties)

			for _, sp := range []types.StreamingProperties{tc.first, tc.second} {
				_, err = c.ModifyStreamingProperties(t.Context(), &wssdk.ModifyStreamingPropertiesInput{
					ResourceId: aws.String("d-1"), StreamingProperties: &sp,
				})
				require.NoError(t, err)
			}

			got := describeDir(t, c, "d-1").StreamingProperties
			require.NotNil(t, got)
			assert.Equal(t, types.StreamingExperiencePreferredProtocolEnumUdp, got.StreamingExperiencePreferredProtocol)
			require.Len(t, got.UserSettings, 1)
			assert.Equal(t, types.UserSettingActionEnumClipboardCopyToLocalDevice, got.UserSettings[0].Action)
			assert.Equal(t, int32(100), aws.ToInt32(got.UserSettings[0].MaximumLength))
			require.Len(t, got.StorageConnectors, 1)
			require.NotNil(t, got.GlobalAccelerator)
			assert.Equal(t, types.AGAModeForDirectoryEnumEnabledAuto, got.GlobalAccelerator.Mode)
		})
	}
}

func TestDescribeWorkspaces_WorkspaceNameFilter(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name  string
		query string
		want  int
	}{
		{name: "match", query: "ws-a", want: 1},
		{name: "no match", query: "nope", want: 0},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			_, err := c.RegisterWorkspaceDirectory(t.Context(), &wssdk.RegisterWorkspaceDirectoryInput{
				DirectoryId: aws.String("d-1"),
			})
			require.NoError(t, err)

			for _, name := range []string{"ws-a", "ws-b"} {
				_, err = c.CreateWorkspaces(t.Context(), &wssdk.CreateWorkspacesInput{
					Workspaces: []types.WorkspaceRequest{{
						BundleId: aws.String("wsb-00000000"), DirectoryId: aws.String("d-1"),
						UserName: aws.String("[UNDEFINED]"), WorkspaceName: aws.String(name),
					}},
				})
				require.NoError(t, err)
			}

			out, err := c.DescribeWorkspaces(t.Context(), &wssdk.DescribeWorkspacesInput{
				WorkspaceName: aws.String(tc.query),
			})
			require.NoError(t, err)
			assert.Len(t, out.Workspaces, tc.want)
		})
	}
}

func TestModifyWorkspaceProperties_DataReplication(t *testing.T) {
	t.Parallel()

	cases := []struct {
		mode     types.DataReplication
		name     string
		wantCode string
	}{
		{name: "primary as source", mode: types.DataReplicationPrimaryAsSource},
		{name: "no replication", mode: types.DataReplicationNoReplication},
		{name: "invalid", mode: "BOGUS", wantCode: "InvalidParameterValuesException"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			id := createSDKWorkspace(t, c)

			_, err := c.ModifyWorkspaceProperties(t.Context(), &wssdk.ModifyWorkspacePropertiesInput{
				WorkspaceId: aws.String(id), DataReplication: tc.mode,
			})

			if tc.wantCode != "" {
				require.ErrorContains(t, err, tc.wantCode)

				return
			}

			require.NoError(t, err)

			out, err := c.DescribeWorkspaces(t.Context(), &wssdk.DescribeWorkspacesInput{WorkspaceIds: []string{id}})
			require.NoError(t, err)
			require.Len(t, out.Workspaces, 1)
			require.NotNil(t, out.Workspaces[0].DataReplicationSettings)
			assert.Equal(t, tc.mode, out.Workspaces[0].DataReplicationSettings.DataReplication)
		})
	}
}

func TestCustomBundle_DescribeReportsStateAndTimes(t *testing.T) {
	t.Parallel()

	c := newTestHandlerAndClient(t)
	img, err := c.CreateWorkspaceImage(t.Context(), &wssdk.CreateWorkspaceImageInput{
		Name: aws.String("src"), Description: aws.String("d"), WorkspaceId: aws.String(createSDKWorkspace(t, c)),
	})
	require.NoError(t, err)

	created, err := c.CreateWorkspaceBundle(t.Context(), &wssdk.CreateWorkspaceBundleInput{
		BundleName:        aws.String("b"),
		BundleDescription: aws.String("d"),
		ComputeType:       &types.ComputeType{Name: types.ComputeValue},
		ImageId:           img.ImageId,
		UserStorage:       &types.UserStorage{Capacity: aws.String("50")},
		RootStorage:       &types.RootStorage{Capacity: aws.String("80")},
	})
	require.NoError(t, err)

	out, err := c.DescribeWorkspaceBundles(t.Context(), &wssdk.DescribeWorkspaceBundlesInput{
		BundleIds: []string{aws.ToString(created.WorkspaceBundle.BundleId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Bundles, 1)

	b := out.Bundles[0]
	assert.Equal(t, types.BundleTypeRegular, b.BundleType)
	assert.Equal(t, types.WorkspaceBundleStateAvailable, b.State)
	require.NotNil(t, b.CreationTime)
	require.NotNil(t, b.LastUpdatedTime)
	assert.False(t, b.CreationTime.IsZero())
	assert.Equal(t, "50", aws.ToString(b.UserStorage.Capacity))
	assert.Equal(t, "80", aws.ToString(b.RootStorage.Capacity))
}

func TestAccountLinks_ClientTokenAndLinkedAccountLookup(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, c *wssdk.Client)
		name string
	}{
		{
			name: "same token replays the link",
			run: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				in := &wssdk.CreateAccountLinkInvitationInput{
					TargetAccountId: aws.String("111111111111"), ClientToken: aws.String("tok-1"),
				}
				first, err := c.CreateAccountLinkInvitation(t.Context(), in)
				require.NoError(t, err)
				second, err := c.CreateAccountLinkInvitation(t.Context(), in)
				require.NoError(t, err)
				assert.Equal(t,
					aws.ToString(first.AccountLink.AccountLinkId), aws.ToString(second.AccountLink.AccountLinkId))

				list, err := c.ListAccountLinks(t.Context(), &wssdk.ListAccountLinksInput{})
				require.NoError(t, err)
				assert.Len(t, list.AccountLinks, 1)
			},
		},
		{
			name: "token reuse with other target fails",
			run: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				_, err := c.CreateAccountLinkInvitation(t.Context(), &wssdk.CreateAccountLinkInvitationInput{
					TargetAccountId: aws.String("111111111111"), ClientToken: aws.String("tok-1"),
				})
				require.NoError(t, err)
				_, err = c.CreateAccountLinkInvitation(t.Context(), &wssdk.CreateAccountLinkInvitationInput{
					TargetAccountId: aws.String("222222222222"), ClientToken: aws.String("tok-1"),
				})
				require.ErrorContains(t, err, "InvalidParameterValuesException")
			},
		},
		{
			name: "no token creates distinct links",
			run: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				for range 2 {
					_, err := c.CreateAccountLinkInvitation(t.Context(), &wssdk.CreateAccountLinkInvitationInput{
						TargetAccountId: aws.String("111111111111"),
					})
					require.NoError(t, err)
				}
				list, err := c.ListAccountLinks(t.Context(), &wssdk.ListAccountLinksInput{})
				require.NoError(t, err)
				assert.Len(t, list.AccountLinks, 2)
			},
		},
		{
			name: "get by linked account id",
			run: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				created, err := c.CreateAccountLinkInvitation(t.Context(), &wssdk.CreateAccountLinkInvitationInput{
					TargetAccountId: aws.String("111111111111"),
				})
				require.NoError(t, err)
				got, err := c.GetAccountLink(t.Context(), &wssdk.GetAccountLinkInput{
					LinkedAccountId: aws.String("111111111111"),
				})
				require.NoError(t, err)
				assert.Equal(t,
					aws.ToString(created.AccountLink.AccountLinkId), aws.ToString(got.AccountLink.AccountLinkId))

				_, err = c.GetAccountLink(t.Context(), &wssdk.GetAccountLinkInput{
					LinkedAccountId: aws.String("999999999999"),
				})
				require.ErrorContains(t, err, "ResourceNotFoundException")
			},
		},
		{
			name: "get needs exactly one identifier",
			run: func(t *testing.T, c *wssdk.Client) {
				t.Helper()
				_, err := c.GetAccountLink(t.Context(), &wssdk.GetAccountLinkInput{})
				require.ErrorContains(t, err, "InvalidParameterValuesException")
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t, newTestHandlerAndClient(t))
		})
	}
}

func TestImportCustomWorkspaceImage_TagsApplied(t *testing.T) {
	t.Parallel()

	const infraARN = "arn:aws:imagebuilder:us-east-1:000000000000:infrastructure-configuration/x"

	c := newTestHandlerAndClient(t)
	out, err := c.ImportCustomWorkspaceImage(t.Context(), &wssdk.ImportCustomWorkspaceImageInput{
		ImageName:                      aws.String("img"),
		ImageDescription:               aws.String("d"),
		ComputeType:                    types.ImageComputeTypeBase,
		ImageSource:                    &types.ImageSourceIdentifierMemberEc2ImageId{Value: "ami-1"},
		InfrastructureConfigurationArn: aws.String(infraARN),
		OsVersion:                      types.OSVersionWindows11,
		Platform:                       types.PlatformWindows,
		Protocol:                       types.CustomImageProtocolDcv,
		Tags:                           []types.Tag{{Key: aws.String("env"), Value: aws.String("dev")}},
	})
	require.NoError(t, err)

	got, err := c.DescribeTags(t.Context(), &wssdk.DescribeTagsInput{ResourceId: out.ImageId})
	require.NoError(t, err)
	require.Len(t, got.TagList, 1)
	assert.Equal(t, "env", aws.ToString(got.TagList[0].Key))
}

func TestWorkspacesPool_ApplicationAndTimeoutSettings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		app      *types.ApplicationSettingsRequest
		timeout  *types.TimeoutSettings
		update   *types.TimeoutSettings
		name     string
		wantCode string
		wantIdle int32
		wantMax  int32
	}{
		{
			name: "timeouts merge on update",
			app: &types.ApplicationSettingsRequest{
				Status: types.ApplicationSettingsStatusEnumEnabled, SettingsGroup: aws.String("grp"),
			},
			timeout: &types.TimeoutSettings{
				IdleDisconnectTimeoutInSeconds: aws.Int32(600), MaxUserDurationInSeconds: aws.Int32(3600),
			},
			update:   &types.TimeoutSettings{MaxUserDurationInSeconds: aws.Int32(7200)},
			wantIdle: 600,
			wantMax:  7200,
		},
		{
			name:     "invalid application status",
			app:      &types.ApplicationSettingsRequest{Status: "BOGUS"},
			wantCode: "InvalidParameterValuesException",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newTestHandlerAndClient(t)
			created, err := c.CreateWorkspacesPool(t.Context(), &wssdk.CreateWorkspacesPoolInput{
				PoolName: aws.String("p"), Description: aws.String("d"), BundleId: aws.String("wsb-00000000"),
				DirectoryId: aws.String("d-1"), Capacity: &types.Capacity{DesiredUserSessions: aws.Int32(2)},
				ApplicationSettings: tc.app, TimeoutSettings: tc.timeout,
			})

			if tc.wantCode != "" {
				require.ErrorContains(t, err, tc.wantCode)

				return
			}

			require.NoError(t, err)
			require.NotNil(t, created.WorkspacesPool.ApplicationSettings)
			assert.Equal(t, "grp", aws.ToString(created.WorkspacesPool.ApplicationSettings.SettingsGroup))

			updated, err := c.UpdateWorkspacesPool(t.Context(), &wssdk.UpdateWorkspacesPoolInput{
				PoolId: created.WorkspacesPool.PoolId, TimeoutSettings: tc.update,
			})
			require.NoError(t, err)

			ts := updated.WorkspacesPool.TimeoutSettings
			require.NotNil(t, ts)
			assert.Equal(t, tc.wantIdle, aws.ToInt32(ts.IdleDisconnectTimeoutInSeconds))
			assert.Equal(t, tc.wantMax, aws.ToInt32(ts.MaxUserDurationInSeconds))
		})
	}
}

func TestSnapshotRestore_KeepsDirectoryClientAndImageState(t *testing.T) {
	t.Parallel()

	cases := []struct {
		seed func(t *testing.T, b *workspaces.InMemoryBackend)
		want func(t *testing.T, b *workspaces.InMemoryBackend)
		name string
	}{
		{
			name: "client properties",
			seed: func(t *testing.T, b *workspaces.InMemoryBackend) {
				t.Helper()
				require.NoError(t, b.ModifyClientProperties(
					"d-1", aws.String("policy"), aws.String("ENABLED"), aws.String("DISABLED")))
			},
			want: func(t *testing.T, b *workspaces.InMemoryBackend) {
				t.Helper()
				got, err := b.DescribeClientProperties([]string{"d-1"})
				require.NoError(t, err)
				assert.Equal(t, "DISABLED", got["d-1"].ReconnectEnabled)
				assert.Equal(t, "policy", got["d-1"].ClientExperiencePolicy)
			},
		},
		{
			name: "image permissions",
			seed: func(t *testing.T, b *workspaces.InMemoryBackend) {
				t.Helper()
				id, err := b.ImportWorkspaceImage("ami-1", "img", "d", "BYOL_REGULAR", nil)
				require.NoError(t, err)
				require.NoError(t, b.UpdateWorkspaceImagePermission(id, "111111111111", true))
			},
			want: func(t *testing.T, b *workspaces.InMemoryBackend) {
				t.Helper()
				imgs, _, err := b.DescribeWorkspaceImages(nil, "", 0, "")
				require.NoError(t, err)
				require.Len(t, imgs, 1)
				_, perms, err := b.DescribeWorkspaceImagePermissions(imgs[0].ImageID, "", 0)
				require.NoError(t, err)
				require.Len(t, perms.Data, 1)
				assert.True(t, perms.Data[0].AllowCopyImage)
			},
		},
		{
			name: "directory registration attributes and streaming",
			seed: func(t *testing.T, b *workspaces.InMemoryBackend) {
				t.Helper()
				_, err := b.RegisterWorkspaceDirectoryWithConfig(workspaces.DirectoryRegistration{
					DirectoryID: "d-2", WorkspaceType: "POOLS", Tenancy: "DEDICATED",
				})
				require.NoError(t, err)
				require.NoError(t, b.ModifyStreamingProperties("d-2", workspaces.StreamingProperties{
					StreamingExperiencePreferredProtocol: "UDP",
				}))
			},
			want: func(t *testing.T, b *workspaces.InMemoryBackend) {
				t.Helper()
				dirs, _, err := b.DescribeWorkspaceDirectoriesFiltered(t.Context(), []string{"d-2"}, nil, nil, 0, "")
				require.NoError(t, err)
				require.Len(t, dirs, 1)
				assert.Equal(t, "POOLS", dirs[0].WorkspaceType)
				assert.Equal(t, "DEDICATED", dirs[0].Tenancy)
				require.NotNil(t, dirs[0].StreamingProperties)
				assert.Equal(t, "UDP", dirs[0].StreamingProperties.StreamingExperiencePreferredProtocol)
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
			tc.seed(t, b)

			fresh := workspaces.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(t.Context(), b.Snapshot(t.Context())))
			tc.want(t, fresh)
		})
	}
}
