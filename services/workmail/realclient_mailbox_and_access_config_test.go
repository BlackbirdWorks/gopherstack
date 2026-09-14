package workmail_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	workmailsdk "github.com/aws/aws-sdk-go-v2/service/workmail"
	"github.com/aws/aws-sdk-go-v2/service/workmail/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/workmail"
)

// newWorkMailClient stands up a fresh backend/handler pair plus a real SDK
// client bound to it. Kept separate from newWorkMailSDKClient (which takes
// an already-built handler) so each subtest below can get a fully isolated
// backend in one line.
func newWorkMailClient(t *testing.T) (*workmail.InMemoryBackend, *workmailsdk.Client) {
	t.Helper()

	backend := workmail.NewInMemoryBackend("000000000000", "us-east-1")
	h := workmail.NewHandler(backend)

	return backend, newWorkMailSDKClient(t, h)
}

// TestRealClient_MailboxAndAccessConfig drives every op the census still
// listed as uncovered before this pass (gopherstack-n3zi typed-client
// coverage). Each case builds its own fresh client/backend.
func TestRealClient_MailboxAndAccessConfig(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{
			name: "tags",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				described, err := client.DescribeOrganization(
					t.Context(),
					&workmailsdk.DescribeOrganizationInput{
						OrganizationId: orgID,
					},
				)
				require.NoError(t, err)
				resourceARN := described.ARN
				require.NotNil(t, resourceARN)

				_, err = client.TagResource(t.Context(), &workmailsdk.TagResourceInput{
					ResourceARN: resourceARN,
					Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
				})
				require.NoError(t, err)

				listed, err := client.ListTagsForResource(
					t.Context(),
					&workmailsdk.ListTagsForResourceInput{
						ResourceARN: resourceARN,
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.Tags, 1)
				assert.Equal(t, "env", aws.ToString(listed.Tags[0].Key))
				assert.Equal(t, "prod", aws.ToString(listed.Tags[0].Value))

				_, err = client.UntagResource(t.Context(), &workmailsdk.UntagResourceInput{
					ResourceARN: resourceARN,
					TagKeys:     []string{"env"},
				})
				require.NoError(t, err)

				listed, err = client.ListTagsForResource(t.Context(), &workmailsdk.ListTagsForResourceInput{
					ResourceARN: resourceARN,
				})
				require.NoError(t, err)
				assert.Empty(t, listed.Tags)
			},
		},
		{
			name: "aliases",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				const alias = "alias1@example.com"

				_, err = client.CreateAlias(t.Context(), &workmailsdk.CreateAliasInput{
					OrganizationId: orgID,
					EntityId:       user.UserId,
					Alias:          aws.String(alias),
				})
				require.NoError(t, err)

				listed, err := client.ListAliases(t.Context(), &workmailsdk.ListAliasesInput{
					OrganizationId: orgID,
					EntityId:       user.UserId,
				})
				require.NoError(t, err)
				assert.Equal(t, []string{alias}, listed.Aliases)

				_, err = client.DeleteAlias(t.Context(), &workmailsdk.DeleteAliasInput{
					OrganizationId: orgID,
					EntityId:       user.UserId,
					Alias:          aws.String(alias),
				})
				require.NoError(t, err)

				listed, err = client.ListAliases(t.Context(), &workmailsdk.ListAliasesInput{
					OrganizationId: orgID,
					EntityId:       user.UserId,
				})
				require.NoError(t, err)
				assert.Empty(t, listed.Aliases)
			},
		},
		{
			name: "availability_config",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				const domain = "avail.example.com"

				_, err := client.CreateAvailabilityConfiguration(
					t.Context(),
					&workmailsdk.CreateAvailabilityConfigurationInput{
						OrganizationId: orgID,
						DomainName:     aws.String(domain),
						EwsProvider: &types.EwsAvailabilityProvider{
							EwsEndpoint: aws.String("https://ews.example.com"),
							EwsUsername: aws.String("ewsuser"),
							EwsPassword: aws.String("ewspass"),
						},
					},
				)
				require.NoError(t, err)

				listed, err := client.ListAvailabilityConfigurations(
					t.Context(),
					&workmailsdk.ListAvailabilityConfigurationsInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				require.Len(t, listed.AvailabilityConfigurations, 1)
				assert.Equal(t, domain, aws.ToString(listed.AvailabilityConfigurations[0].DomainName))
				assert.Equal(
					t,
					types.AvailabilityProviderTypeEws,
					listed.AvailabilityConfigurations[0].ProviderType,
				)
				require.NotNil(t, listed.AvailabilityConfigurations[0].EwsProvider)
				assert.Equal(
					t,
					"https://ews.example.com",
					aws.ToString(listed.AvailabilityConfigurations[0].EwsProvider.EwsEndpoint),
				)

				_, err = client.UpdateAvailabilityConfiguration(
					t.Context(),
					&workmailsdk.UpdateAvailabilityConfigurationInput{
						OrganizationId: orgID,
						DomainName:     aws.String(domain),
						LambdaProvider: &types.LambdaAvailabilityProvider{
							LambdaArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:avail"),
						},
					},
				)
				require.NoError(t, err)

				listed, err = client.ListAvailabilityConfigurations(
					t.Context(),
					&workmailsdk.ListAvailabilityConfigurationsInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				require.Len(t, listed.AvailabilityConfigurations, 1)
				assert.Equal(
					t,
					types.AvailabilityProviderTypeLambda,
					listed.AvailabilityConfigurations[0].ProviderType,
				)

				tested, err := client.TestAvailabilityConfiguration(
					t.Context(),
					&workmailsdk.TestAvailabilityConfigurationInput{
						OrganizationId: orgID,
						DomainName:     aws.String(domain),
						LambdaProvider: &types.LambdaAvailabilityProvider{
							LambdaArn: aws.String("arn:aws:lambda:us-east-1:000000000000:function:avail"),
						},
					},
				)
				require.NoError(t, err)
				assert.True(t, tested.TestPassed)

				_, err = client.DeleteAvailabilityConfiguration(
					t.Context(),
					&workmailsdk.DeleteAvailabilityConfigurationInput{
						OrganizationId: orgID,
						DomainName:     aws.String(domain),
					},
				)
				require.NoError(t, err)

				listed, err = client.ListAvailabilityConfigurations(
					t.Context(),
					&workmailsdk.ListAvailabilityConfigurationsInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.AvailabilityConfigurations)
			},
		},
		{
			name: "identity_center_and_provider",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				app, err := client.CreateIdentityCenterApplication(
					t.Context(),
					&workmailsdk.CreateIdentityCenterApplicationInput{
						InstanceArn: aws.String("arn:aws:sso:::instance/ssoins-1234567890abcdef"),
						Name:        aws.String("wm-app"),
					},
				)
				require.NoError(t, err)
				appARN := aws.ToString(app.ApplicationArn)
				require.NotEmpty(t, appARN)

				_, err = client.PutIdentityProviderConfiguration(
					t.Context(),
					&workmailsdk.PutIdentityProviderConfigurationInput{
						OrganizationId:     orgID,
						AuthenticationMode: types.IdentityProviderAuthenticationModeIdentityProviderAndDirectory,
						IdentityCenterConfiguration: &types.IdentityCenterConfiguration{
							ApplicationArn: aws.String(appARN),
							InstanceArn:    aws.String("arn:aws:sso:::instance/ssoins-1234567890abcdef"),
						},
						PersonalAccessTokenConfiguration: &types.PersonalAccessTokenConfiguration{
							Status: types.PersonalAccessTokenConfigurationStatusActive,
						},
					},
				)
				require.NoError(t, err)

				described, err := client.DescribeIdentityProviderConfiguration(
					t.Context(),
					&workmailsdk.DescribeIdentityProviderConfigurationInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					types.IdentityProviderAuthenticationModeIdentityProviderAndDirectory,
					described.AuthenticationMode,
				)
				require.NotNil(t, described.IdentityCenterConfiguration)
				assert.Equal(t, appARN, aws.ToString(described.IdentityCenterConfiguration.ApplicationArn))
				require.NotNil(t, described.PersonalAccessTokenConfiguration)
				assert.Equal(
					t,
					types.PersonalAccessTokenConfigurationStatusActive,
					described.PersonalAccessTokenConfiguration.Status,
				)

				_, err = client.DeleteIdentityProviderConfiguration(
					t.Context(),
					&workmailsdk.DeleteIdentityProviderConfigurationInput{OrganizationId: orgID},
				)
				require.NoError(t, err)

				_, err = client.DeleteIdentityCenterApplication(
					t.Context(),
					&workmailsdk.DeleteIdentityCenterApplicationInput{
						ApplicationArn: aws.String(appARN),
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "impersonation",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				created, err := client.CreateImpersonationRole(
					t.Context(),
					&workmailsdk.CreateImpersonationRoleInput{
						OrganizationId: orgID,
						Name:           aws.String("role1"),
						Type:           types.ImpersonationRoleTypeFullAccess,
						Rules: []types.ImpersonationRule{
							{
								ImpersonationRuleId: aws.String("rule1"),
								Effect:              types.AccessEffectAllow,
								TargetUsers:         []string{"target-user"},
							},
						},
					},
				)
				require.NoError(t, err)
				roleID := aws.ToString(created.ImpersonationRoleId)

				listed, err := client.ListImpersonationRoles(
					t.Context(),
					&workmailsdk.ListImpersonationRolesInput{
						OrganizationId: orgID,
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.Roles, 1)
				assert.Equal(t, "role1", aws.ToString(listed.Roles[0].Name))
				assert.Equal(t, types.ImpersonationRoleTypeFullAccess, listed.Roles[0].Type)

				effect, err := client.GetImpersonationRoleEffect(
					t.Context(),
					&workmailsdk.GetImpersonationRoleEffectInput{
						OrganizationId:      orgID,
						ImpersonationRoleId: aws.String(roleID),
						TargetUser:          aws.String("target-user"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.AccessEffectAllow, effect.Effect)
				require.Len(t, effect.MatchedRules, 1)
				assert.Equal(t, "rule1", aws.ToString(effect.MatchedRules[0].ImpersonationRuleId))

				_, err = client.UpdateImpersonationRole(
					t.Context(),
					&workmailsdk.UpdateImpersonationRoleInput{
						OrganizationId:      orgID,
						ImpersonationRoleId: aws.String(roleID),
						Name:                aws.String("role1-renamed"),
						Type:                types.ImpersonationRoleTypeReadOnly,
						Rules: []types.ImpersonationRule{
							{
								ImpersonationRuleId: aws.String("rule1"),
								Effect:              types.AccessEffectDeny,
								TargetUsers:         []string{"target-user"},
							},
						},
					},
				)
				require.NoError(t, err)

				listed, err = client.ListImpersonationRoles(
					t.Context(),
					&workmailsdk.ListImpersonationRolesInput{
						OrganizationId: orgID,
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.Roles, 1)
				assert.Equal(t, "role1-renamed", aws.ToString(listed.Roles[0].Name))
				assert.Equal(t, types.ImpersonationRoleTypeReadOnly, listed.Roles[0].Type)

				assumed, err := client.AssumeImpersonationRole(
					t.Context(),
					&workmailsdk.AssumeImpersonationRoleInput{
						OrganizationId:      orgID,
						ImpersonationRoleId: aws.String(roleID),
					},
				)
				require.NoError(t, err)
				assert.NotEmpty(t, aws.ToString(assumed.Token))
				assert.Positive(t, aws.ToInt64(assumed.ExpiresIn))

				_, err = client.DeleteImpersonationRole(
					t.Context(),
					&workmailsdk.DeleteImpersonationRoleInput{
						OrganizationId:      orgID,
						ImpersonationRoleId: aws.String(roleID),
					},
				)
				require.NoError(t, err)

				listed, err = client.ListImpersonationRoles(
					t.Context(),
					&workmailsdk.ListImpersonationRolesInput{
						OrganizationId: orgID,
					},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.Roles)
			},
		},
		{
			name: "mobile_device_access_rules",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				created, err := client.CreateMobileDeviceAccessRule(
					t.Context(),
					&workmailsdk.CreateMobileDeviceAccessRuleInput{
						OrganizationId: orgID,
						Name:           aws.String("rule1"),
						Effect:         types.MobileDeviceAccessRuleEffectAllow,
						DeviceTypes:    []string{"iOS"},
					},
				)
				require.NoError(t, err)
				ruleID := aws.ToString(created.MobileDeviceAccessRuleId)

				listed, err := client.ListMobileDeviceAccessRules(
					t.Context(),
					&workmailsdk.ListMobileDeviceAccessRulesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				require.Len(t, listed.Rules, 1)
				assert.Equal(t, "rule1", aws.ToString(listed.Rules[0].Name))
				assert.Equal(t, types.MobileDeviceAccessRuleEffectAllow, listed.Rules[0].Effect)

				effect, err := client.GetMobileDeviceAccessEffect(
					t.Context(),
					&workmailsdk.GetMobileDeviceAccessEffectInput{
						OrganizationId: orgID,
						DeviceType:     aws.String("iOS"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.MobileDeviceAccessRuleEffectAllow, effect.Effect)

				_, err = client.UpdateMobileDeviceAccessRule(
					t.Context(),
					&workmailsdk.UpdateMobileDeviceAccessRuleInput{
						OrganizationId:           orgID,
						MobileDeviceAccessRuleId: aws.String(ruleID),
						Name:                     aws.String("rule1-renamed"),
						Effect:                   types.MobileDeviceAccessRuleEffectDeny,
						DeviceTypes:              []string{"iOS"},
					},
				)
				require.NoError(t, err)

				listed, err = client.ListMobileDeviceAccessRules(
					t.Context(),
					&workmailsdk.ListMobileDeviceAccessRulesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				require.Len(t, listed.Rules, 1)
				assert.Equal(t, "rule1-renamed", aws.ToString(listed.Rules[0].Name))
				assert.Equal(t, types.MobileDeviceAccessRuleEffectDeny, listed.Rules[0].Effect)

				_, err = client.DeleteMobileDeviceAccessRule(
					t.Context(),
					&workmailsdk.DeleteMobileDeviceAccessRuleInput{
						OrganizationId:           orgID,
						MobileDeviceAccessRuleId: aws.String(ruleID),
					},
				)
				require.NoError(t, err)

				listed, err = client.ListMobileDeviceAccessRules(
					t.Context(),
					&workmailsdk.ListMobileDeviceAccessRulesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.Rules)
			},
		},
		{
			name: "mobile_device_access_overrides",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				const deviceID = "device-1"

				_, err = client.PutMobileDeviceAccessOverride(
					t.Context(),
					&workmailsdk.PutMobileDeviceAccessOverrideInput{
						OrganizationId: orgID,
						UserId:         user.UserId,
						DeviceId:       aws.String(deviceID),
						Effect:         types.MobileDeviceAccessRuleEffectDeny,
						Description:    aws.String("blocked device"),
					},
				)
				require.NoError(t, err)

				got, err := client.GetMobileDeviceAccessOverride(
					t.Context(),
					&workmailsdk.GetMobileDeviceAccessOverrideInput{
						OrganizationId: orgID,
						UserId:         user.UserId,
						DeviceId:       aws.String(deviceID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.MobileDeviceAccessRuleEffectDeny, got.Effect)
				assert.Equal(t, "blocked device", aws.ToString(got.Description))

				listed, err := client.ListMobileDeviceAccessOverrides(
					t.Context(),
					&workmailsdk.ListMobileDeviceAccessOverridesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				require.Len(t, listed.Overrides, 1)
				assert.Equal(t, deviceID, aws.ToString(listed.Overrides[0].DeviceId))

				_, err = client.DeleteMobileDeviceAccessOverride(
					t.Context(),
					&workmailsdk.DeleteMobileDeviceAccessOverrideInput{
						OrganizationId: orgID,
						UserId:         user.UserId,
						DeviceId:       aws.String(deviceID),
					},
				)
				require.NoError(t, err)

				listed, err = client.ListMobileDeviceAccessOverrides(
					t.Context(),
					&workmailsdk.ListMobileDeviceAccessOverridesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.Overrides)
			},
		},
		{
			name: "personal_access_tokens",
			run: func(t *testing.T) {
				t.Helper()

				backend, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				// The real WorkMail API has no CreatePersonalAccessToken op -- tokens
				// are minted by the mailbox client itself, not this control-plane API
				// (see wire_enableddate_test.go's newWorkMailSDKClient doc and
				// personal_access_tokens.go's own CreatePersonalAccessToken comment).
				// Seed one via the backend directly, the same pattern this package's
				// existing tests already use.
				tok, err := backend.CreatePersonalAccessToken(
					aws.ToString(orgID), aws.ToString(user.UserId), "my-token", []string{"read"},
				)
				require.NoError(t, err)
				require.NotNil(t, tok)

				listed, err := client.ListPersonalAccessTokens(
					t.Context(),
					&workmailsdk.ListPersonalAccessTokensInput{
						OrganizationId: orgID,
						UserId:         user.UserId,
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.PersonalAccessTokenSummaries, 1)
				assert.Equal(t, "my-token", aws.ToString(listed.PersonalAccessTokenSummaries[0].Name))

				_, err = client.DeletePersonalAccessToken(
					t.Context(),
					&workmailsdk.DeletePersonalAccessTokenInput{
						OrganizationId:        orgID,
						PersonalAccessTokenId: listed.PersonalAccessTokenSummaries[0].PersonalAccessTokenId,
					},
				)
				require.NoError(t, err)

				listed, err = client.ListPersonalAccessTokens(
					t.Context(),
					&workmailsdk.ListPersonalAccessTokensInput{
						OrganizationId: orgID,
						UserId:         user.UserId,
					},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.PersonalAccessTokenSummaries)
			},
		},
		{
			name: "delegates",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				resource, err := client.CreateResource(t.Context(), &workmailsdk.CreateResourceInput{
					OrganizationId: orgID,
					Name:           aws.String("room1"),
					Type:           types.ResourceTypeRoom,
				})
				require.NoError(t, err)

				_, err = client.AssociateDelegateToResource(
					t.Context(),
					&workmailsdk.AssociateDelegateToResourceInput{
						OrganizationId: orgID,
						ResourceId:     resource.ResourceId,
						EntityId:       user.UserId,
					},
				)
				require.NoError(t, err)

				listed, err := client.ListResourceDelegates(
					t.Context(),
					&workmailsdk.ListResourceDelegatesInput{
						OrganizationId: orgID,
						ResourceId:     resource.ResourceId,
					},
				)
				require.NoError(t, err)
				require.Len(t, listed.Delegates, 1)
				assert.Equal(t, aws.ToString(user.UserId), aws.ToString(listed.Delegates[0].Id))

				_, err = client.DisassociateDelegateFromResource(
					t.Context(),
					&workmailsdk.DisassociateDelegateFromResourceInput{
						OrganizationId: orgID,
						ResourceId:     resource.ResourceId,
						EntityId:       user.UserId,
					},
				)
				require.NoError(t, err)

				listed, err = client.ListResourceDelegates(
					t.Context(),
					&workmailsdk.ListResourceDelegatesInput{
						OrganizationId: orgID,
						ResourceId:     resource.ResourceId,
					},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.Delegates)
			},
		},
		{
			name: "dmarc",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				_, err := client.PutInboundDmarcSettings(
					t.Context(),
					&workmailsdk.PutInboundDmarcSettingsInput{
						OrganizationId: orgID,
						Enforced:       aws.Bool(true),
					},
				)
				require.NoError(t, err)

				described, err := client.DescribeInboundDmarcSettings(
					t.Context(),
					&workmailsdk.DescribeInboundDmarcSettingsInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.True(t, described.Enforced)
			},
		},
		{
			name: "email_monitoring",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				_, err := client.PutEmailMonitoringConfiguration(
					t.Context(),
					&workmailsdk.PutEmailMonitoringConfigurationInput{
						OrganizationId: orgID,
						LogGroupArn: aws.String(
							"arn:aws:logs:us-east-1:000000000000:log-group:/wm/monitor",
						),
						RoleArn: aws.String("arn:aws:iam::000000000000:role/wm-monitor"),
					},
				)
				require.NoError(t, err)

				described, err := client.DescribeEmailMonitoringConfiguration(
					t.Context(),
					&workmailsdk.DescribeEmailMonitoringConfigurationInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Equal(
					t,
					"arn:aws:logs:us-east-1:000000000000:log-group:/wm/monitor",
					aws.ToString(described.LogGroupArn),
				)
				assert.Equal(
					t,
					"arn:aws:iam::000000000000:role/wm-monitor",
					aws.ToString(described.RoleArn),
				)

				_, err = client.DeleteEmailMonitoringConfiguration(
					t.Context(),
					&workmailsdk.DeleteEmailMonitoringConfigurationInput{OrganizationId: orgID},
				)
				require.NoError(t, err)

				described, err = client.DescribeEmailMonitoringConfiguration(
					t.Context(),
					&workmailsdk.DescribeEmailMonitoringConfigurationInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Empty(t, aws.ToString(described.LogGroupArn))
			},
		},
		{
			name: "retention_policy",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				_, err := client.PutRetentionPolicy(t.Context(), &workmailsdk.PutRetentionPolicyInput{
					OrganizationId: orgID,
					Name:           aws.String("default-policy"),
					FolderConfigurations: []types.FolderConfiguration{
						{
							Name:   types.FolderNameInbox,
							Action: types.RetentionActionDelete,
							Period: aws.Int32(30),
						},
					},
				})
				require.NoError(t, err)

				got, err := client.GetDefaultRetentionPolicy(
					t.Context(),
					&workmailsdk.GetDefaultRetentionPolicyInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Equal(t, "default-policy", aws.ToString(got.Name))
				require.Len(t, got.FolderConfigurations, 1)
				assert.Equal(t, types.FolderNameInbox, got.FolderConfigurations[0].Name)
				assert.Equal(t, types.RetentionActionDelete, got.FolderConfigurations[0].Action)
				assert.Equal(t, int32(30), aws.ToInt32(got.FolderConfigurations[0].Period))

				_, err = client.DeleteRetentionPolicy(t.Context(), &workmailsdk.DeleteRetentionPolicyInput{
					OrganizationId: orgID,
					Id:             got.Id,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "access_control",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				_, err := client.PutAccessControlRule(t.Context(), &workmailsdk.PutAccessControlRuleInput{
					OrganizationId: orgID,
					Name:           aws.String("rule1"),
					Effect:         types.AccessControlRuleEffectDeny,
					Description:    aws.String("block bad ip"),
					IpRanges:       []string{"10.0.0.0/8"},
				})
				require.NoError(t, err)

				listed, err := client.ListAccessControlRules(
					t.Context(),
					&workmailsdk.ListAccessControlRulesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				require.Len(t, listed.Rules, 1)
				assert.Equal(t, "rule1", aws.ToString(listed.Rules[0].Name))
				assert.Equal(t, []string{"10.0.0.0/8"}, listed.Rules[0].IpRanges)

				effect, err := client.GetAccessControlEffect(
					t.Context(),
					&workmailsdk.GetAccessControlEffectInput{
						OrganizationId: orgID,
						IpAddress:      aws.String("10.1.2.3"),
						Action:         aws.String("SEND_EMAIL"),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.AccessControlRuleEffectDeny, effect.Effect)
				assert.Contains(t, effect.MatchedRules, "rule1")

				_, err = client.DeleteAccessControlRule(
					t.Context(),
					&workmailsdk.DeleteAccessControlRuleInput{
						OrganizationId: orgID,
						Name:           aws.String("rule1"),
					},
				)
				require.NoError(t, err)

				listed, err = client.ListAccessControlRules(
					t.Context(),
					&workmailsdk.ListAccessControlRulesInput{OrganizationId: orgID},
				)
				require.NoError(t, err)
				assert.Empty(t, listed.Rules)
			},
		},
		{
			name: "mail_domain_default",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				const domainA = "domain-a.example.com"

				const domainB = "domain-b.example.com"

				_, err := client.RegisterMailDomain(t.Context(), &workmailsdk.RegisterMailDomainInput{
					OrganizationId: orgID,
					DomainName:     aws.String(domainA),
				})
				require.NoError(t, err)

				_, err = client.RegisterMailDomain(t.Context(), &workmailsdk.RegisterMailDomainInput{
					OrganizationId: orgID,
					DomainName:     aws.String(domainB),
				})
				require.NoError(t, err)

				_, err = client.UpdateDefaultMailDomain(
					t.Context(),
					&workmailsdk.UpdateDefaultMailDomainInput{
						OrganizationId: orgID,
						DomainName:     aws.String(domainB),
					},
				)
				require.NoError(t, err)

				described, err := client.DescribeOrganization(
					t.Context(),
					&workmailsdk.DescribeOrganizationInput{
						OrganizationId: orgID,
					},
				)
				require.NoError(t, err)
				assert.Equal(t, domainB, aws.ToString(described.DefaultMailDomain))
			},
		},
		{
			name: "mailbox_extras",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				details, err := client.GetMailboxDetails(t.Context(), &workmailsdk.GetMailboxDetailsInput{
					OrganizationId: orgID,
					UserId:         user.UserId,
				})
				require.NoError(t, err)
				assert.Positive(t, aws.ToInt32(details.MailboxQuota))

				_, err = client.UpdateMailboxQuota(t.Context(), &workmailsdk.UpdateMailboxQuotaInput{
					OrganizationId: orgID,
					UserId:         user.UserId,
					MailboxQuota:   aws.Int32(2048),
				})
				require.NoError(t, err)

				details, err = client.GetMailboxDetails(t.Context(), &workmailsdk.GetMailboxDetailsInput{
					OrganizationId: orgID,
					UserId:         user.UserId,
				})
				require.NoError(t, err)
				assert.Equal(t, int32(2048), aws.ToInt32(details.MailboxQuota))

				started, err := client.StartMailboxExportJob(
					t.Context(),
					&workmailsdk.StartMailboxExportJobInput{
						OrganizationId: orgID,
						EntityId:       user.UserId,
						RoleArn:        aws.String("arn:aws:iam::000000000000:role/wm-export"),
						KmsKeyArn:      aws.String("arn:aws:kms:us-east-1:000000000000:key/abc"),
						S3BucketName:   aws.String("wm-export-bucket"),
						S3Prefix:       aws.String("exports/"),
						ClientToken:    aws.String(uuid.NewString()),
					},
				)
				require.NoError(t, err)
				jobID := aws.ToString(started.JobId)
				require.NotEmpty(t, jobID)

				described, err := client.DescribeMailboxExportJob(
					t.Context(),
					&workmailsdk.DescribeMailboxExportJobInput{
						OrganizationId: orgID,
						JobId:          aws.String(jobID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.MailboxExportJobStateRunning, described.State)

				_, err = client.CancelMailboxExportJob(
					t.Context(),
					&workmailsdk.CancelMailboxExportJobInput{
						OrganizationId: orgID,
						JobId:          aws.String(jobID),
						ClientToken:    aws.String(uuid.NewString()),
					},
				)
				require.NoError(t, err)

				described, err = client.DescribeMailboxExportJob(
					t.Context(),
					&workmailsdk.DescribeMailboxExportJobInput{
						OrganizationId: orgID,
						JobId:          aws.String(jobID),
					},
				)
				require.NoError(t, err)
				assert.Equal(t, types.MailboxExportJobStateCancelled, described.State)
			},
		},
		{
			name: "entities_groups",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				described, err := client.DescribeEntity(t.Context(), &workmailsdk.DescribeEntityInput{
					OrganizationId: orgID,
					Email:          user.UserId,
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(user.UserId), aws.ToString(described.EntityId))
				assert.Equal(t, types.EntityTypeUser, described.Type)

				groupName := "group-" + uuid.NewString()[:8]
				group, err := client.CreateGroup(t.Context(), &workmailsdk.CreateGroupInput{
					OrganizationId: orgID,
					Name:           aws.String(groupName),
				})
				require.NoError(t, err)

				_, err = client.AssociateMemberToGroup(
					t.Context(),
					&workmailsdk.AssociateMemberToGroupInput{
						OrganizationId: orgID,
						GroupId:        group.GroupId,
						MemberId:       user.UserId,
					},
				)
				require.NoError(t, err)

				listedGroups, err := client.ListGroupsForEntity(
					t.Context(),
					&workmailsdk.ListGroupsForEntityInput{
						OrganizationId: orgID,
						EntityId:       user.UserId,
					},
				)
				require.NoError(t, err)
				require.Len(t, listedGroups.Groups, 1)
				assert.Equal(t, aws.ToString(group.GroupId), aws.ToString(listedGroups.Groups[0].GroupId))

				_, err = client.UpdateGroup(t.Context(), &workmailsdk.UpdateGroupInput{
					OrganizationId:              orgID,
					GroupId:                     group.GroupId,
					HiddenFromGlobalAddressList: aws.Bool(true),
				})
				require.NoError(t, err)

				describedGroup, err := client.DescribeGroup(t.Context(), &workmailsdk.DescribeGroupInput{
					OrganizationId: orgID,
					GroupId:        group.GroupId,
				})
				require.NoError(t, err)
				assert.True(t, describedGroup.HiddenFromGlobalAddressList)
			},
		},
		{
			name: "users_misc",
			run: func(t *testing.T) {
				t.Helper()

				_, client := newWorkMailClient(t)
				orgID := newWorkMailOrg(t, client)

				userName := "user-" + uuid.NewString()[:8]
				user, err := client.CreateUser(t.Context(), &workmailsdk.CreateUserInput{
					OrganizationId: orgID,
					Name:           aws.String(userName),
					DisplayName:    aws.String(userName),
				})
				require.NoError(t, err)

				_, err = client.ResetPassword(t.Context(), &workmailsdk.ResetPasswordInput{
					OrganizationId: orgID,
					UserId:         user.UserId,
					Password:       aws.String("N3wP@ssword!"),
				})
				require.NoError(t, err)

				const email = "primary@example.com"

				_, err = client.UpdatePrimaryEmailAddress(
					t.Context(),
					&workmailsdk.UpdatePrimaryEmailAddressInput{
						OrganizationId: orgID,
						EntityId:       user.UserId,
						Email:          aws.String(email),
					},
				)
				require.NoError(t, err)

				described, err := client.DescribeUser(t.Context(), &workmailsdk.DescribeUserInput{
					OrganizationId: orgID,
					UserId:         user.UserId,
				})
				require.NoError(t, err)
				assert.Equal(t, email, aws.ToString(described.Email))
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
