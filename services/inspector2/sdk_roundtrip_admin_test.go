package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestRealClient_Admin drives account enablement, delegated admin, member,
// usage/permission, EC2 deep inspection, and encryption key ops through a
// real aws-sdk-go-v2 inspector2 client.
func TestRealClient_Admin(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *inspector2.InMemoryBackend, client *inspector2sdk.Client)
		name string
	}{
		{
			name: "enablement_and_delegated_admin",
			run: func(t *testing.T, _ *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				enableOut, err := client.Enable(ctx, &inspector2sdk.EnableInput{
					ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
				})
				require.NoError(t, err)
				require.Len(t, enableOut.Accounts, 1)
				assert.Equal(t, types.StatusEnabled, enableOut.Accounts[0].ResourceStatus.Ec2)

				batchOut, err := client.BatchGetAccountStatus(ctx, &inspector2sdk.BatchGetAccountStatusInput{})
				require.NoError(t, err)
				require.Len(t, batchOut.Accounts, 1)
				assert.Equal(t, types.StatusEnabled, batchOut.Accounts[0].ResourceState.Ec2.Status)

				_, err = client.UpdateConfiguration(ctx, &inspector2sdk.UpdateConfigurationInput{
					Ec2Configuration: &types.Ec2Configuration{ScanMode: types.Ec2ScanModeEc2Hybrid},
				})
				require.NoError(t, err)

				_, err = client.EnableDelegatedAdminAccount(ctx, &inspector2sdk.EnableDelegatedAdminAccountInput{
					DelegatedAdminAccountId: aws.String("222222222222"),
				})
				require.NoError(t, err)

				getAdmin, err := client.GetDelegatedAdminAccount(ctx, &inspector2sdk.GetDelegatedAdminAccountInput{})
				require.NoError(t, err)
				require.NotNil(t, getAdmin.DelegatedAdmin)
				assert.Equal(t, "222222222222", aws.ToString(getAdmin.DelegatedAdmin.AccountId))
				assert.Equal(t, types.RelationshipStatusEnabled, getAdmin.DelegatedAdmin.RelationshipStatus,
					"real GetDelegatedAdminAccountOutput.DelegatedAdmin.RelationshipStatus must decode -- "+
						"previously wire-keyed \"status\" (the sibling ListDelegatedAdminAccounts shape), always nil")

				_, err = client.DisableDelegatedAdminAccount(ctx, &inspector2sdk.DisableDelegatedAdminAccountInput{
					DelegatedAdminAccountId: aws.String("222222222222"),
				})
				require.NoError(t, err)

				_, err = client.GetDelegatedAdminAccount(ctx, &inspector2sdk.GetDelegatedAdminAccountInput{})
				require.Error(t, err)

				_, err = client.Disable(ctx, &inspector2sdk.DisableInput{
					ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
				})
				require.NoError(t, err)
			},
		},
		{
			name: "member_management",
			run: func(t *testing.T, _ *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.AssociateMember(ctx, &inspector2sdk.AssociateMemberInput{
					AccountId: aws.String("333333333333"),
				})
				require.NoError(t, err)

				getMember, err := client.GetMember(ctx, &inspector2sdk.GetMemberInput{
					AccountId: aws.String("333333333333"),
				})
				require.NoError(t, err)
				require.NotNil(t, getMember.Member)
				assert.Equal(t, "333333333333", aws.ToString(getMember.Member.AccountId))
				assert.Equal(t, types.RelationshipStatusEnabled, getMember.Member.RelationshipStatus)

				listMembers, err := client.ListMembers(ctx, &inspector2sdk.ListMembersInput{})
				require.NoError(t, err)
				require.Len(t, listMembers.Members, 1)

				_, err = client.DisassociateMember(ctx, &inspector2sdk.DisassociateMemberInput{
					AccountId: aws.String("333333333333"),
				})
				require.NoError(t, err)

				_, err = client.GetMember(ctx, &inspector2sdk.GetMemberInput{
					AccountId: aws.String("333333333333"),
				})
				assert.Error(t, err)
			},
		},
		{
			name: "usage_and_permissions",
			run: func(t *testing.T, backend *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := client.Enable(ctx, &inspector2sdk.EnableInput{
					ResourceTypes: []types.ResourceScanType{types.ResourceScanTypeEc2},
				})
				require.NoError(t, err)

				_, err = backend.SeedCoverage(inspector2.CoverageEntry{
					ResourceID:   "i-usage-slice12",
					ResourceType: "AWS_EC2_INSTANCE",
					ScanType:     "EC2",
				})
				require.NoError(t, err)

				usageOut, err := client.ListUsageTotals(ctx, &inspector2sdk.ListUsageTotalsInput{})
				require.NoError(t, err)
				require.Len(t, usageOut.Totals, 1)
				assert.Equal(t, rtTestAccountID, aws.ToString(usageOut.Totals[0].AccountId))
				require.NotEmpty(t, usageOut.Totals[0].Usage)

				permsOut, err := client.ListAccountPermissions(ctx, &inspector2sdk.ListAccountPermissionsInput{})
				require.NoError(t, err)
				assert.NotNil(t, permsOut.Permissions)

				delegatedOut, err := client.ListDelegatedAdminAccounts(
					ctx, &inspector2sdk.ListDelegatedAdminAccountsInput{},
				)
				require.NoError(t, err)
				assert.NotNil(t, delegatedOut.DelegatedAdminAccounts)
			},
		},
		{
			name: "ec2_deep_inspection_configuration",
			run: func(t *testing.T, _ *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				getOut, err := client.GetEc2DeepInspectionConfiguration(
					ctx, &inspector2sdk.GetEc2DeepInspectionConfigurationInput{},
				)
				require.NoError(t, err)
				assert.Equal(t, types.Ec2DeepInspectionStatusDeactivated, getOut.Status)

				updOut, err := client.UpdateEc2DeepInspectionConfiguration(
					ctx, &inspector2sdk.UpdateEc2DeepInspectionConfigurationInput{
						PackagePaths: []string{"/opt/custom/paths"},
					},
				)
				require.NoError(t, err)
				assert.Contains(t, updOut.PackagePaths, "/opt/custom/paths")

				_, err = client.UpdateOrgEc2DeepInspectionConfiguration(
					ctx, &inspector2sdk.UpdateOrgEc2DeepInspectionConfigurationInput{
						OrgPackagePaths: []string{"/opt/org/paths"},
					},
				)
				require.NoError(t, err)
			},
		},
		{
			name: "encryption_key_lifecycle",
			run: func(t *testing.T, _ *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				getOut, err := client.GetEncryptionKey(ctx, &inspector2sdk.GetEncryptionKeyInput{
					ResourceType: types.ResourceTypeAwsEcrContainerImage,
					ScanType:     types.ScanTypeNetwork,
				})
				require.NoError(t, err)
				assert.Equal(t, "AWS_OWNED_KEY", aws.ToString(getOut.KmsKeyId))

				_, err = client.UpdateEncryptionKey(ctx, &inspector2sdk.UpdateEncryptionKeyInput{
					KmsKeyId:     aws.String("arn:aws:kms:us-east-1:123456789012:key/my-key"),
					ResourceType: types.ResourceTypeAwsEcrContainerImage,
					ScanType:     types.ScanTypeNetwork,
				})
				require.NoError(t, err)

				getOut2, err := client.GetEncryptionKey(ctx, &inspector2sdk.GetEncryptionKeyInput{
					ResourceType: types.ResourceTypeAwsEcrContainerImage,
					ScanType:     types.ScanTypeNetwork,
				})
				require.NoError(t, err)
				assert.Equal(t, "arn:aws:kms:us-east-1:123456789012:key/my-key", aws.ToString(getOut2.KmsKeyId))

				_, err = client.ResetEncryptionKey(ctx, &inspector2sdk.ResetEncryptionKeyInput{
					ResourceType: types.ResourceTypeAwsEcrContainerImage,
					ScanType:     types.ScanTypeNetwork,
				})
				require.NoError(t, err)

				getOut3, err := client.GetEncryptionKey(ctx, &inspector2sdk.GetEncryptionKeyInput{
					ResourceType: types.ResourceTypeAwsEcrContainerImage,
					ScanType:     types.ScanTypeNetwork,
				})
				require.NoError(t, err)
				assert.Equal(t, "AWS_OWNED_KEY", aws.ToString(getOut3.KmsKeyId))
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend, client := newRealClient(t)
			tc.run(t, backend, client)
		})
	}
}
