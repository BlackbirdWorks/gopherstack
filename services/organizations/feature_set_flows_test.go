package organizations_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	organizationssdk "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationstypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/organizations"
)

func newSDKOrg(
	t *testing.T,
	featureSet organizationstypes.OrganizationFeatureSet,
) (*organizationssdk.Client, string, string) {
	t.Helper()

	backend := organizations.NewInMemoryBackend("000000000000", tagsRTRegion)
	client := newTestOrganizationsClient(t, organizations.NewHandler(backend))

	org, err := client.CreateOrganization(
		t.Context(),
		&organizationssdk.CreateOrganizationInput{FeatureSet: featureSet},
	)
	require.NoError(t, err)

	roots, err := client.ListRoots(t.Context(), &organizationssdk.ListRootsInput{})
	require.NoError(t, err)

	return client, aws.ToString(roots.Roots[0].Id), aws.ToString(org.Organization.MasterAccountId)
}

func TestDelegatedAdminRejections(t *testing.T) {
	t.Parallel()

	const principal = "config.amazonaws.com"

	tests := []struct {
		setup      func(t *testing.T, c *organizationssdk.Client, mgmt string) string
		name       string
		wantReason organizationstypes.ConstraintViolationExceptionReason
	}{
		{
			name: "management_account",
			setup: func(_ *testing.T, _ *organizationssdk.Client, mgmt string) string {
				return mgmt
			},
			wantReason: organizationstypes.ConstraintViolationExceptionReasonCannotRegisterMasterAsDelegatedAdministrator,
		},
		{
			name: "closed_account",
			setup: func(t *testing.T, c *organizationssdk.Client, _ string) string {
				t.Helper()

				created, err := c.CreateAccount(t.Context(), &organizationssdk.CreateAccountInput{
					AccountName: aws.String("member"), Email: aws.String("member@example.com"),
				})
				require.NoError(t, err)

				id := aws.ToString(created.CreateAccountStatus.AccountId)
				_, err = c.CloseAccount(t.Context(), &organizationssdk.CloseAccountInput{AccountId: aws.String(id)})
				require.NoError(t, err)

				return id
			},
			wantReason: organizationstypes.
				ConstraintViolationExceptionReasonCannotRegisterSuspendedAccountAsDelegatedAdministrator,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, _, mgmt := newSDKOrg(t, organizationstypes.OrganizationFeatureSetAll)
			_, err := client.EnableAWSServiceAccess(t.Context(), &organizationssdk.EnableAWSServiceAccessInput{
				ServicePrincipal: aws.String(principal),
			})
			require.NoError(t, err)

			id := tt.setup(t, client, mgmt)

			_, err = client.RegisterDelegatedAdministrator(
				t.Context(),
				&organizationssdk.RegisterDelegatedAdministratorInput{
					AccountId: aws.String(id), ServicePrincipal: aws.String(principal),
				},
			)
			require.Error(t, err)

			var cve *organizationstypes.ConstraintViolationException
			require.ErrorAs(t, err, &cve)
			assert.Equal(t, tt.wantReason, cve.Reason)
		})
	}
}

func TestPolicyTypeNeedsAllFeatures(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		featureSet  organizationstypes.OrganizationFeatureSet
		policyType  organizationstypes.PolicyType
		acceptFirst bool
		wantErr     bool
	}{
		{
			name:       "billing_only_rejects_scp",
			featureSet: organizationstypes.OrganizationFeatureSetConsolidatedBilling,
			policyType: organizationstypes.PolicyTypeServiceControlPolicy,
			wantErr:    true,
		},
		{
			name:       "billing_only_rejects_tag_policy",
			featureSet: organizationstypes.OrganizationFeatureSetConsolidatedBilling,
			policyType: organizationstypes.PolicyTypeTagPolicy,
			wantErr:    true,
		},
		{
			name:        "accepted_upgrade_allows_scp",
			featureSet:  organizationstypes.OrganizationFeatureSetConsolidatedBilling,
			policyType:  organizationstypes.PolicyTypeServiceControlPolicy,
			acceptFirst: true,
		},
		{
			name:       "all_features_allows_tag_policy",
			featureSet: organizationstypes.OrganizationFeatureSetAll,
			policyType: organizationstypes.PolicyTypeTagPolicy,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client, rootID, _ := newSDKOrg(t, tt.featureSet)

			if tt.acceptFirst {
				hs, err := client.EnableAllFeatures(t.Context(), &organizationssdk.EnableAllFeaturesInput{})
				require.NoError(t, err)

				_, err = client.AcceptHandshake(
					t.Context(),
					&organizationssdk.AcceptHandshakeInput{HandshakeId: hs.Handshake.Id},
				)
				require.NoError(t, err)

				desc, err := client.DescribeOrganization(t.Context(), &organizationssdk.DescribeOrganizationInput{})
				require.NoError(t, err)
				assert.Equal(t, organizationstypes.OrganizationFeatureSetAll, desc.Organization.FeatureSet)
			}

			_, err := client.EnablePolicyType(t.Context(), &organizationssdk.EnablePolicyTypeInput{
				RootId: aws.String(rootID), PolicyType: tt.policyType,
			})

			if tt.wantErr {
				var nae *organizationstypes.PolicyTypeNotAvailableForOrganizationException
				require.ErrorAs(t, err, &nae)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestResourcePolicyID_MatchesSDKPattern(t *testing.T) {
	t.Parallel()

	client, _, _ := newSDKOrg(t, organizationstypes.OrganizationFeatureSetAll)

	put, err := client.PutResourcePolicy(t.Context(), &organizationssdk.PutResourcePolicyInput{
		Content: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
	})
	require.NoError(t, err)

	sum := put.ResourcePolicy.ResourcePolicySummary
	assert.Regexp(t, `^rp-[0-9a-zA-Z_]{4,128}$`, aws.ToString(sum.Id))
	assert.Contains(t, aws.ToString(sum.Arn), "/"+aws.ToString(sum.Id))

	got, err := client.DescribeResourcePolicy(t.Context(), &organizationssdk.DescribeResourcePolicyInput{})
	require.NoError(t, err)
	assert.Equal(t, aws.ToString(sum.Id), aws.ToString(got.ResourcePolicy.ResourcePolicySummary.Id))
}
