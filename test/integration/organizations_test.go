//go:build integration
// +build integration

package integration_test

import (
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	organizationsSDK "github.com/aws/aws-sdk-go-v2/service/organizations"
	organizationsSDKtypes "github.com/aws/aws-sdk-go-v2/service/organizations/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createOrganizationsClient returns an Organizations client pointed at the shared test container.
func createOrganizationsClient(t *testing.T) *organizationsSDK.Client {
	t.Helper()

	cfg, err := config.LoadDefaultConfig(
		t.Context(),
		config.WithRegion("us-east-1"),
		config.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider("test", "test", ""),
		),
	)
	require.NoError(t, err, "unable to load SDK config")

	return organizationsSDK.NewFromConfig(cfg, func(o *organizationsSDK.Options) {
		o.BaseEndpoint = aws.String(endpoint)
	})
}

// ensureOrg creates an org if none exists; returns without error if one already exists.
func ensureOrg(t *testing.T, client *organizationsSDK.Client) {
	t.Helper()

	ctx := t.Context()

	_, err := client.CreateOrganization(ctx, &organizationsSDK.CreateOrganizationInput{
		FeatureSet: organizationsSDKtypes.OrganizationFeatureSetAll,
	})
	if err == nil {
		return
	}

	var already *organizationsSDKtypes.AlreadyInOrganizationException
	if !errors.As(err, &already) {
		require.NoError(t, err, "CreateOrganization should succeed or be AlreadyInOrganizationException")
	}
}

// TestIntegration_Organizations_OrgLifecycle tests org creation, description, and deletion.
func TestIntegration_Organizations_OrgLifecycle(t *testing.T) {
	tests := []struct {
		name string
	}{
		{name: "full_lifecycle_ALL"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			client := createOrganizationsClient(t)

			// CreateOrganization (may already exist from another test run).
			ensureOrg(t, client)

			t.Cleanup(func() {
				_, _ = client.DeleteOrganization(ctx, &organizationsSDK.DeleteOrganizationInput{})
			})

			// DescribeOrganization.
			descOut, err := client.DescribeOrganization(ctx, &organizationsSDK.DescribeOrganizationInput{})
			require.NoError(t, err, "DescribeOrganization should succeed")
			require.NotNil(t, descOut.Organization)
			assert.Equal(t, "ALL", string(descOut.Organization.FeatureSet))

			// ListRoots.
			rootsOut, err := client.ListRoots(ctx, &organizationsSDK.ListRootsInput{})
			require.NoError(t, err, "ListRoots should succeed")
			require.NotEmpty(t, rootsOut.Roots, "organization should have a root")

			rootID := aws.ToString(rootsOut.Roots[0].Id)
			assert.NotEmpty(t, rootID, "root ID should not be empty")
		})
	}
}

// TestIntegration_Organizations_AccountLifecycle tests account creation and description.
func TestIntegration_Organizations_AccountLifecycle(t *testing.T) {
	tests := []struct {
		name        string
		accountName string
		email       string
	}{
		{
			name:        "create_and_describe",
			accountName: "int-test-account",
			email:       "int-test@example.com",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			client := createOrganizationsClient(t)

			// Ensure org exists.
			ensureOrg(t, client)

			// CreateAccount.
			createOut, err := client.CreateAccount(ctx, &organizationsSDK.CreateAccountInput{
				AccountName: aws.String(tt.accountName),
				Email:       aws.String(tt.email),
			})
			require.NoError(t, err, "CreateAccount should succeed")
			require.NotNil(t, createOut.CreateAccountStatus)
			assert.Equal(t, "SUCCEEDED", string(createOut.CreateAccountStatus.State))

			accountID := aws.ToString(createOut.CreateAccountStatus.AccountId)
			assert.NotEmpty(t, accountID, "account ID should not be empty")

			// DescribeAccount.
			descOut, err := client.DescribeAccount(ctx, &organizationsSDK.DescribeAccountInput{
				AccountId: aws.String(accountID),
			})
			require.NoError(t, err, "DescribeAccount should succeed")
			require.NotNil(t, descOut.Account)
			assert.Equal(t, tt.accountName, aws.ToString(descOut.Account.Name))

			// ListAccounts.
			listOut, err := client.ListAccounts(ctx, &organizationsSDK.ListAccountsInput{})
			require.NoError(t, err, "ListAccounts should succeed")

			found := false

			for _, a := range listOut.Accounts {
				if aws.ToString(a.Id) == accountID {
					found = true

					break
				}
			}

			assert.True(t, found, "created account should appear in ListAccounts output")
		})
	}
}

// TestIntegration_Organizations_OULifecycle tests OU creation and description.
func TestIntegration_Organizations_OULifecycle(t *testing.T) {
	tests := []struct {
		name   string
		ouName string
	}{
		{
			name:   "create_and_describe",
			ouName: "int-test-ou",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := t.Context()
			client := createOrganizationsClient(t)

			// Ensure org exists.
			ensureOrg(t, client)

			// Get root ID.
			rootsOut, err := client.ListRoots(ctx, &organizationsSDK.ListRootsInput{})
			require.NoError(t, err, "ListRoots should succeed")
			require.NotEmpty(t, rootsOut.Roots)

			rootID := aws.ToString(rootsOut.Roots[0].Id)

			// CreateOrganizationalUnit.
			createOut, err := client.CreateOrganizationalUnit(ctx, &organizationsSDK.CreateOrganizationalUnitInput{
				ParentId: aws.String(rootID),
				Name:     aws.String(tt.ouName),
			})
			require.NoError(t, err, "CreateOrganizationalUnit should succeed")
			require.NotNil(t, createOut.OrganizationalUnit)

			ouID := aws.ToString(createOut.OrganizationalUnit.Id)
			assert.NotEmpty(t, ouID, "OU ID should not be empty")

			// DescribeOrganizationalUnit.
			descOut, err := client.DescribeOrganizationalUnit(ctx, &organizationsSDK.DescribeOrganizationalUnitInput{
				OrganizationalUnitId: aws.String(ouID),
			})
			require.NoError(t, err, "DescribeOrganizationalUnit should succeed")
			require.NotNil(t, descOut.OrganizationalUnit)
			assert.Equal(t, tt.ouName, aws.ToString(descOut.OrganizationalUnit.Name))

			// ListOrganizationalUnitsForParent.
			listOut, err := client.ListOrganizationalUnitsForParent(ctx, &organizationsSDK.ListOrganizationalUnitsForParentInput{
				ParentId: aws.String(rootID),
			})
			require.NoError(t, err, "ListOrganizationalUnitsForParent should succeed")

			found := false

			for _, ou := range listOut.OrganizationalUnits {
				if aws.ToString(ou.Id) == ouID {
					found = true

					break
				}
			}

			assert.True(t, found, "created OU should appear in ListOrganizationalUnitsForParent output")

			// DeleteOrganizationalUnit.
			_, err = client.DeleteOrganizationalUnit(ctx, &organizationsSDK.DeleteOrganizationalUnitInput{
				OrganizationalUnitId: aws.String(ouID),
			})
			require.NoError(t, err, "DeleteOrganizationalUnit should succeed")
		})
	}
}
