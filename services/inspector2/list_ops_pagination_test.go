package inspector2_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// TestRoundTrip_ListOpsHonourMaxResults pages ops whose body maxResults/
// nextToken members were previously ignored (serializers.go, inspector2@v1.54.1).
func TestRoundTrip_ListOpsHonourMaxResults(t *testing.T) {
	t.Parallel()

	tests := []struct {
		seed  func(t *testing.T, b *inspector2.InMemoryBackend)
		fetch func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string)
		name  string
		total int
	}{
		{
			name: "members", total: 3,
			seed: func(t *testing.T, b *inspector2.InMemoryBackend) {
				t.Helper()

				for _, id := range []string{"111111111111", "222222222222", "333333333333"} {
					require.NoError(t, b.AssociateMember(id))
				}
			},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListMembers(t.Context(), &inspector2sdk.ListMembersInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Members), out.NextToken
			},
		},
		{
			name: "delegated_admins", total: 3,
			seed: func(t *testing.T, b *inspector2.InMemoryBackend) {
				t.Helper()

				for _, id := range []string{"111111111111", "222222222222", "333333333333"} {
					require.NoError(t, b.EnableDelegatedAdminAccount(id))
				}
			},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListDelegatedAdminAccounts(t.Context(), &inspector2sdk.ListDelegatedAdminAccountsInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.DelegatedAdminAccounts), out.NextToken
			},
		},
		{
			name: "usage_totals", total: 3,
			seed: func(*testing.T, *inspector2.InMemoryBackend) {},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListUsageTotals(t.Context(), &inspector2sdk.ListUsageTotalsInput{
					AccountIds: []string{"111111111111", "222222222222", "333333333333"},
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Totals), out.NextToken
			},
		},
		{
			name: "cis_scan_configurations", total: 3,
			seed: seedCisScanConfigurations,
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListCisScanConfigurations(t.Context(), &inspector2sdk.ListCisScanConfigurationsInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.ScanConfigurations), out.NextToken
			},
		},
		{
			name: "cis_scans", total: 3,
			seed: seedCisScanConfigurations,
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListCisScans(t.Context(), &inspector2sdk.ListCisScansInput{
					MaxResults: aws.Int32(1), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Scans), out.NextToken
			},
		},
		{
			name: "code_security_integrations", total: 3,
			seed: func(t *testing.T, b *inspector2.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"int-a", "int-b", "int-c"} {
					_, err := b.CreateCodeSecurityIntegration(n, "GITHUB", nil, nil)
					require.NoError(t, err)
				}
			},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListCodeSecurityIntegrations(
					t.Context(),
					&inspector2sdk.ListCodeSecurityIntegrationsInput{
						MaxResults: aws.Int32(1), NextToken: token,
					},
				)
				require.NoError(t, err)

				return len(out.Integrations), out.NextToken
			},
		},
		{
			name: "code_security_scan_configurations", total: 3,
			seed: func(t *testing.T, b *inspector2.InMemoryBackend) {
				t.Helper()

				for _, n := range []string{"cfg-a", "cfg-b", "cfg-c"} {
					_, err := b.CreateCodeSecurityScanConfiguration(n, "ACCOUNT", []string{"SAST"}, nil, nil, nil, nil)
					require.NoError(t, err)
				}
			},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListCodeSecurityScanConfigurations(
					t.Context(), &inspector2sdk.ListCodeSecurityScanConfigurationsInput{
						MaxResults: aws.Int32(1), NextToken: token,
					},
				)
				require.NoError(t, err)

				return len(out.Configurations), out.NextToken
			},
		},
		{
			name: "code_security_associations", total: 3,
			seed: func(t *testing.T, b *inspector2.InMemoryBackend) {
				t.Helper()

				cfg, err := b.CreateCodeSecurityScanConfiguration(
					"cfg-assoc",
					"ACCOUNT",
					[]string{"SAST"},
					nil,
					nil,
					nil,
					nil,
				)
				require.NoError(t, err)

				_, err = b.BatchAssociateCodeSecurityScanConfiguration(cfg.Arn, []string{"p1", "p2", "p3"})
				require.NoError(t, err)
			},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				cfgs, err := c.ListCodeSecurityScanConfigurations(
					t.Context(), &inspector2sdk.ListCodeSecurityScanConfigurationsInput{},
				)
				require.NoError(t, err)
				require.Len(t, cfgs.Configurations, 1)

				out, err := c.ListCodeSecurityScanConfigurationAssociations(
					t.Context(), &inspector2sdk.ListCodeSecurityScanConfigurationAssociationsInput{
						ScanConfigurationArn: cfgs.Configurations[0].ScanConfigurationArn,
						MaxResults:           aws.Int32(1), NextToken: token,
					},
				)
				require.NoError(t, err)

				return len(out.Associations), out.NextToken
			},
		},
		{
			name: "account_permissions", total: 12,
			seed: func(*testing.T, *inspector2.InMemoryBackend) {},
			fetch: func(t *testing.T, c *inspector2sdk.Client, token *string) (int, *string) {
				t.Helper()

				out, err := c.ListAccountPermissions(t.Context(), &inspector2sdk.ListAccountPermissionsInput{
					MaxResults: aws.Int32(4), NextToken: token,
				})
				require.NoError(t, err)

				return len(out.Permissions), out.NextToken
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
			client := newRoundTripClient(t, inspector2.NewHandler(backend))
			tt.seed(t, backend)

			var token *string

			seen := 0

			for pages := 0; ; pages++ {
				require.Less(t, pages, 20)

				n, next := tt.fetch(t, client, token)
				seen += n

				if next == nil {
					break
				}

				token = next
			}

			require.Equal(t, tt.total, seen)
		})
	}
}

func seedCisScanConfigurations(t *testing.T, b *inspector2.InMemoryBackend) {
	t.Helper()

	for _, n := range []string{"cis-a", "cis-b", "cis-c"} {
		_, err := b.CreateCisScanConfiguration(n, map[string]any{}, map[string]any{}, nil)
		require.NoError(t, err)
	}
}
