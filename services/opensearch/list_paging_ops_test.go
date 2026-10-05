package opensearch_test

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opensearchsdk "github.com/aws/aws-sdk-go-v2/service/opensearch"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/opensearch"
)

type osKeyPageFn func(
	ctx context.Context, c *opensearchsdk.Client, size int32, token *string,
) ([]string, *string, error)

// TestListOps_MaxResultsPagesEveryItemOnce pages package, upgrade and scheduled-action lists at MaxResults=2.
func TestListOps_MaxResultsPagesEveryItemOnce(t *testing.T) {
	t.Parallel()

	const (
		domain = "paged-ops-domain"
		pkgID  = "F1"
	)

	tests := []struct {
		list osKeyPageFn
		seed func(t *testing.T, b *opensearch.InMemoryBackend)
		name string
	}{
		{
			name: "package_version_history",
			seed: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreatePackage("pkg", "TXT-DICTIONARY", "d", nil, nil)
				require.NoError(t, err)

				for range 2 {
					_, err = b.UpdatePackage(pkgID, "next")
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz int32, tok *string) ([]string, *string, error) {
				out, err := c.GetPackageVersionHistory(ctx, &opensearchsdk.GetPackageVersionHistoryInput{
					PackageID: aws.String(pkgID), MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return nil, nil, err
				}

				ids := make([]string, 0, len(out.PackageVersionHistoryList))
				for _, v := range out.PackageVersionHistoryList {
					ids = append(ids, aws.ToString(v.PackageVersion))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "upgrade_history",
			seed: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: domain})
				require.NoError(t, err)

				for _, n := range []string{"up-a", "up-b", "up-c"} {
					require.NoError(t, b.UpgradeDomain(domain, n))
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz int32, tok *string) ([]string, *string, error) {
				out, err := c.GetUpgradeHistory(ctx, &opensearchsdk.GetUpgradeHistoryInput{
					DomainName: aws.String(domain), MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return nil, nil, err
				}

				ids := make([]string, 0, len(out.UpgradeHistories))
				for _, v := range out.UpgradeHistories {
					ids = append(ids, aws.ToString(v.UpgradeName))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "domains_for_package",
			seed: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreatePackage("pkg", "TXT-DICTIONARY", "d", nil, nil)
				require.NoError(t, err)

				for _, n := range []string{"dom-a", "dom-b", "dom-c"} {
					_, err = b.CreateDomain(opensearch.CreateDomainInput{Name: n})
					require.NoError(t, err)

					_, err = b.AssociatePackage(pkgID, n)
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz int32, tok *string) ([]string, *string, error) {
				out, err := c.ListDomainsForPackage(ctx, &opensearchsdk.ListDomainsForPackageInput{
					PackageID: aws.String(pkgID), MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return nil, nil, err
				}

				ids := make([]string, 0, len(out.DomainPackageDetailsList))
				for _, v := range out.DomainPackageDetailsList {
					ids = append(ids, aws.ToString(v.DomainName))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "packages_for_domain",
			seed: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: domain})
				require.NoError(t, err)

				for _, id := range []string{"F1", "F2", "F3"} {
					_, err = b.CreatePackage("pkg-"+id, "TXT-DICTIONARY", "d", nil, nil)
					require.NoError(t, err)

					_, err = b.AssociatePackage(id, domain)
					require.NoError(t, err)
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz int32, tok *string) ([]string, *string, error) {
				out, err := c.ListPackagesForDomain(ctx, &opensearchsdk.ListPackagesForDomainInput{
					DomainName: aws.String(domain), MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return nil, nil, err
				}

				ids := make([]string, 0, len(out.DomainPackageDetailsList))
				for _, v := range out.DomainPackageDetailsList {
					ids = append(ids, aws.ToString(v.PackageID))
				}

				return ids, out.NextToken, nil
			},
		},
		{
			name: "scheduled_actions",
			seed: func(t *testing.T, b *opensearch.InMemoryBackend) {
				t.Helper()

				_, err := b.CreateDomain(opensearch.CreateDomainInput{Name: domain})
				require.NoError(t, err)

				for _, id := range []string{"sa-1", "sa-2", "sa-3"} {
					opensearch.AddScheduledActionInternal(b, domain, &opensearch.ScheduledAction{
						ID: id, Type: "SERVICE_SOFTWARE_UPDATE", Severity: "LOW", Status: "PENDING_UPDATE",
					})
				}
			},
			list: func(ctx context.Context, c *opensearchsdk.Client, sz int32, tok *string) ([]string, *string, error) {
				out, err := c.ListScheduledActions(ctx, &opensearchsdk.ListScheduledActionsInput{
					DomainName: aws.String(domain), MaxResults: sz, NextToken: tok,
				})
				if err != nil {
					return nil, nil, err
				}

				ids := make([]string, 0, len(out.ScheduledActions))
				for _, v := range out.ScheduledActions {
					ids = append(ids, aws.ToString(v.Id))
				}

				return ids, out.NextToken, nil
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := opensearch.NewInMemoryBackend(testAccountID, testRegion)
			c := newTestOpenSearchClient(t, opensearch.NewHandler(b))
			tt.seed(t, b)

			all, next, err := tt.list(t.Context(), c, 0, nil)
			require.NoError(t, err)
			require.Len(t, all, 3)
			assert.Empty(t, aws.ToString(next))

			first, next, err := tt.list(t.Context(), c, 2, nil)
			require.NoError(t, err)
			assert.Equal(t, all[:2], first)
			require.NotEmpty(t, aws.ToString(next))

			rest, next, err := tt.list(t.Context(), c, 2, next)
			require.NoError(t, err)
			assert.Equal(t, all[2:], rest)
			assert.Empty(t, aws.ToString(next))
		})
	}
}
