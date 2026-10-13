package inspector2_test

import (
	"slices"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func TestCisListSorting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run     func(t *testing.T, c *inspector2sdk.Client, scanArn string) ([]string, error)
		name    string
		want    []string
		wantErr bool
	}{
		{
			name: "configs name desc",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) ([]string, error) {
				t.Helper()

				out, err := c.ListCisScanConfigurations(t.Context(), &inspector2sdk.ListCisScanConfigurationsInput{
					SortBy:    types.CisScanConfigurationsSortByScanName,
					SortOrder: types.CisSortOrderDesc,
				})
				if err != nil {
					return nil, err
				}

				var got []string
				for _, s := range out.ScanConfigurations {
					got = append(got, aws.ToString(s.ScanName))
				}

				return got, nil
			},
			want: []string{"cis-c", "cis-b", "cis-a"},
		},
		{
			name: "configs bad sortby",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) ([]string, error) {
				t.Helper()

				_, err := c.ListCisScanConfigurations(t.Context(), &inspector2sdk.ListCisScanConfigurationsInput{
					SortBy: types.CisScanConfigurationsSortBy("BOGUS"),
				})

				return nil, err
			},
			wantErr: true,
		},
		{
			name: "targets resource id desc",
			run: func(t *testing.T, c *inspector2sdk.Client, scanArn string) ([]string, error) {
				t.Helper()

				out, err := c.ListCisScanResultsAggregatedByTargetResource(t.Context(),
					&inspector2sdk.ListCisScanResultsAggregatedByTargetResourceInput{
						ScanArn:   aws.String(scanArn),
						SortBy:    types.CisScanResultsAggregatedByTargetResourceSortByAccountId,
						SortOrder: types.CisSortOrderDesc,
					})
				if err != nil {
					return nil, err
				}

				var got []string
				for _, a := range out.TargetResourceAggregations {
					got = append(got, aws.ToString(a.AccountId))
					assert.Equal(t, types.CisTargetStatusCompleted, a.TargetStatus)
				}

				return got, nil
			},
			want: []string{"333333333333", "222222222222"},
		},
		{
			name: "checks check id desc",
			run: func(t *testing.T, c *inspector2sdk.Client, scanArn string) ([]string, error) {
				t.Helper()

				out, err := c.ListCisScanResultsAggregatedByChecks(t.Context(),
					&inspector2sdk.ListCisScanResultsAggregatedByChecksInput{
						ScanArn:   aws.String(scanArn),
						SortBy:    types.CisScanResultsAggregatedByChecksSortByCheckId,
						SortOrder: types.CisSortOrderDesc,
					})
				if err != nil {
					return nil, err
				}

				var got []string
				for _, a := range out.CheckAggregations {
					got = append(got, aws.ToString(a.CheckId))
				}

				return got, nil
			},
			want: []string{"5.2.1", "1.3.1", "1.1.1"},
		},
		{
			name: "details status asc",
			run: func(t *testing.T, c *inspector2sdk.Client, scanArn string) ([]string, error) {
				t.Helper()

				agg, err := c.ListCisScanResultsAggregatedByTargetResource(t.Context(),
					&inspector2sdk.ListCisScanResultsAggregatedByTargetResourceInput{ScanArn: aws.String(scanArn)})
				require.NoError(t, err)
				require.NotEmpty(t, agg.TargetResourceAggregations)

				out, err := c.GetCisScanResultDetails(t.Context(), &inspector2sdk.GetCisScanResultDetailsInput{
					ScanArn:          aws.String(scanArn),
					AccountId:        agg.TargetResourceAggregations[0].AccountId,
					TargetResourceId: agg.TargetResourceAggregations[0].TargetResourceId,
					SortBy:           types.CisScanResultDetailsSortByStatus,
					SortOrder:        types.CisSortOrderAsc,
				})
				if err != nil {
					return nil, err
				}

				var got []string
				for _, d := range out.ScanResultDetails {
					got = append(got, string(d.Status))
				}

				return got, nil
			},
			want: nil,
		},
		{
			name: "scans bad detail level",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) ([]string, error) {
				t.Helper()

				_, err := c.ListCisScans(t.Context(), &inspector2sdk.ListCisScansInput{
					DetailLevel: types.ListCisScansDetailLevel("GLOBAL"),
				})

				return nil, err
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
			accounts := map[string][]any{
				"cis-a": {"222222222222", "333333333333"},
				"cis-b": {"333333333333"},
				"cis-c": {"333333333333"},
			}

			for _, n := range []string{"cis-a", "cis-b", "cis-c"} {
				_, err := backend.CreateCisScanConfiguration(n, "", map[string]any{},
					map[string]any{"accountIds": accounts[n]}, nil)
				require.NoError(t, err)
			}

			client := newRoundTripClient(t, inspector2.NewHandler(backend))
			scans, err := backend.ListCisScans()
			require.NoError(t, err)

			var scanArn string

			for _, s := range scans {
				if s["scanName"] == "cis-a" {
					scanArn, _ = s["scanArn"].(string)
				}
			}

			got, err := tt.run(t, client, scanArn)
			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)

			if tt.want != nil {
				assert.Equal(t, tt.want, got)
			} else {
				assert.True(t, slices.IsSorted(got))
			}
		})
	}
}
