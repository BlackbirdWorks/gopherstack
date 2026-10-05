package inspector2_test

import (
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	inspector2sdk "github.com/aws/aws-sdk-go-v2/service/inspector2"
	"github.com/aws/aws-sdk-go-v2/service/inspector2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

func cisEq(v string) []types.CisStringFilter {
	return []types.CisStringFilter{{Comparison: types.CisStringComparisonEquals, Value: aws.String(v)}}
}

// TestCisListFilters covers FilterCriteria on the CIS list ops (inspector2@v1.54.1 types.go:1138-1213, 3941-3989).
func TestCisListFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *inspector2sdk.Client, scanArn string) []string
		name string
		want []string
	}{
		{
			name: "configs_name_equals",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) []string {
				t.Helper()

				out, err := c.ListCisScanConfigurations(t.Context(), &inspector2sdk.ListCisScanConfigurationsInput{
					FilterCriteria: &types.ListCisScanConfigurationsFilterCriteria{ScanNameFilters: cisEq("cis-b")},
				})
				require.NoError(t, err)

				got := make([]string, 0, len(out.ScanConfigurations))
				for _, s := range out.ScanConfigurations {
					got = append(got, aws.ToString(s.ScanName))
				}

				return got
			},
			want: []string{"cis-b"},
		},
		{
			name: "configs_name_not_equals",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) []string {
				t.Helper()

				out, err := c.ListCisScanConfigurations(t.Context(), &inspector2sdk.ListCisScanConfigurationsInput{
					FilterCriteria: &types.ListCisScanConfigurationsFilterCriteria{
						ScanNameFilters: []types.CisStringFilter{
							{Comparison: types.CisStringComparisonNotEquals, Value: aws.String("cis-a")},
						},
					},
				})
				require.NoError(t, err)

				return []string{
					aws.ToString(out.ScanConfigurations[0].ScanName),
					aws.ToString(out.ScanConfigurations[1].ScanName),
				}
			},
			want: []string{"cis-b", "cis-c"},
		},
		{
			name: "scans_target_account",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) []string {
				t.Helper()

				out, err := c.ListCisScans(t.Context(), &inspector2sdk.ListCisScansInput{
					FilterCriteria: &types.ListCisScansFilterCriteria{TargetAccountIdFilters: cisEq("222222222222")},
				})
				require.NoError(t, err)

				got := make([]string, 0, len(out.Scans))
				for _, s := range out.Scans {
					got = append(got, aws.ToString(s.ScanName))
				}

				return got
			},
			want: []string{"cis-b", "cis-c"},
		},
		{
			name: "scans_failed_checks_range_miss",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) []string {
				t.Helper()

				out, err := c.ListCisScans(t.Context(), &inspector2sdk.ListCisScansInput{
					FilterCriteria: &types.ListCisScansFilterCriteria{
						FailedChecksFilters: []types.CisNumberFilter{{LowerInclusive: aws.Int32(2)}},
					},
				})
				require.NoError(t, err)

				return []string{string(rune('0' + len(out.Scans)))}
			},
			want: []string{"0"},
		},
		{
			name: "scans_scan_at_future_miss",
			run: func(t *testing.T, c *inspector2sdk.Client, _ string) []string {
				t.Helper()

				out, err := c.ListCisScans(t.Context(), &inspector2sdk.ListCisScansInput{
					FilterCriteria: &types.ListCisScansFilterCriteria{
						ScanAtFilters: []types.CisDateFilter{
							{EarliestScanStartTime: aws.Time(time.Now().Add(time.Hour))},
						},
					},
				})
				require.NoError(t, err)

				return []string{string(rune('0' + len(out.Scans)))}
			},
			want: []string{"0"},
		},
		{
			name: "checks_failed_resources",
			run: func(t *testing.T, c *inspector2sdk.Client, scanArn string) []string {
				t.Helper()

				out, err := c.ListCisScanResultsAggregatedByChecks(
					t.Context(),
					&inspector2sdk.ListCisScanResultsAggregatedByChecksInput{
						ScanArn: aws.String(scanArn),
						FilterCriteria: &types.CisScanResultsAggregatedByChecksFilterCriteria{
							FailedResourcesFilters: []types.CisNumberFilter{{LowerInclusive: aws.Int32(1)}},
						},
					},
				)
				require.NoError(t, err)

				got := make([]string, 0, len(out.CheckAggregations))
				for _, a := range out.CheckAggregations {
					got = append(got, aws.ToString(a.CheckId))
				}

				return got
			},
			want: []string{"1.3.1"},
		},
		{
			name: "targets_account_id",
			run: func(t *testing.T, c *inspector2sdk.Client, scanArn string) []string {
				t.Helper()

				out, err := c.ListCisScanResultsAggregatedByTargetResource(t.Context(),
					&inspector2sdk.ListCisScanResultsAggregatedByTargetResourceInput{
						ScanArn: aws.String(scanArn),
						FilterCriteria: &types.CisScanResultsAggregatedByTargetResourceFilterCriteria{
							AccountIdFilters: cisEq("999999999999"),
						},
					})
				require.NoError(t, err)

				return []string{string(rune('0' + len(out.TargetResourceAggregations)))}
			},
			want: []string{"0"},
		},
		{
			name: "details_finding_status",
			run: func(t *testing.T, c *inspector2sdk.Client, scanArn string) []string {
				t.Helper()

				out, err := c.GetCisScanResultDetails(t.Context(), &inspector2sdk.GetCisScanResultDetailsInput{
					ScanArn:          aws.String(scanArn),
					AccountId:        aws.String("111111111111"),
					TargetResourceId: aws.String(firstTargetID(t, c, scanArn)),
					FilterCriteria: &types.CisScanResultDetailsFilterCriteria{
						FindingStatusFilters: []types.CisFindingStatusFilter{
							{Comparison: types.CisFindingStatusComparisonEquals, Value: types.CisFindingStatusFailed},
						},
					},
				})
				require.NoError(t, err)

				got := make([]string, 0, len(out.ScanResultDetails))
				for _, d := range out.ScanResultDetails {
					got = append(got, aws.ToString(d.CheckId))
				}

				return got
			},
			want: []string{"1.3.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
			accounts := map[string]string{"cis-a": "111111111111", "cis-b": "222222222222", "cis-c": "222222222222"}

			for _, n := range []string{"cis-a", "cis-b", "cis-c"} {
				_, err := backend.CreateCisScanConfiguration(n, map[string]any{},
					map[string]any{"accountIds": []any{accounts[n]}}, nil)
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

			assert.ElementsMatch(t, tt.want, tt.run(t, client, scanArn))
		})
	}
}

func firstTargetID(t *testing.T, c *inspector2sdk.Client, scanArn string) string {
	t.Helper()

	out, err := c.ListCisScanResultsAggregatedByTargetResource(t.Context(),
		&inspector2sdk.ListCisScanResultsAggregatedByTargetResourceInput{ScanArn: aws.String(scanArn)})
	require.NoError(t, err)
	require.NotEmpty(t, out.TargetResourceAggregations)

	return aws.ToString(out.TargetResourceAggregations[0].TargetResourceId)
}

// TestListFindingAggregations_Pages checks MaxResults/NextToken in stable key order.
func TestListFindingAggregations_Pages(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		pages [][]string
		size  int32
	}{
		{name: "two_then_one", size: 2, pages: [][]string{{"t-a", "t-b"}, {"t-c"}}},
		{name: "default_one_page", size: 0, pages: [][]string{{"t-a", "t-b", "t-c"}}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := inspector2.NewInMemoryBackend("123456789012", "us-east-1")
			for _, title := range []string{"t-c", "t-a", "t-b"} {
				_, err := backend.SeedFinding(inspector2.Finding{Type: "PACKAGE_VULNERABILITY", Title: title})
				require.NoError(t, err)
			}

			client := newRoundTripClient(t, inspector2.NewHandler(backend))

			var token *string

			for i, want := range tt.pages {
				in := &inspector2sdk.ListFindingAggregationsInput{
					AggregationType: types.AggregationTypeTitle, NextToken: token,
				}
				if tt.size > 0 {
					in.MaxResults = aws.Int32(tt.size)
				}

				out, err := client.ListFindingAggregations(t.Context(), in)
				require.NoError(t, err)

				got := make([]string, 0, len(out.Responses))
				for _, r := range out.Responses {
					got = append(got, aws.ToString(r.(*types.AggregationResponseMemberTitleAggregation).Value.Title))
				}

				assert.Equal(t, want, got)

				if i == len(tt.pages)-1 {
					assert.Nil(t, out.NextToken)
				} else {
					require.NotNil(t, out.NextToken)
				}

				token = out.NextToken
			}
		})
	}
}
