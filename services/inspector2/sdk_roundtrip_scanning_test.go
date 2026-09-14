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

// TestRealClient_Scanning drives CIS scan configuration and coverage ops
// through a real aws-sdk-go-v2 inspector2 client.
func TestRealClient_Scanning(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *inspector2.InMemoryBackend, client *inspector2sdk.Client)
		name string
	}{
		{
			name: "cis_scan_lifecycle",
			run: func(t *testing.T, _ *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				createOut, err := client.CreateCisScanConfiguration(
					ctx, &inspector2sdk.CreateCisScanConfigurationInput{
						ScanName:      aws.String("nightly-cis-scan"),
						SecurityLevel: types.CisSecurityLevelLevel1,
						Schedule: &types.ScheduleMemberDaily{
							Value: types.DailySchedule{
								StartTime: &types.Time{TimeOfDay: aws.String("02:00"), Timezone: aws.String("UTC")},
							},
						},
						Targets: &types.CreateCisTargets{
							AccountIds:         []string{rtTestAccountID},
							TargetResourceTags: map[string][]string{},
						},
					},
				)
				require.NoError(t, err)
				require.NotNil(t, createOut.ScanConfigurationArn)

				_, err = client.UpdateCisScanConfiguration(ctx, &inspector2sdk.UpdateCisScanConfigurationInput{
					ScanConfigurationArn: createOut.ScanConfigurationArn,
					ScanName:             aws.String("nightly-cis-scan-renamed"),
				})
				require.NoError(t, err)

				listCfgs, err := client.ListCisScanConfigurations(ctx, &inspector2sdk.ListCisScanConfigurationsInput{})
				require.NoError(t, err)
				require.Len(t, listCfgs.ScanConfigurations, 1)

				listScans, err := client.ListCisScans(ctx, &inspector2sdk.ListCisScansInput{})
				require.NoError(t, err)
				require.Len(t, listScans.Scans, 1)
				scanArn := listScans.Scans[0].ScanArn
				require.NotNil(t, scanArn)

				reportOut, err := client.GetCisScanReport(ctx, &inspector2sdk.GetCisScanReportInput{
					ScanArn: scanArn,
				})
				require.NoError(t, err)
				assert.Equal(t, types.CisReportStatusSucceeded, reportOut.Status)

				resourcesOut, err := client.ListCisScanResultsAggregatedByTargetResource(
					ctx, &inspector2sdk.ListCisScanResultsAggregatedByTargetResourceInput{ScanArn: scanArn},
				)
				require.NoError(t, err)
				require.Len(t, resourcesOut.TargetResourceAggregations, 1)
				target := resourcesOut.TargetResourceAggregations[0]

				// AccountId/TargetResourceId are both real, required
				// GetCisScanResultDetailsInput members: previously silently
				// dropped, so every real client's scoped request returned
				// every check result for the whole scan.
				detailsOut, err := client.GetCisScanResultDetails(ctx, &inspector2sdk.GetCisScanResultDetailsInput{
					ScanArn:          scanArn,
					AccountId:        target.AccountId,
					TargetResourceId: target.TargetResourceId,
				})
				require.NoError(t, err)
				require.NotEmpty(t, detailsOut.ScanResultDetails)
				for _, d := range detailsOut.ScanResultDetails {
					assert.Equal(t, aws.ToString(target.TargetResourceId), aws.ToString(d.TargetResourceId))
				}

				checksOut, err := client.ListCisScanResultsAggregatedByChecks(
					ctx, &inspector2sdk.ListCisScanResultsAggregatedByChecksInput{ScanArn: scanArn},
				)
				require.NoError(t, err)
				assert.NotNil(t, checksOut.CheckAggregations)

				startOut, err := client.StartCisSession(ctx, &inspector2sdk.StartCisSessionInput{
					ScanJobId: aws.String("scan-job-slice12"),
					Message:   &types.StartCisSessionMessage{SessionToken: aws.String("session-token-1")},
				})
				require.NoError(t, err)
				_ = startOut

				// SessionToken is required client-side on all three ops
				// below, but this package's CIS session health/telemetry/
				// stop lifecycle is a disclosed no-op beyond routing/basic
				// state (PARITY.md deferred list) -- not re-litigated here,
				// just satisfied so the real client can send the request at
				// all.
				_, err = client.SendCisSessionHealth(ctx, &inspector2sdk.SendCisSessionHealthInput{
					ScanJobId:    aws.String("scan-job-slice12"),
					SessionToken: aws.String("session-token-1"),
				})
				require.NoError(t, err)

				_, err = client.SendCisSessionTelemetry(ctx, &inspector2sdk.SendCisSessionTelemetryInput{
					ScanJobId:    aws.String("scan-job-slice12"),
					SessionToken: aws.String("session-token-1"),
					Messages:     []types.CisSessionMessage{},
				})
				require.NoError(t, err)

				_, err = client.StopCisSession(ctx, &inspector2sdk.StopCisSessionInput{
					ScanJobId:    aws.String("scan-job-slice12"),
					SessionToken: aws.String("session-token-1"),
					Message: &types.StopCisSessionMessage{
						Status:   types.StopCisSessionStatusSuccess,
						Progress: &types.StopCisMessageProgress{},
					},
				})
				require.NoError(t, err)

				_, err = client.DeleteCisScanConfiguration(ctx, &inspector2sdk.DeleteCisScanConfigurationInput{
					ScanConfigurationArn: createOut.ScanConfigurationArn,
				})
				require.NoError(t, err)
			},
		},
		{
			name: "coverage_and_clusters",
			run: func(t *testing.T, backend *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				_, err := backend.SeedCoverage(inspector2.CoverageEntry{
					ResourceID:   "i-slice12coverage",
					ResourceType: "AWS_EC2_INSTANCE",
					ScanType:     "NETWORK",
				})
				require.NoError(t, err)

				listOut, err := client.ListCoverage(ctx, &inspector2sdk.ListCoverageInput{})
				require.NoError(t, err)
				require.Len(t, listOut.CoveredResources, 1)
				assert.Equal(t, "i-slice12coverage", aws.ToString(listOut.CoveredResources[0].ResourceId))

				statsOut, err := client.ListCoverageStatistics(ctx, &inspector2sdk.ListCoverageStatisticsInput{})
				require.NoError(t, err)
				assert.NotZero(t, aws.ToInt64(statsOut.TotalCounts))

				clustersOut, err := client.GetClustersForImage(ctx, &inspector2sdk.GetClustersForImageInput{
					Filter: &types.ClusterForImageFilterCriteria{
						ResourceId: aws.String("i-slice12coverage"),
					},
				})
				require.NoError(t, err)
				assert.Empty(t, clustersOut.Cluster)
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
