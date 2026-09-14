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

// TestRealClient_Findings drives findings reports, SBOM exports, filters,
// and tags through a real aws-sdk-go-v2 inspector2 client.
func TestRealClient_Findings(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T, backend *inspector2.InMemoryBackend, client *inspector2sdk.Client)
		name string
	}{
		{
			name: "findings_reports_and_sbom",
			run: func(t *testing.T, backend *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				reportOut, err := client.CreateFindingsReport(ctx, &inspector2sdk.CreateFindingsReportInput{
					ReportFormat: types.ReportFormatCsv,
					S3Destination: &types.Destination{
						BucketName: aws.String("my-findings-bucket"),
						KeyPrefix:  aws.String("reports/"),
						KmsKeyArn:  aws.String("arn:aws:kms:us-east-1:123456789012:key/report-key"),
					},
				})
				require.NoError(t, err)
				require.NotNil(t, reportOut.ReportId)

				statusOut, err := client.GetFindingsReportStatus(ctx, &inspector2sdk.GetFindingsReportStatusInput{
					ReportId: reportOut.ReportId,
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(reportOut.ReportId), aws.ToString(statusOut.ReportId))

				_, err = client.CancelFindingsReport(ctx, &inspector2sdk.CancelFindingsReportInput{
					ReportId: reportOut.ReportId,
				})
				require.NoError(t, err)

				sbomOut, err := client.CreateSbomExport(ctx, &inspector2sdk.CreateSbomExportInput{
					ReportFormat: types.SbomReportFormatCyclonedx14,
					S3Destination: &types.Destination{
						BucketName: aws.String("my-sbom-bucket"),
						KeyPrefix:  aws.String("sboms/"),
						KmsKeyArn:  aws.String("arn:aws:kms:us-east-1:123456789012:key/sbom-key"),
					},
				})
				require.NoError(t, err)
				require.NotNil(t, sbomOut.ReportId)

				getSbomOut, err := client.GetSbomExport(ctx, &inspector2sdk.GetSbomExportInput{
					ReportId: sbomOut.ReportId,
				})
				require.NoError(t, err)
				assert.Equal(t, types.SbomReportFormatCyclonedx14, getSbomOut.Format)

				_, err = client.CancelSbomExport(ctx, &inspector2sdk.CancelSbomExportInput{
					ReportId: sbomOut.ReportId,
				})
				require.NoError(t, err)

				findingArn := inspector2.SeedFinding(
					backend, "PACKAGE_VULNERABILITY", "HIGH", "ACTIVE",
					"slice12 finding", "seeded for BatchGetFindingDetails", nil,
				)

				detailsOut, err := client.BatchGetFindingDetails(ctx, &inspector2sdk.BatchGetFindingDetailsInput{
					FindingArns: []string{findingArn},
				})
				require.NoError(t, err)
				require.Len(t, detailsOut.FindingDetails, 1)
				assert.Equal(t, findingArn, aws.ToString(detailsOut.FindingDetails[0].FindingArn))
				assert.Empty(t, detailsOut.Errors)

				snippetOut, err := client.BatchGetCodeSnippet(ctx, &inspector2sdk.BatchGetCodeSnippetInput{
					FindingArns: []string{findingArn},
				})
				require.NoError(t, err)
				require.Len(t, snippetOut.Errors, 1, "no code snippet was ever seeded for this finding")
				assert.Equal(t, findingArn, aws.ToString(snippetOut.Errors[0].FindingArn))
				assert.Equal(t, types.CodeSnippetErrorCode("CODE_SNIPPET_NOT_FOUND"), snippetOut.Errors[0].ErrorCode)

				trialOut, err := client.BatchGetFreeTrialInfo(ctx, &inspector2sdk.BatchGetFreeTrialInfoInput{
					AccountIds: []string{rtTestAccountID},
				})
				require.NoError(t, err)
				require.Len(t, trialOut.Accounts, 1)
				assert.Equal(t, rtTestAccountID, aws.ToString(trialOut.Accounts[0].AccountId))

				searchOut, err := client.SearchVulnerabilities(ctx, &inspector2sdk.SearchVulnerabilitiesInput{
					FilterCriteria: &types.SearchVulnerabilitiesFilterCriteria{
						VulnerabilityIds: []string{"CVE-2024-0001"},
					},
				})
				require.NoError(t, err)
				assert.NotNil(t, searchOut.Vulnerabilities)

				aggOut, err := client.ListFindingAggregations(ctx, &inspector2sdk.ListFindingAggregationsInput{
					AggregationType: types.AggregationTypeAccount,
				})
				require.NoError(t, err)
				assert.NotNil(t, aggOut.Responses)
			},
		},
		{
			name: "filter_update_and_tags",
			run: func(t *testing.T, _ *inspector2.InMemoryBackend, client *inspector2sdk.Client) {
				t.Helper()
				ctx := t.Context()

				createOut, err := client.CreateFilter(ctx, &inspector2sdk.CreateFilterInput{
					Name:           aws.String("slice12-filter"),
					Action:         types.FilterActionNone,
					FilterCriteria: &types.FilterCriteria{},
				})
				require.NoError(t, err)
				require.NotNil(t, createOut.Arn)

				updOut, err := client.UpdateFilter(ctx, &inspector2sdk.UpdateFilterInput{
					FilterArn:   createOut.Arn,
					Action:      types.FilterActionSuppress,
					Description: aws.String("suppress noisy findings"),
				})
				require.NoError(t, err)
				assert.Equal(t, aws.ToString(createOut.Arn), aws.ToString(updOut.Arn))

				_, err = client.TagResource(ctx, &inspector2sdk.TagResourceInput{
					ResourceArn: createOut.Arn,
					Tags:        map[string]string{"team": "security"},
				})
				require.NoError(t, err)

				listTagsOut, err := client.ListTagsForResource(ctx, &inspector2sdk.ListTagsForResourceInput{
					ResourceArn: createOut.Arn,
				})
				require.NoError(t, err)
				assert.Equal(t, "security", listTagsOut.Tags["team"])

				_, err = client.UntagResource(ctx, &inspector2sdk.UntagResourceInput{
					ResourceArn: createOut.Arn,
					TagKeys:     []string{"team"},
				})
				require.NoError(t, err)

				listTagsOut2, err := client.ListTagsForResource(ctx, &inspector2sdk.ListTagsForResourceInput{
					ResourceArn: createOut.Arn,
				})
				require.NoError(t, err)
				assert.NotContains(t, listTagsOut2.Tags, "team")
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
