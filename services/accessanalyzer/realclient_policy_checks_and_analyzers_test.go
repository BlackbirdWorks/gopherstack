package accessanalyzer_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	aasdk "github.com/aws/aws-sdk-go-v2/service/accessanalyzer"
	aatypes "github.com/aws/aws-sdk-go-v2/service/accessanalyzer/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/accessanalyzer"
)

// TestRealClient_PolicyChecksAndAnalyzers drives every remaining uncovered op
// through a real aws-sdk-go-v2 accessanalyzer client: CheckAccessNotGranted,
// CheckNoNewAccess, CheckNoPublicAccess, CreateServiceLinkedAnalyzer,
// DeleteServiceLinkedAnalyzer, GetAnalyzedResource, GetFinding,
// GetFindingRecommendation, GetFindingV2, ListAccessPreviews,
// ListAnalyzedResources, ListPolicyGenerations, StartPolicyGeneration,
// StartResourceScan, UpdateAnalyzer, UpdateArchiveRule, UpdateFindings.
func TestRealClient_PolicyChecksAndAnalyzers(t *testing.T) {
	t.Parallel()
	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{name: "policy checks: CheckAccessNotGranted, CheckNoNewAccess, CheckNoPublicAccess", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestAccessAnalyzerClient(t, h)
			ctx := t.Context()

			allowPolicy := `{"Version":"2012-10-17","Statement":[{"Effect":"Allow","Action":"s3:GetObject","Resource":"*"}]}`

			notGranted, err := client.CheckAccessNotGranted(ctx, &aasdk.CheckAccessNotGrantedInput{
				PolicyDocument: aws.String(allowPolicy),
				PolicyType:     aatypes.AccessCheckPolicyTypeIdentityPolicy,
				Access: []aatypes.Access{
					{Actions: []string{"s3:GetObject"}, Resources: []string{"*"}},
				},
			})
			require.NoError(t, err)
			assert.Equal(t, aatypes.CheckAccessNotGrantedResultFail, notGranted.Result)
			assert.NotEmpty(t, notGranted.Reasons)

			noNewAccess, err := client.CheckNoNewAccess(ctx, &aasdk.CheckNoNewAccessInput{
				ExistingPolicyDocument: aws.String(allowPolicy),
				NewPolicyDocument:      aws.String(allowPolicy),
				PolicyType:             aatypes.AccessCheckPolicyTypeIdentityPolicy,
			})
			require.NoError(t, err)
			assert.Equal(t, aatypes.CheckNoNewAccessResultPass, noNewAccess.Result)

			noPublicAccess, err := client.CheckNoPublicAccess(ctx, &aasdk.CheckNoPublicAccessInput{
				PolicyDocument: aws.String(allowPolicy),
				ResourceType:   aatypes.AccessCheckResourceTypeS3Bucket,
			})
			require.NoError(t, err)
			assert.NotEmpty(t, string(noPublicAccess.Result))
		}},
		{
			name: "analyzer lifecycle: CreateServiceLinkedAnalyzer, UpdateAnalyzer, DeleteServiceLinkedAnalyzer",
			run: func(t *testing.T) {
				t.Helper()

				h := newTestHandler(t)
				client := newTestAccessAnalyzerClient(t, h)
				ctx := t.Context()

				created, err := client.CreateServiceLinkedAnalyzer(
					ctx,
					&aasdk.CreateServiceLinkedAnalyzerInput{
						Type: aatypes.TypeAccountUnusedAccess,
					},
				)
				require.NoError(t, err)
				analyzerArn := aws.ToString(created.Arn)
				require.NotEmpty(t, analyzerArn)

				got, err := client.GetAnalyzer(ctx, &aasdk.GetAnalyzerInput{
					AnalyzerName: aws.String(analyzerNameFromARN(analyzerArn)),
				})
				require.NoError(t, err)
				require.NotNil(t, got.Analyzer)

				updated, err := client.UpdateAnalyzer(ctx, &aasdk.UpdateAnalyzerInput{
					AnalyzerName: aws.String(analyzerNameFromARN(analyzerArn)),
					Configuration: &aatypes.AnalyzerConfigurationMemberUnusedAccess{
						Value: aatypes.UnusedAccessConfiguration{UnusedAccessAge: aws.Int32(30)},
					},
				})
				require.NoError(t, err)
				require.NotNil(t, updated.Configuration)
				member, ok := updated.Configuration.(*aatypes.AnalyzerConfigurationMemberUnusedAccess)
				require.True(t, ok)
				assert.EqualValues(t, 30, aws.ToInt32(member.Value.UnusedAccessAge))

				_, err = client.DeleteServiceLinkedAnalyzer(
					ctx,
					&aasdk.DeleteServiceLinkedAnalyzerInput{
						AnalyzerName: aws.String(analyzerNameFromARN(analyzerArn)),
					},
				)
				require.NoError(t, err)

				_, err = client.GetAnalyzer(ctx, &aasdk.GetAnalyzerInput{
					AnalyzerName: aws.String(analyzerNameFromARN(analyzerArn)),
				})
				require.Error(t, err)
			},
		},
		{
			name: "analyzed resources: StartResourceScan, GetAnalyzedResource, ListAnalyzedResources",
			run: func(t *testing.T) {
				t.Helper()

				b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
				h := accessanalyzer.NewHandler(b)
				client := newTestAccessAnalyzerClient(t, h)
				ctx := t.Context()

				analyzer, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
					AnalyzerName: aws.String("slice25-scan-analyzer"),
					Type:         aatypes.TypeAccount,
				})
				require.NoError(t, err)

				resourceArn := "arn:aws:s3:::slice25-scanned-bucket"

				_, err = client.StartResourceScan(ctx, &aasdk.StartResourceScanInput{
					AnalyzerArn: analyzer.Arn,
					ResourceArn: aws.String(resourceArn),
				})
				require.NoError(t, err)

				_, err = b.AddAnalyzedResource(
					aws.ToString(analyzer.Arn),
					resourceArn,
					"AWS::S3::Bucket",
					false,
				)
				require.NoError(t, err)

				got, err := client.GetAnalyzedResource(ctx, &aasdk.GetAnalyzedResourceInput{
					AnalyzerArn: analyzer.Arn,
					ResourceArn: aws.String(resourceArn),
				})
				require.NoError(t, err)
				require.NotNil(t, got.Resource)
				assert.Equal(t, resourceArn, aws.ToString(got.Resource.ResourceArn))

				listOut, err := client.ListAnalyzedResources(ctx, &aasdk.ListAnalyzedResourcesInput{
					AnalyzerArn: analyzer.Arn,
				})
				require.NoError(t, err)
				require.Len(t, listOut.AnalyzedResources, 1)
				assert.Equal(t, resourceArn, aws.ToString(listOut.AnalyzedResources[0].ResourceArn))
			},
		},
		{name: "UpdateArchiveRule", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestAccessAnalyzerClient(t, h)
			ctx := t.Context()

			_, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("slice25-archive-analyzer"),
				Type:         aatypes.TypeAccount,
			})
			require.NoError(t, err)

			_, err = client.CreateArchiveRule(ctx, &aasdk.CreateArchiveRuleInput{
				AnalyzerName: aws.String("slice25-archive-analyzer"),
				RuleName:     aws.String("slice25-rule"),
				Filter: map[string]aatypes.Criterion{
					"resourceType": {Eq: []string{"AWS::S3::Bucket"}},
				},
			})
			require.NoError(t, err)

			_, err = client.UpdateArchiveRule(ctx, &aasdk.UpdateArchiveRuleInput{
				AnalyzerName: aws.String("slice25-archive-analyzer"),
				RuleName:     aws.String("slice25-rule"),
				Filter: map[string]aatypes.Criterion{
					"resourceType": {Eq: []string{"AWS::S3::Object"}},
				},
			})
			require.NoError(t, err)

			got, err := client.GetArchiveRule(ctx, &aasdk.GetArchiveRuleInput{
				AnalyzerName: aws.String("slice25-archive-analyzer"),
				RuleName:     aws.String("slice25-rule"),
			})
			require.NoError(t, err)
			require.NotNil(t, got.ArchiveRule)
			require.Contains(t, got.ArchiveRule.Filter, "resourceType")
			assert.Equal(t, []string{"AWS::S3::Object"}, got.ArchiveRule.Filter["resourceType"].Eq)
		}},
		{name: "findings: GetFinding, UpdateFindings, GetFindingV2, GetFindingRecommendation", run: func(t *testing.T) {
			t.Helper()

			b := accessanalyzer.NewInMemoryBackend("000000000000", "us-east-1")
			h := accessanalyzer.NewHandler(b)
			client := newTestAccessAnalyzerClient(t, h)
			ctx := t.Context()

			analyzer, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("slice25-findings-analyzer"),
				Type:         aatypes.TypeAccount,
			})
			require.NoError(t, err)
			analyzerArn := aws.ToString(analyzer.Arn)

			finding, err := b.AddFinding(
				"slice25-findings-analyzer",
				"AWS::S3::Bucket",
				"arn:aws:s3:::slice25-finding-bucket",
				nil,
				nil,
				nil,
			)
			require.NoError(t, err)

			got, err := client.GetFinding(ctx, &aasdk.GetFindingInput{
				AnalyzerArn: analyzer.Arn,
				Id:          aws.String(finding.ID),
			})
			require.NoError(t, err)
			require.NotNil(t, got.Finding)
			assert.Equal(t, finding.ID, aws.ToString(got.Finding.Id))
			assert.Equal(t, aatypes.FindingStatusActive, got.Finding.Status)

			gotV2, err := client.GetFindingV2(ctx, &aasdk.GetFindingV2Input{
				AnalyzerArn: analyzer.Arn,
				Id:          aws.String(finding.ID),
			})
			require.NoError(t, err)
			assert.Equal(t, aatypes.FindingTypeExternalAccess, gotV2.FindingType)

			_, err = client.UpdateFindings(ctx, &aasdk.UpdateFindingsInput{
				AnalyzerArn: aws.String(analyzerArn),
				Status:      aatypes.FindingStatusUpdateArchived,
				Ids:         []string{finding.ID},
			})
			require.NoError(t, err)

			got, err = client.GetFinding(ctx, &aasdk.GetFindingInput{
				AnalyzerArn: analyzer.Arn,
				Id:          aws.String(finding.ID),
			})
			require.NoError(t, err)
			assert.Equal(t, aatypes.FindingStatusArchived, got.Finding.Status)

			_, err = client.GenerateFindingRecommendation(
				ctx,
				&aasdk.GenerateFindingRecommendationInput{
					AnalyzerArn: analyzer.Arn,
					Id:          aws.String(finding.ID),
				},
			)
			require.NoError(t, err)

			rec, err := client.GetFindingRecommendation(ctx, &aasdk.GetFindingRecommendationInput{
				AnalyzerArn: analyzer.Arn,
				Id:          aws.String(finding.ID),
			})
			require.NoError(t, err)
			assert.Equal(t, finding.ResourceArn, aws.ToString(rec.ResourceArn))
		}},
		{name: "ListAccessPreviews", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestAccessAnalyzerClient(t, h)
			ctx := t.Context()

			analyzer, err := client.CreateAnalyzer(ctx, &aasdk.CreateAnalyzerInput{
				AnalyzerName: aws.String("slice25-preview-analyzer"),
				Type:         aatypes.TypeAccount,
			})
			require.NoError(t, err)

			created, err := client.CreateAccessPreview(ctx, &aasdk.CreateAccessPreviewInput{
				AnalyzerArn: analyzer.Arn,
				Configurations: map[string]aatypes.Configuration{
					"arn:aws:s3:::slice25-preview-bucket": &aatypes.ConfigurationMemberS3Bucket{
						Value: aatypes.S3BucketConfiguration{
							BucketPolicy: aws.String(`{"Version":"2012-10-17","Statement":[]}`),
						},
					},
				},
			})
			require.NoError(t, err)

			listOut, err := client.ListAccessPreviews(ctx, &aasdk.ListAccessPreviewsInput{
				AnalyzerArn: analyzer.Arn,
			})
			require.NoError(t, err)
			var found bool
			for _, ap := range listOut.AccessPreviews {
				if aws.ToString(ap.Id) == aws.ToString(created.Id) {
					found = true
				}
			}
			assert.True(t, found, "created access preview must appear in ListAccessPreviews")
		}},
		{name: "policy generation: StartPolicyGeneration, ListPolicyGenerations", run: func(t *testing.T) {
			t.Helper()

			h := newTestHandler(t)
			client := newTestAccessAnalyzerClient(t, h)
			ctx := t.Context()

			principalArn := "arn:aws:iam::000000000000:role/slice25-role"

			started, err := client.StartPolicyGeneration(ctx, &aasdk.StartPolicyGenerationInput{
				PolicyGenerationDetails: &aatypes.PolicyGenerationDetails{
					PrincipalArn: aws.String(principalArn),
				},
			})
			require.NoError(t, err)
			jobID := aws.ToString(started.JobId)
			require.NotEmpty(t, jobID)

			listOut, err := client.ListPolicyGenerations(ctx, &aasdk.ListPolicyGenerationsInput{
				PrincipalArn: aws.String(principalArn),
			})
			require.NoError(t, err)
			var found bool
			for _, pg := range listOut.PolicyGenerations {
				if aws.ToString(pg.JobId) == jobID {
					found = true
				}
			}
			assert.True(t, found, "started policy generation job must appear in ListPolicyGenerations")
		}}}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

// analyzerNameFromARN extracts the analyzer name from its ARN
// (arn:aws:access-analyzer:region:account:analyzer/name), matching this
// package's own analyzerNameFromArn convention for tests that only have the
// ARN returned by CreateServiceLinkedAnalyzer.
func analyzerNameFromARN(arn string) string {
	for i := len(arn) - 1; i >= 0; i-- {
		if arn[i] == '/' {
			return arn[i+1:]
		}
	}

	return arn
}
