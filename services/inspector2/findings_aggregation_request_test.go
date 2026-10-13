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

func sevOf(label string) inspector2.FindingSeverity { return inspector2.FindingSeverity{Label: label} }

func seedLayerFindings(t *testing.T, b *inspector2.InMemoryBackend) {
	t.Helper()

	seedAggregationFindings(t, b, []inspector2.Finding{
		{
			Type: "PACKAGE_VULNERABILITY", Severity: sevOf("CRITICAL"), FixAvailable: "YES", ExploitAvailable: "YES",
			Resources: []inspector2.FindingResource{
				{Type: "AWS_ECR_CONTAINER_IMAGE", ID: "sha256:img1", Repository: "repo-a"},
				{Type: "AWS_EC2_INSTANCE", ID: "i-1", ImageID: "ami-111"},
			},
			PackageVulnerabilityDetails: &inspector2.PackageVulnerabilityDetails{
				Source: "NVD", VulnerabilityID: "CVE-1",
				VulnerablePackages: []inspector2.VulnerablePackage{
					{Name: "openssl", Version: "1", SourceLayerHash: "sha256:layer1"},
					{Name: "zlib", Version: "2", SourceLayerHash: "sha256:layer2"},
				},
			},
		},
		{
			Type: "PACKAGE_VULNERABILITY", Severity: sevOf("HIGH"), FixAvailable: "NO",
			Resources: []inspector2.FindingResource{
				{Type: "AWS_ECR_CONTAINER_IMAGE", ID: "sha256:img1", Repository: "repo-a"},
				{Type: "AWS_EC2_INSTANCE", ID: "i-2", ImageID: "ami-111"},
			},
			PackageVulnerabilityDetails: &inspector2.PackageVulnerabilityDetails{
				Source: "NVD", VulnerabilityID: "CVE-2",
				VulnerablePackages: []inspector2.VulnerablePackage{
					{Name: "openssl", Version: "1", SourceLayerHash: "sha256:layer1"},
				},
			},
		},
		{
			Type: "PACKAGE_VULNERABILITY", Severity: sevOf("LOW"), AccountID: "999999999999",
			Resources: []inspector2.FindingResource{
				{Type: "AWS_LAMBDA_FUNCTION", ID: "fn-1", FunctionName: "my-fn"},
				{Type: "AWS_EC2_INSTANCE", ID: "i-3", ImageID: "ami-222"},
			},
			PackageVulnerabilityDetails: &inspector2.PackageVulnerabilityDetails{
				Source: "NVD", VulnerabilityID: "CVE-3",
				VulnerablePackages: []inspector2.VulnerablePackage{
					{Name: "lodash", Version: "4", SourceLambdaLayerArn: "arn:aws:lambda:us-east-1:1:layer:l:1"},
				},
			},
		},
	})
}

func TestListFindingAggregations_DerivedTypes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, out *inspector2sdk.ListFindingAggregationsOutput)
		name  string
		typ   types.AggregationType
	}{
		{
			name: "package", typ: types.AggregationTypePackage,
			check: func(t *testing.T, out *inspector2sdk.ListFindingAggregationsOutput) {
				t.Helper()

				got := map[string]int64{}
				for _, r := range out.Responses {
					p := r.(*types.AggregationResponseMemberPackageAggregation).Value
					got[aws.ToString(p.PackageName)] += aws.ToInt64(p.SeverityCounts.All)
				}

				assert.Equal(t, map[string]int64{"openssl": 2, "zlib": 1, "lodash": 1}, got)
			},
		},
		{
			name: "image layer", typ: types.AggregationTypeImageLayer,
			check: func(t *testing.T, out *inspector2sdk.ListFindingAggregationsOutput) {
				t.Helper()

				require.Len(t, out.Responses, 2)

				for _, r := range out.Responses {
					l := r.(*types.AggregationResponseMemberImageLayerAggregation).Value
					assert.Equal(t, "sha256:img1", aws.ToString(l.ResourceId))
					assert.Equal(t, "repo-a", aws.ToString(l.Repository))

					if aws.ToString(l.LayerHash) == "sha256:layer1" {
						assert.EqualValues(t, 2, aws.ToInt64(l.SeverityCounts.All))
					}
				}
			},
		},
		{
			name: "lambda layer", typ: types.AggregationTypeLambdaLayer,
			check: func(t *testing.T, out *inspector2sdk.ListFindingAggregationsOutput) {
				t.Helper()

				require.Len(t, out.Responses, 1)
				l := out.Responses[0].(*types.AggregationResponseMemberLambdaLayerAggregation).Value
				assert.Equal(t, "arn:aws:lambda:us-east-1:1:layer:l:1", aws.ToString(l.LayerArn))
				assert.Equal(t, "fn-1", aws.ToString(l.ResourceId))
				assert.Equal(t, "my-fn", aws.ToString(l.FunctionName))
			},
		},
		{
			name: "ami", typ: types.AggregationTypeAmi,
			check: func(t *testing.T, out *inspector2sdk.ListFindingAggregationsOutput) {
				t.Helper()

				got := map[string]int64{}
				for _, r := range out.Responses {
					a := r.(*types.AggregationResponseMemberAmiAggregation).Value
					got[aws.ToString(a.Ami)] = aws.ToInt64(a.AffectedInstances)
				}

				assert.Equal(t, map[string]int64{"ami-111": 2, "ami-222": 1}, got)
			},
		},
		{
			name: "finding type", typ: types.AggregationTypeFindingType,
			check: func(t *testing.T, out *inspector2sdk.ListFindingAggregationsOutput) {
				t.Helper()

				got := map[string][2]int64{}
				for _, r := range out.Responses {
					f := r.(*types.AggregationResponseMemberFindingTypeAggregation).Value
					got[aws.ToString(f.AccountId)] = [2]int64{
						aws.ToInt64(f.ExploitAvailableCount),
						aws.ToInt64(f.FixAvailableCount),
					}
				}

				assert.Equal(t, map[string][2]int64{"123456789012": {1, 1}, "999999999999": {0, 0}}, got)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClient(t)
			seedLayerFindings(t, backend)

			out, err := client.ListFindingAggregations(t.Context(), &inspector2sdk.ListFindingAggregationsInput{
				AggregationType: tc.typ,
			})
			require.NoError(t, err)
			assert.Equal(t, tc.typ, out.AggregationType)
			tc.check(t, out)
		})
	}
}

func TestListFindingAggregations_RequestNarrowing(t *testing.T) {
	t.Parallel()

	eq := func(v string) []types.StringFilter {
		return []types.StringFilter{{Comparison: types.StringComparisonEquals, Value: aws.String(v)}}
	}

	tests := []struct {
		in   inspector2sdk.ListFindingAggregationsInput
		name string
		want []string
	}{
		{
			name: "account ids",
			in: inspector2sdk.ListFindingAggregationsInput{
				AggregationType: types.AggregationTypePackage, AccountIds: eq("999999999999"),
			},
			want: []string{"lodash"},
		},
		{
			name: "package name filter",
			in: inspector2sdk.ListFindingAggregationsInput{
				AggregationType: types.AggregationTypePackage,
				AggregationRequest: &types.AggregationRequestMemberPackageAggregation{
					Value: types.PackageAggregation{PackageNames: eq("zlib")},
				},
			},
			want: []string{"zlib"},
		},
		{
			name: "sort all desc",
			in: inspector2sdk.ListFindingAggregationsInput{
				AggregationType: types.AggregationTypePackage,
				AggregationRequest: &types.AggregationRequestMemberPackageAggregation{
					Value: types.PackageAggregation{SortBy: types.PackageSortByAll, SortOrder: types.SortOrderDesc},
				},
			},
			want: []string{"openssl", "zlib", "lodash"},
		},
		{
			name: "sort critical asc",
			in: inspector2sdk.ListFindingAggregationsInput{
				AggregationType: types.AggregationTypePackage,
				AggregationRequest: &types.AggregationRequestMemberPackageAggregation{
					Value: types.PackageAggregation{SortBy: types.PackageSortByCritical, SortOrder: types.SortOrderAsc},
				},
			},
			want: []string{"lodash", "openssl", "zlib"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClient(t)
			seedLayerFindings(t, backend)

			out, err := client.ListFindingAggregations(t.Context(), &tc.in)
			require.NoError(t, err)

			got := make([]string, 0, len(out.Responses))
			for _, r := range out.Responses {
				got = append(
					got,
					aws.ToString(r.(*types.AggregationResponseMemberPackageAggregation).Value.PackageName),
				)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}

func TestListFindingAggregations_InvalidSort(t *testing.T) {
	t.Parallel()

	backend, client := newRealClient(t)
	seedLayerFindings(t, backend)

	_, err := client.ListFindingAggregations(t.Context(), &inspector2sdk.ListFindingAggregationsInput{
		AggregationType: types.AggregationTypePackage,
		AggregationRequest: &types.AggregationRequestMemberPackageAggregation{
			Value: types.PackageAggregation{SortBy: types.PackageSortBy("BOGUS")},
		},
	})
	require.Error(t, err)
}

func TestListFindingAggregations_ResourceDetailFilters(t *testing.T) {
	t.Parallel()

	eq := func(v string) []types.StringFilter {
		return []types.StringFilter{{Comparison: types.StringComparisonEquals, Value: aws.String(v)}}
	}

	tests := []struct {
		req  types.AggregationRequest
		name string
		typ  types.AggregationType
		want string
	}{
		{
			name: "ec2 tags", typ: types.AggregationTypeAwsEc2Instance,
			req: &types.AggregationRequestMemberEc2InstanceAggregation{Value: types.Ec2InstanceAggregation{
				InstanceTags: []types.MapFilter{
					{Comparison: types.MapComparisonEquals, Key: aws.String("env"), Value: aws.String("prod")},
				},
			}},
			want: "i-prod",
		},
		{
			name: "ec2 os", typ: types.AggregationTypeAwsEc2Instance,
			req: &types.AggregationRequestMemberEc2InstanceAggregation{Value: types.Ec2InstanceAggregation{
				OperatingSystems: eq("WINDOWS"),
			}},
			want: "i-win",
		},
		{
			name: "ecr image tag", typ: types.AggregationTypeAwsEcrContainer,
			req: &types.AggregationRequestMemberAwsEcrContainerAggregation{Value: types.AwsEcrContainerAggregation{
				ImageTags: eq("v2"),
			}},
			want: "sha256:img2",
		},
		{
			name: "ecr arch", typ: types.AggregationTypeAwsEcrContainer,
			req: &types.AggregationRequestMemberAwsEcrContainerAggregation{Value: types.AwsEcrContainerAggregation{
				Architectures: eq("arm64"),
			}},
			want: "sha256:img2",
		},
		{
			name: "lambda runtime", typ: types.AggregationTypeAwsLambdaFunction,
			req: &types.AggregationRequestMemberLambdaFunctionAggregation{Value: types.LambdaFunctionAggregation{
				Runtimes: eq("python3.12"),
			}},
			want: "fn-py",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClient(t)
			seedAggregationFindings(t, backend, []inspector2.Finding{
				{Severity: sevOf("HIGH"), Resources: []inspector2.FindingResource{
					{Type: "AWS_EC2_INSTANCE", ID: "i-prod", Tags: map[string]string{"env": "prod"}, Platform: "LINUX"},
				}},
				{Severity: sevOf("HIGH"), Resources: []inspector2.FindingResource{
					{Type: "AWS_EC2_INSTANCE", ID: "i-win", Tags: map[string]string{"env": "dev"}, Platform: "WINDOWS"},
				}},
				{Severity: sevOf("HIGH"), Resources: []inspector2.FindingResource{
					{
						Type:         "AWS_ECR_CONTAINER_IMAGE",
						ID:           "sha256:img1",
						Architecture: "amd64",
						ImageTags:    []string{"v1"},
					},
				}},
				{Severity: sevOf("HIGH"), Resources: []inspector2.FindingResource{
					{
						Type:         "AWS_ECR_CONTAINER_IMAGE",
						ID:           "sha256:img2",
						Architecture: "arm64",
						ImageTags:    []string{"v2"},
					},
				}},
				{Severity: sevOf("HIGH"), Resources: []inspector2.FindingResource{
					{Type: "AWS_LAMBDA_FUNCTION", ID: "fn-py", Runtime: "python3.12"},
				}},
				{Severity: sevOf("HIGH"), Resources: []inspector2.FindingResource{
					{Type: "AWS_LAMBDA_FUNCTION", ID: "fn-node", Runtime: "nodejs20.x"},
				}},
			})

			out, err := client.ListFindingAggregations(t.Context(), &inspector2sdk.ListFindingAggregationsInput{
				AggregationType: tc.typ, AggregationRequest: tc.req,
			})
			require.NoError(t, err)
			require.Len(t, out.Responses, 1)

			var got string

			switch r := out.Responses[0].(type) {
			case *types.AggregationResponseMemberEc2InstanceAggregation:
				got = aws.ToString(r.Value.InstanceId)
			case *types.AggregationResponseMemberAwsEcrContainerAggregation:
				got = aws.ToString(r.Value.ResourceId)
			case *types.AggregationResponseMemberLambdaFunctionAggregation:
				got = aws.ToString(r.Value.ResourceId)
			}

			assert.Equal(t, tc.want, got)
		})
	}
}
