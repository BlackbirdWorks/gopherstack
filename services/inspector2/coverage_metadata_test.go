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

func seedCoverageMetadata(t *testing.T, b *inspector2.InMemoryBackend) {
	t.Helper()

	pulled := time.Unix(1700000000, 0).UTC()

	entries := []inspector2.CoverageEntry{
		{
			ResourceID: "i-1", ResourceType: "AWS_EC2_INSTANCE", ScanType: "PACKAGE",
			ResourceMetadata: &inspector2.CoverageResourceMetadata{Ec2: &inspector2.CoverageEc2Metadata{
				AmiID: "ami-1", Platform: "LINUX", Tags: map[string]string{"env": "prod"},
			}},
		},
		{
			ResourceID: "i-2", ResourceType: "AWS_EC2_INSTANCE", ScanType: "PACKAGE",
			ResourceMetadata: &inspector2.CoverageResourceMetadata{Ec2: &inspector2.CoverageEc2Metadata{
				Tags: map[string]string{"env": "dev"},
			}},
		},
		{
			ResourceID: "img-1", ResourceType: "AWS_ECR_CONTAINER_IMAGE", ScanType: "PACKAGE",
			ResourceMetadata: &inspector2.CoverageResourceMetadata{
				EcrImage: &inspector2.CoverageEcrImageMetadata{
					Tags: []string{"latest", "v1"}, InUseCount: 5, ImagePulledAt: pulled, LastInUseAt: pulled,
				},
				EcrRepository: &inspector2.CoverageEcrRepositoryMetadata{Name: "repo-a", ScanFrequency: "SCAN_ON_PUSH"},
			},
		},
		{
			ResourceID: "img-2", ResourceType: "AWS_ECR_CONTAINER_IMAGE", ScanType: "PACKAGE",
			ResourceMetadata: &inspector2.CoverageResourceMetadata{
				EcrImage:      &inspector2.CoverageEcrImageMetadata{Tags: []string{"v2"}, InUseCount: 1},
				EcrRepository: &inspector2.CoverageEcrRepositoryMetadata{Name: "repo-b"},
			},
		},
		{
			ResourceID: "fn-1", ResourceType: "AWS_LAMBDA_FUNCTION", ScanType: "PACKAGE",
			ResourceMetadata: &inspector2.CoverageResourceMetadata{
				LambdaFunction: &inspector2.CoverageLambdaFunctionMetadata{
					FunctionName: "my-fn", Runtime: "PYTHON_3_12", FunctionTags: map[string]string{"team": "a"},
				},
			},
		},
		{
			ResourceID: "proj-1", ResourceType: "CODE_REPOSITORY", ScanType: "CODE",
			ResourceMetadata: &inspector2.CoverageResourceMetadata{
				CodeRepository: &inspector2.CoverageCodeRepositoryMetadata{
					ProjectName: "proj", ProviderType: "GITHUB", ProviderTypeVisibility: "PRIVATE",
					LastScannedCommitID: "abc123",
				},
			},
		},
		{ResourceID: "bare", ResourceType: "AWS_EC2_INSTANCE", ScanType: "PACKAGE"},
	}

	for _, e := range entries {
		_, err := b.SeedCoverage(e)
		require.NoError(t, err)
	}
}

func sEq(v string) []types.CoverageStringFilter {
	return []types.CoverageStringFilter{{Comparison: types.CoverageStringComparisonEquals, Value: aws.String(v)}}
}

func mEq(k, v string) []types.CoverageMapFilter {
	return []types.CoverageMapFilter{
		{Comparison: types.CoverageMapComparisonEquals, Key: aws.String(k), Value: aws.String(v)},
	}
}

func TestListCoverage_MetadataFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		want   []string
		filter types.CoverageFilterCriteria
	}{
		{
			name:   "ec2 tags",
			filter: types.CoverageFilterCriteria{Ec2InstanceTags: mEq("env", "prod")},
			want:   []string{"i-1"},
		},
		{name: "ec2 tag key only", filter: types.CoverageFilterCriteria{
			Ec2InstanceTags: []types.CoverageMapFilter{
				{Comparison: types.CoverageMapComparisonEquals, Key: aws.String("env")},
			},
		}, want: []string{"i-1", "i-2"}},
		{name: "ecr image tag", filter: types.CoverageFilterCriteria{EcrImageTags: sEq("v2")}, want: []string{"img-2"}},
		{
			name:   "ecr repo",
			filter: types.CoverageFilterCriteria{EcrRepositoryName: sEq("repo-a")},
			want:   []string{"img-1"},
		},
		{name: "ecr in use", filter: types.CoverageFilterCriteria{
			EcrImageInUseCount: []types.CoverageNumberFilter{{LowerInclusive: aws.Int64(3)}},
		}, want: []string{"img-1"}},
		{name: "image pulled", filter: types.CoverageFilterCriteria{
			ImagePulledAt: []types.CoverageDateFilter{{StartInclusive: aws.Time(time.Unix(1699999999, 0))}},
		}, want: []string{"img-1"}},
		{
			name:   "lambda name",
			filter: types.CoverageFilterCriteria{LambdaFunctionName: sEq("my-fn")},
			want:   []string{"fn-1"},
		},
		{
			name:   "lambda runtime",
			filter: types.CoverageFilterCriteria{LambdaFunctionRuntime: sEq("PYTHON_3_12")},
			want:   []string{"fn-1"},
		},
		{
			name:   "lambda tags",
			filter: types.CoverageFilterCriteria{LambdaFunctionTags: mEq("team", "a")},
			want:   []string{"fn-1"},
		},
		{
			name:   "code project",
			filter: types.CoverageFilterCriteria{CodeRepositoryProjectName: sEq("proj")},
			want:   []string{"proj-1"},
		},
		{
			name:   "code provider",
			filter: types.CoverageFilterCriteria{CodeRepositoryProviderType: sEq("GITHUB")},
			want:   []string{"proj-1"},
		},
		{
			name:   "code visibility",
			filter: types.CoverageFilterCriteria{CodeRepositoryProviderTypeVisibility: sEq("PRIVATE")},
			want:   []string{"proj-1"},
		},
		{
			name:   "commit",
			filter: types.CoverageFilterCriteria{LastScannedCommitId: sEq("abc123")},
			want:   []string{"proj-1"},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend, client := newRealClient(t)
			seedCoverageMetadata(t, backend)

			out, err := client.ListCoverage(t.Context(), &inspector2sdk.ListCoverageInput{FilterCriteria: &tc.filter})
			require.NoError(t, err)

			got := make([]string, 0, len(out.CoveredResources))
			for _, r := range out.CoveredResources {
				got = append(got, aws.ToString(r.ResourceId))
			}

			assert.ElementsMatch(t, tc.want, got)
		})
	}
}

func TestListCoverage_MetadataRoundTrip(t *testing.T) {
	t.Parallel()

	backend, client := newRealClient(t)
	seedCoverageMetadata(t, backend)

	out, err := client.ListCoverage(t.Context(), &inspector2sdk.ListCoverageInput{
		FilterCriteria: &types.CoverageFilterCriteria{ResourceId: sEq("img-1")},
	})
	require.NoError(t, err)
	require.Len(t, out.CoveredResources, 1)

	md := out.CoveredResources[0].ResourceMetadata
	require.NotNil(t, md)
	require.NotNil(t, md.EcrImage)
	assert.Equal(t, []string{"latest", "v1"}, md.EcrImage.Tags)
	assert.EqualValues(t, 5, aws.ToInt64(md.EcrImage.InUseCount))
	assert.True(t, md.EcrImage.ImagePulledAt.Equal(time.Unix(1700000000, 0)))
	require.NotNil(t, md.EcrRepository)
	assert.Equal(t, "repo-a", aws.ToString(md.EcrRepository.Name))

	stats, err := client.ListCoverageStatistics(t.Context(), &inspector2sdk.ListCoverageStatisticsInput{
		GroupBy:        types.GroupKeyEcrRepositoryName,
		FilterCriteria: &types.CoverageFilterCriteria{ResourceType: sEq("AWS_ECR_CONTAINER_IMAGE")},
	})
	require.NoError(t, err)

	got := map[string]int64{}
	for _, g := range stats.CountsByGroup {
		got[string(g.GroupKey)] = g.Count
	}

	assert.Equal(t, map[string]int64{"repo-a": 1, "repo-b": 1}, got)
}
