package opsworks_test

import (
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
	"github.com/aws/aws-sdk-go-v2/service/opsworks/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// createTestStackSDK creates a minimal Stack through the real SDK client
// and returns its StackId, for tests that need a valid FK to register
// resources against.
func createTestStackSDK(t *testing.T, client *opsworkssdk.Client, name string) string {
	t.Helper()

	out, err := client.CreateStack(t.Context(), &opsworkssdk.CreateStackInput{
		Name:                      aws.String(name),
		Region:                    aws.String(rtTestRegion),
		DefaultInstanceProfileArn: aws.String("arn:aws:iam::000000000000:instance-profile/opsworks"),
		ServiceRoleArn:            aws.String("arn:aws:iam::000000000000:role/opsworks"),
	})
	require.NoError(t, err)

	return aws.ToString(out.StackId)
}

// TestDescribeEcsClusters_Pagination proves DescribeEcsClusters applies its
// MaxResults/NextToken parameters (api_op_DescribeEcsClusters.go's Input
// doc comment) instead of always returning every registered cluster, as
// handleDescribeEcsClusters did before this fix (it never read either field
// from the request body).
func TestDescribeEcsClusters_Pagination(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID := createTestStackSDK(t, client, "ecs-pagination-stack")

	registered := make([]string, 0, 3)
	for i := range 3 {
		arn := "arn:aws:ecs:us-east-1:000000000000:cluster/pagination-" + string(rune('a'+i))
		_, err := client.RegisterEcsCluster(t.Context(), &opsworkssdk.RegisterEcsClusterInput{
			EcsClusterArn: aws.String(arn),
			StackId:       aws.String(stackID),
		})
		require.NoError(t, err)
		registered = append(registered, arn)
	}

	first, err := client.DescribeEcsClusters(t.Context(), &opsworkssdk.DescribeEcsClustersInput{
		StackId:    aws.String(stackID),
		MaxResults: aws.Int32(2),
	})
	require.NoError(t, err)
	require.Len(t, first.EcsClusters, 2)
	require.NotEmpty(t, aws.ToString(first.NextToken))

	second, err := client.DescribeEcsClusters(t.Context(), &opsworkssdk.DescribeEcsClustersInput{
		StackId:    aws.String(stackID),
		MaxResults: aws.Int32(2),
		NextToken:  first.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, second.EcsClusters, 1)
	require.Empty(t, aws.ToString(second.NextToken))

	seen := make(map[string]bool)
	for _, c := range append(first.EcsClusters, second.EcsClusters...) {
		seen[aws.ToString(c.EcsClusterArn)] = true
	}
	for _, arn := range registered {
		require.True(t, seen[arn], "expected %s across the two pages", arn)
	}
}

// TestDescribeVolumes_RaidArrayIDExcludesAll proves DescribeVolumes honours
// a non-empty RaidArrayId by excluding every volume, since this backend
// never associates a volume with a RAID array (DescribeRaidArrays always
// returns empty) -- rather than silently ignoring RaidArrayId and returning
// every volume in the stack, as it did before this fix.
func TestDescribeVolumes_RaidArrayIDExcludesAll(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	stackID := createTestStackSDK(t, client, "volume-raid-stack")

	_, err := client.RegisterVolume(t.Context(), &opsworkssdk.RegisterVolumeInput{
		Ec2VolumeId: aws.String("vol-abc123"),
		StackId:     aws.String(stackID),
	})
	require.NoError(t, err)

	unfiltered, err := client.DescribeVolumes(t.Context(), &opsworkssdk.DescribeVolumesInput{
		StackId: aws.String(stackID),
	})
	require.NoError(t, err)
	require.Len(t, unfiltered.Volumes, 1)

	filtered, err := client.DescribeVolumes(t.Context(), &opsworkssdk.DescribeVolumesInput{
		StackId:     aws.String(stackID),
		RaidArrayId: aws.String("nonexistent-raid-array"),
	})
	require.NoError(t, err)
	require.Empty(t, filtered.Volumes)
}

// TestDescribeAgentVersions_ConfigurationManagerFilter covers
// wrapper-key-sweep-opsworks-1: real DescribeAgentVersionsInput
// (opsworks@v1.31.0 api_op_DescribeAgentVersions.go) carries a
// ConfigurationManager field (wire key "ConfigurationManager", a
// StackConfigurationManager{Name,Version} pair -- confirmed against
// awsAwsjson11_serializeOpDocumentDescribeAgentVersionsInput in the pinned
// SDK's serializers.go) that gopherstack's handler never read at all. This
// backend's static agent-version catalog has two Chef entries, versions "12"
// and "11.10"; before the fix, filtering by ConfigurationManager={Chef,
// 11.10} silently returned both entries instead of just the matching one.
func TestDescribeAgentVersions_ConfigurationManagerFilter(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	all, err := client.DescribeAgentVersions(ctx, &opsworkssdk.DescribeAgentVersionsInput{})
	require.NoError(t, err)
	require.Len(t, all.AgentVersions, 2, "sanity: the static catalog has two entries before filtering")

	filtered, err := client.DescribeAgentVersions(ctx, &opsworkssdk.DescribeAgentVersionsInput{
		ConfigurationManager: &types.StackConfigurationManager{
			Name:    aws.String("Chef"),
			Version: aws.String("11.10"),
		},
	})
	require.NoError(t, err)
	require.Len(t, filtered.AgentVersions, 1,
		"ConfigurationManager={Chef,11.10} must return only the matching entry -- "+
			"pre-fix the filter was dropped and both entries came back instead")
	assert.Equal(t, "11.10", aws.ToString(filtered.AgentVersions[0].ConfigurationManager.Version))
}

// TestDescribeEcsClusters_PaginationStableOrder covers
// wrapper-key-sweep-opsworks-2: pkgs/page.New requires "a fully sorted
// slice" (its own doc comment) because its NextToken is a raw positional
// index -- but DescribeEcsClusters (ecs_clusters.go), when called with no
// StackId filter, paginates directly over InMemoryBackend.ecsClusters.All(),
// and pkgs/store.Table.All's own doc comment says its iteration order is Go
// map order, UNSPECIFIED from one call to the next. Walking every page with
// the NextToken the previous page returned can therefore drop or duplicate
// clusters. This creates enough clusters that an unsorted map-order
// paginator is virtually certain to produce a duplicate or a gap across
// several page fetches. (TestDescribeEcsClusters_Pagination above always
// filters by StackId, which goes through ecsClustersByStack, an
// append-ordered index, so it can't catch this -- the bug is specific to
// the unfiltered "list everything" path.)
func TestDescribeEcsClusters_PaginationStableOrder(t *testing.T) {
	t.Parallel()

	client := newTestClient(t)
	ctx := t.Context()

	stackID := createTestStackSDK(t, client, "ecs-order-stack")

	const clusterCount = 25

	want := make(map[string]bool, clusterCount)

	for i := range clusterCount {
		clusterArn := fmt.Sprintf("arn:aws:ecs:us-east-1:000000000000:cluster/order-%02d", i)
		_, registerErr := client.RegisterEcsCluster(ctx, &opsworkssdk.RegisterEcsClusterInput{
			EcsClusterArn: aws.String(clusterArn),
			StackId:       aws.String(stackID),
		})
		require.NoError(t, registerErr)

		want[clusterArn] = true
	}

	got := make(map[string]int, clusterCount)

	var nextToken *string

	for {
		out, describeErr := client.DescribeEcsClusters(ctx, &opsworkssdk.DescribeEcsClustersInput{
			MaxResults: aws.Int32(5),
			NextToken:  nextToken,
		})
		require.NoError(t, describeErr)

		for _, c := range out.EcsClusters {
			got[aws.ToString(c.EcsClusterArn)]++
		}

		if out.NextToken == nil {
			break
		}

		nextToken = out.NextToken
	}

	for clusterArn := range want {
		assert.Equal(t, 1, got[clusterArn], "cluster %s must appear exactly once across all pages", clusterArn)
	}

	assert.Len(t, got, clusterCount,
		"walking every page must yield exactly the clusters created, no drops or duplicates")
}
