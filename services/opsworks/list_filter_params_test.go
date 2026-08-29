package opsworks_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	opsworkssdk "github.com/aws/aws-sdk-go-v2/service/opsworks"
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
