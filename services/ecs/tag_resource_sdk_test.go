package ecs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecssdk "github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real AWS: ecs's TagResourceInput.Tags is []types.Tag, serialized as an
// array of {"key","value"} objects (aws-sdk-go-v2/service/ecs@v1.90.0
// serializers.go:8688-8700, awsAwsjson11_serializeDocumentTag), matching this
// emulator's []Tag{Key,Value} shape already.
func Test_SDKRoundTrip_ECS_TagResource_UntagResource_ListTagsForResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	client := newTestECSClient(t, h)
	ctx := t.Context()

	clusterName := "tag-rt-cluster-" + uuid.NewString()[:8]
	cluster, err := client.CreateCluster(ctx, &ecssdk.CreateClusterInput{
		ClusterName: aws.String(clusterName),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &ecssdk.TagResourceInput{
		ResourceArn: cluster.Cluster.ClusterArn,
		Tags: []ecstypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("infra")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &ecssdk.ListTagsForResourceInput{
		ResourceArn: cluster.Cluster.ClusterArn,
	})
	require.NoError(t, err)

	got := make(map[string]string, len(listed.Tags))
	for _, tag := range listed.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, got)

	_, err = client.UntagResource(ctx, &ecssdk.UntagResourceInput{
		ResourceArn: cluster.Cluster.ClusterArn,
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListTagsForResource(ctx, &ecssdk.ListTagsForResourceInput{
		ResourceArn: cluster.Cluster.ClusterArn,
	})
	require.NoError(t, err)

	gotAfter := make(map[string]string, len(afterUntag.Tags))
	for _, tag := range afterUntag.Tags {
		gotAfter[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod"}, gotAfter)
}
