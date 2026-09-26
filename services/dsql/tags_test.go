package dsql_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	dsqlsdk "github.com/aws/aws-sdk-go-v2/service/dsql"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTagResourceListUntag(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	cluster, err := client.CreateCluster(ctx, &dsqlsdk.CreateClusterInput{Tags: map[string]string{"a": "1"}})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &dsqlsdk.TagResourceInput{
		ResourceArn: cluster.Arn,
		Tags:        map[string]string{"b": "2"},
	})
	require.NoError(t, err)

	listOut, err := client.ListTagsForResource(ctx, &dsqlsdk.ListTagsForResourceInput{ResourceArn: cluster.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"a": "1", "b": "2"}, listOut.Tags)

	_, err = client.UntagResource(ctx, &dsqlsdk.UntagResourceInput{
		ResourceArn: cluster.Arn,
		TagKeys:     []string{"a"},
	})
	require.NoError(t, err)

	listOut, err = client.ListTagsForResource(ctx, &dsqlsdk.ListTagsForResourceInput{ResourceArn: cluster.Arn})
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"b": "2"}, listOut.Tags)
}

func TestTagResource_ClusterNotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.TagResource(t.Context(), &dsqlsdk.TagResourceInput{
		ResourceArn: aws.String("arn:aws:dsql:us-east-1:123456789012:cluster/does-not-exist"),
		Tags:        map[string]string{"a": "1"},
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ResourceNotFoundException")
}

func TestListTagsForResource_InvalidARN(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.ListTagsForResource(t.Context(), &dsqlsdk.ListTagsForResourceInput{
		ResourceArn: aws.String("not-an-arn"),
	})
	require.Error(t, err)
	assertAPIErrorCode(t, err, "ValidationException")
}
