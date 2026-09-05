package efs_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	efssdk "github.com/aws/aws-sdk-go-v2/service/efs"
	efssdktypes "github.com/aws/aws-sdk-go-v2/service/efs/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real AWS: efs's TagResourceInput.Tags is []types.Tag, an array of
// {"Key","Value"} objects (aws-sdk-go-v2/service/efs@v1.44.4
// serializers.go:2883-2898, awsRestjson1_serializeDocumentTag), matching
// this emulator's []tagEntry{Key,Value} shape exactly.
func Test_SDKRoundTrip_EFS_TagResource_UntagResource_ListTagsForResource(t *testing.T) {
	t.Parallel()

	client, _ := newWireTestClient(t)
	ctx := t.Context()

	fsOut, err := client.CreateFileSystem(ctx, &efssdk.CreateFileSystemInput{
		CreationToken: aws.String("tag-rt-token"),
	})
	require.NoError(t, err)

	_, err = client.TagResource(ctx, &efssdk.TagResourceInput{
		ResourceId: fsOut.FileSystemId,
		Tags: []efssdktypes.Tag{
			{Key: aws.String("env"), Value: aws.String("prod")},
			{Key: aws.String("team"), Value: aws.String("infra")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListTagsForResource(ctx, &efssdk.ListTagsForResourceInput{
		ResourceId: fsOut.FileSystemId,
	})
	require.NoError(t, err)

	got := make(map[string]string, len(listed.Tags))
	for _, tag := range listed.Tags {
		got[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, got)

	_, err = client.UntagResource(ctx, &efssdk.UntagResourceInput{
		ResourceId: fsOut.FileSystemId,
		TagKeys:    []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListTagsForResource(ctx, &efssdk.ListTagsForResourceInput{
		ResourceId: fsOut.FileSystemId,
	})
	require.NoError(t, err)

	gotAfter := make(map[string]string, len(afterUntag.Tags))
	for _, tag := range afterUntag.Tags {
		gotAfter[aws.ToString(tag.Key)] = aws.ToString(tag.Value)
	}
	assert.Equal(t, map[string]string{"env": "prod"}, gotAfter)
}
