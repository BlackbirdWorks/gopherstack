package kms_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	kmssdk "github.com/aws/aws-sdk-go-v2/service/kms"
	kmstypes "github.com/aws/aws-sdk-go-v2/service/kms/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Real AWS: kms's TagResourceInput.Tags is []types.Tag, serialized as an
// array of {"TagKey","TagValue"} objects (aws-sdk-go-v2/service/kms@v1.55.4
// serializers.go:3400-3415, awsAwsjson11_serializeDocumentTag), matching
// this emulator's []kmsTagEntry{TagKey,TagValue} shape exactly.
func Test_SDKRoundTrip_KMS_TagResource_UntagResource_ListResourceTags(t *testing.T) {
	t.Parallel()

	client := newTestKMSClient(t, newTestKMSHandler())
	ctx := t.Context()

	created, err := client.CreateKey(ctx, &kmssdk.CreateKeyInput{})
	require.NoError(t, err)
	keyID := created.KeyMetadata.KeyId

	_, err = client.TagResource(ctx, &kmssdk.TagResourceInput{
		KeyId: keyID,
		Tags: []kmstypes.Tag{
			{TagKey: aws.String("env"), TagValue: aws.String("prod")},
			{TagKey: aws.String("team"), TagValue: aws.String("infra")},
		},
	})
	require.NoError(t, err)

	listed, err := client.ListResourceTags(ctx, &kmssdk.ListResourceTagsInput{KeyId: keyID})
	require.NoError(t, err)

	got := make(map[string]string, len(listed.Tags))
	for _, tag := range listed.Tags {
		got[aws.ToString(tag.TagKey)] = aws.ToString(tag.TagValue)
	}
	assert.Equal(t, map[string]string{"env": "prod", "team": "infra"}, got)

	_, err = client.UntagResource(ctx, &kmssdk.UntagResourceInput{
		KeyId:   keyID,
		TagKeys: []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListResourceTags(ctx, &kmssdk.ListResourceTagsInput{KeyId: keyID})
	require.NoError(t, err)

	gotAfter := make(map[string]string, len(afterUntag.Tags))
	for _, tag := range afterUntag.Tags {
		gotAfter[aws.ToString(tag.TagKey)] = aws.ToString(tag.TagValue)
	}
	assert.Equal(t, map[string]string{"env": "prod"}, gotAfter)
}
