package ec2_test

// uox6-copyimagetags: CopyImage declares CopyImageTags ("Indicates whether
// to include your resource tags in the copied image. ... Default: Your
// user-defined AMI tags are not copied." -- api_op_CopyImage.go,
// ec2@v1.319.1), but the handler never read it, so a copy never carried the
// source image's tags even when asked. DescribeImages didn't even echo an
// image's TagSet at all before this fix -- honouring CopyImageTags would
// have been unobservable without that, so both are fixed together (the
// same "singular sibling" shape as store.go's own DescribeImages/Describe*
// pattern: the record existed, nothing surfaced it).

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ec2sdk "github.com/aws/aws-sdk-go-v2/service/ec2"
	"github.com/aws/aws-sdk-go-v2/service/ec2/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCopyImage_CopyImageTagsTrue_CopiesSourceTags(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	src, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{Name: aws.String("uox6-src-ami")})
	require.NoError(t, err)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(src.ImageId)},
		Tags:      []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	copied, err := client.CopyImage(t.Context(), &ec2sdk.CopyImageInput{
		SourceImageId: src.ImageId,
		SourceRegion:  aws.String("us-east-1"),
		Name:          aws.String("uox6-copy-ami"),
		CopyImageTags: aws.Bool(true),
	})
	require.NoError(t, err)

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds: []string{aws.ToString(copied.ImageId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Images, 1)
	require.Len(t, out.Images[0].Tags, 1)
	assert.Equal(t, "env", aws.ToString(out.Images[0].Tags[0].Key))
	assert.Equal(t, "prod", aws.ToString(out.Images[0].Tags[0].Value))
}

func TestCopyImage_CopyImageTagsOmitted_DoesNotCopyTags(t *testing.T) {
	t.Parallel()

	_, client := newTestBackendAndClient(t)

	src, err := client.RegisterImage(t.Context(), &ec2sdk.RegisterImageInput{Name: aws.String("uox6-src-ami-2")})
	require.NoError(t, err)

	_, err = client.CreateTags(t.Context(), &ec2sdk.CreateTagsInput{
		Resources: []string{aws.ToString(src.ImageId)},
		Tags:      []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	copied, err := client.CopyImage(t.Context(), &ec2sdk.CopyImageInput{
		SourceImageId: src.ImageId,
		SourceRegion:  aws.String("us-east-1"),
		Name:          aws.String("uox6-copy-ami-2"),
	})
	require.NoError(t, err)

	out, err := client.DescribeImages(t.Context(), &ec2sdk.DescribeImagesInput{
		ImageIds: []string{aws.ToString(copied.ImageId)},
	})
	require.NoError(t, err)
	require.Len(t, out.Images, 1)
	assert.Empty(t, out.Images[0].Tags)
}
