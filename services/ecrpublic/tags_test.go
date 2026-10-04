package ecrpublic_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	ecrpublicsdk "github.com/aws/aws-sdk-go-v2/service/ecrpublic"
	"github.com/aws/aws-sdk-go-v2/service/ecrpublic/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func tagMap(tags []types.Tag) map[string]string {
	out := make(map[string]string, len(tags))
	for _, t := range tags {
		out[aws.ToString(t.Key)] = aws.ToString(t.Value)
	}

	return out
}

func TestResourceTagLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
		RepositoryName: aws.String("tagged-repo"),
		Tags:           []types.Tag{{Key: aws.String("team"), Value: aws.String("video")}},
	})
	require.NoError(t, err)

	repoARN := aws.ToString(created.Repository.RepositoryArn)

	listed, err := client.ListTagsForResource(
		ctx,
		&ecrpublicsdk.ListTagsForResourceInput{ResourceArn: aws.String(repoARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "video"}, tagMap(listed.Tags))

	_, err = client.TagResource(ctx, &ecrpublicsdk.TagResourceInput{
		ResourceArn: aws.String(repoARN),
		Tags:        []types.Tag{{Key: aws.String("env"), Value: aws.String("prod")}},
	})
	require.NoError(t, err)

	afterTag, err := client.ListTagsForResource(
		ctx,
		&ecrpublicsdk.ListTagsForResourceInput{ResourceArn: aws.String(repoARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"team": "video", "env": "prod"}, tagMap(afterTag.Tags))

	_, err = client.UntagResource(ctx, &ecrpublicsdk.UntagResourceInput{
		ResourceArn: aws.String(repoARN),
		TagKeys:     []string{"team"},
	})
	require.NoError(t, err)

	afterUntag, err := client.ListTagsForResource(
		ctx,
		&ecrpublicsdk.ListTagsForResourceInput{ResourceArn: aws.String(repoARN)},
	)
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"env": "prod"}, tagMap(afterUntag.Tags))
}

func TestListTagsForResource_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.ListTagsForResource(t.Context(), &ecrpublicsdk.ListTagsForResourceInput{
		ResourceArn: aws.String("arn:aws:ecr-public::123456789012:repository/missing"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryNotFoundException", apiErr.ErrorCode())
}

func TestTagResource_TooManyTags(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	created, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("many-tags")},
	)
	require.NoError(t, err)

	tags := make([]types.Tag, 0, 51)
	for i := range 51 {
		tags = append(tags, types.Tag{Key: aws.String(string(rune('a' + i))), Value: aws.String("v")})
	}

	_, err = client.TagResource(ctx, &ecrpublicsdk.TagResourceInput{
		ResourceArn: created.Repository.RepositoryArn,
		Tags:        tags,
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "TooManyTagsException", apiErr.ErrorCode())
}
