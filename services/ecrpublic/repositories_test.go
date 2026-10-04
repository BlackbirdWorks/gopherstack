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

func TestCreateRepository(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	out, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{
		RepositoryName: aws.String("my-app"),
		CatalogData: &types.RepositoryCatalogDataInput{
			AboutText:        aws.String("about"),
			Description:      aws.String("desc"),
			UsageText:        aws.String("usage"),
			Architectures:    []string{"ARM"},
			OperatingSystems: []string{"Linux"},
			LogoImageBlob:    []byte("logo-bytes"),
		},
		Tags: []types.Tag{{Key: aws.String("team"), Value: aws.String("video")}},
	})
	require.NoError(t, err)
	require.NotNil(t, out.Repository)
	assert.Equal(t, "my-app", aws.ToString(out.Repository.RepositoryName))
	assert.Equal(t, "123456789012", aws.ToString(out.Repository.RegistryId))
	assert.Equal(t, "arn:aws:ecr-public::123456789012:repository/my-app", aws.ToString(out.Repository.RepositoryArn))
	assert.Regexp(t, `^public\.ecr\.aws/[0-9a-f]+/my-app$`, aws.ToString(out.Repository.RepositoryUri))
	require.NotNil(t, out.CatalogData)
	assert.Equal(t, "about", aws.ToString(out.CatalogData.AboutText))
	assert.NotEmpty(t, aws.ToString(out.CatalogData.LogoUrl))
}

func TestCreateRepository_AlreadyExists(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("dup")})
	require.NoError(t, err)

	_, err = client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("dup")})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryAlreadyExistsException", apiErr.ErrorCode())
}

func TestDescribeRepositories(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	for _, name := range []string{"repo-a", "repo-b"} {
		_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String(name)})
		require.NoError(t, err)
	}

	t.Run("list all", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{})
		require.NoError(t, err)
		assert.Len(t, out.Repositories, 2)
	})

	t.Run("filtered", func(t *testing.T) {
		t.Parallel()

		out, err := client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{
			RepositoryNames: []string{"repo-a"},
		})
		require.NoError(t, err)
		require.Len(t, out.Repositories, 1)
		assert.Equal(t, "repo-a", aws.ToString(out.Repositories[0].RepositoryName))
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()

		_, err := client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{
			RepositoryNames: []string{"missing"},
		})
		require.Error(t, err)

		var apiErr smithy.APIError
		require.ErrorAs(t, err, &apiErr)
		assert.Equal(t, "RepositoryNotFoundException", apiErr.ErrorCode())
	})
}

func TestDeleteRepository(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("gone")})
	require.NoError(t, err)

	out, err := client.DeleteRepository(ctx, &ecrpublicsdk.DeleteRepositoryInput{RepositoryName: aws.String("gone")})
	require.NoError(t, err)
	assert.Equal(t, "gone", aws.ToString(out.Repository.RepositoryName))

	_, err = client.DescribeRepositories(ctx, &ecrpublicsdk.DescribeRepositoriesInput{
		RepositoryNames: []string{"gone"},
	})
	require.Error(t, err)
}

func TestDeleteRepository_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.DeleteRepository(t.Context(), &ecrpublicsdk.DeleteRepositoryInput{
		RepositoryName: aws.String("missing"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryNotFoundException", apiErr.ErrorCode())
}

func TestDeleteRepository_NotEmptyWithoutForce(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("has-image")})
	require.NoError(t, err)

	_, err = client.PutImage(ctx, &ecrpublicsdk.PutImageInput{
		RepositoryName: aws.String("has-image"),
		ImageManifest:  aws.String(`{"schemaVersion":2}`),
	})
	require.NoError(t, err)

	_, err = client.DeleteRepository(ctx, &ecrpublicsdk.DeleteRepositoryInput{RepositoryName: aws.String("has-image")})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryNotEmptyException", apiErr.ErrorCode())

	out, err := client.DeleteRepository(ctx, &ecrpublicsdk.DeleteRepositoryInput{
		RepositoryName: aws.String("has-image"),
		Force:          true,
	})
	require.NoError(t, err)
	assert.Equal(t, "has-image", aws.ToString(out.Repository.RepositoryName))
}
