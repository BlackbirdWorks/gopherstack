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

func TestRepositoryCatalogDataLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	_, err := client.CreateRepository(
		ctx,
		&ecrpublicsdk.CreateRepositoryInput{RepositoryName: aws.String("catalog-repo")},
	)
	require.NoError(t, err)

	got, err := client.GetRepositoryCatalogData(ctx, &ecrpublicsdk.GetRepositoryCatalogDataInput{
		RepositoryName: aws.String("catalog-repo"),
	})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.CatalogData.AboutText))

	put, err := client.PutRepositoryCatalogData(ctx, &ecrpublicsdk.PutRepositoryCatalogDataInput{
		RepositoryName: aws.String("catalog-repo"),
		CatalogData: &types.RepositoryCatalogDataInput{
			AboutText:     aws.String("new about"),
			Architectures: []string{"x86-64"},
		},
	})
	require.NoError(t, err)
	assert.Equal(t, "new about", aws.ToString(put.CatalogData.AboutText))

	after, err := client.GetRepositoryCatalogData(ctx, &ecrpublicsdk.GetRepositoryCatalogDataInput{
		RepositoryName: aws.String("catalog-repo"),
	})
	require.NoError(t, err)
	assert.Equal(t, "new about", aws.ToString(after.CatalogData.AboutText))
	assert.Equal(t, []string{"x86-64"}, after.CatalogData.Architectures)
}

func TestGetRepositoryCatalogData_NotFound(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	_, err := client.GetRepositoryCatalogData(t.Context(), &ecrpublicsdk.GetRepositoryCatalogDataInput{
		RepositoryName: aws.String("missing"),
	})
	require.Error(t, err)

	var apiErr smithy.APIError
	require.ErrorAs(t, err, &apiErr)
	assert.Equal(t, "RepositoryNotFoundException", apiErr.ErrorCode())
}

func TestDescribeRegistries(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())

	out, err := client.DescribeRegistries(t.Context(), &ecrpublicsdk.DescribeRegistriesInput{})
	require.NoError(t, err)
	require.Len(t, out.Registries, 1)

	reg := out.Registries[0]
	assert.Equal(t, testAccountID, aws.ToString(reg.RegistryId))
	assert.Equal(t, "arn:aws:ecr-public::123456789012:registry", aws.ToString(reg.RegistryArn))
	require.Len(t, reg.Aliases, 1)
	assert.True(t, reg.Aliases[0].DefaultRegistryAlias)
	assert.True(t, reg.Aliases[0].PrimaryRegistryAlias)
	assert.Equal(t, types.RegistryAliasStatusActive, reg.Aliases[0].Status)
}

func TestRegistryCatalogDataLifecycle(t *testing.T) {
	t.Parallel()

	client := newTestClient(t, newTestHandler())
	ctx := t.Context()

	got, err := client.GetRegistryCatalogData(ctx, &ecrpublicsdk.GetRegistryCatalogDataInput{})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.RegistryCatalogData.DisplayName))

	put, err := client.PutRegistryCatalogData(ctx, &ecrpublicsdk.PutRegistryCatalogDataInput{
		DisplayName: aws.String("My Org"),
	})
	require.NoError(t, err)
	assert.Equal(t, "My Org", aws.ToString(put.RegistryCatalogData.DisplayName))

	after, err := client.GetRegistryCatalogData(ctx, &ecrpublicsdk.GetRegistryCatalogDataInput{})
	require.NoError(t, err)
	assert.Equal(t, "My Org", aws.ToString(after.RegistryCatalogData.DisplayName))
}
