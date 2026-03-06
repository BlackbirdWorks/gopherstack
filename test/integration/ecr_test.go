package integration_test

import (
	"encoding/base64"
	"fmt"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/ecr"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIntegration_ECR_CreateRepository(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := fmt.Sprintf("test-repo-%s", uuid.NewString()[:8])

	out, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})

	require.NoError(t, err)
	require.NotNil(t, out.Repository)
	assert.Equal(t, repoName, *out.Repository.RepositoryName)
	assert.NotEmpty(t, *out.Repository.RepositoryArn)
	assert.NotEmpty(t, *out.Repository.RepositoryUri)
	assert.Contains(t, *out.Repository.RepositoryArn, repoName)
	assert.Contains(t, *out.Repository.RepositoryUri, repoName)
}

func TestIntegration_ECR_CreateRepository_AlreadyExists(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := fmt.Sprintf("duplicate-repo-%s", uuid.NewString()[:8])

	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	_, err = client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RepositoryAlreadyExistsException")
}

func TestIntegration_ECR_DescribeRepositories(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := fmt.Sprintf("describe-repo-%s", uuid.NewString()[:8])

	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	out, err := client.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{repoName},
	})
	require.NoError(t, err)
	require.Len(t, out.Repositories, 1)
	assert.Equal(t, repoName, *out.Repositories[0].RepositoryName)
}

func TestIntegration_ECR_DescribeRepositories_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	_, err := client.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{"nonexistent-repo-" + uuid.NewString()[:8]},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RepositoryNotFoundException")
}

func TestIntegration_ECR_DeleteRepository(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	repoName := fmt.Sprintf("delete-repo-%s", uuid.NewString()[:8])

	_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	out, err := client.DeleteRepository(ctx, &ecr.DeleteRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Repository)
	assert.Equal(t, repoName, *out.Repository.RepositoryName)

	// Confirm deletion
	_, err = client.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{
		RepositoryNames: []string{repoName},
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "RepositoryNotFoundException")
}

func TestIntegration_ECR_GetAuthorizationToken(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	out, err := client.GetAuthorizationToken(ctx, &ecr.GetAuthorizationTokenInput{})
	require.NoError(t, err)
	require.NotEmpty(t, out.AuthorizationData)

	tokenRaw := aws.ToString(out.AuthorizationData[0].AuthorizationToken)
	require.NotEmpty(t, tokenRaw)

	decoded, err := base64.StdEncoding.DecodeString(tokenRaw)
	require.NoError(t, err)
	assert.Equal(t, "AWS:dummy-password", string(decoded))

	assert.NotNil(t, out.AuthorizationData[0].ExpiresAt)
}

func TestIntegration_ECR_ListAllRepositories(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createECRClient(t)
	ctx := t.Context()

	suffix := uuid.NewString()[:8]
	repoNames := []string{
		fmt.Sprintf("list-repo-a-%s", suffix),
		fmt.Sprintf("list-repo-b-%s", suffix),
	}

	for _, name := range repoNames {
		_, err := client.CreateRepository(ctx, &ecr.CreateRepositoryInput{
			RepositoryName: aws.String(name),
		})
		require.NoError(t, err)
	}

	out, err := client.DescribeRepositories(ctx, &ecr.DescribeRepositoriesInput{})
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(out.Repositories), 2)
}
