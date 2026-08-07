package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIntegration_CodeCommit_RepositoryLifecycle exercises the core CodeCommit
// workflow via the AWS SDK v2: create a repository, get/list it, then delete.
// Primary integration coverage for the CodeCommit JSON-RPC handler.
func TestIntegration_CodeCommit_RepositoryLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeCommitClient(t)
	ctx := t.Context()

	const repoName = "it-codecommit-repo"

	// CreateRepository.
	createOut, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName:        aws.String(repoName),
		RepositoryDescription: aws.String("integration test repository"),
	})
	require.NoError(t, err)
	require.NotNil(t, createOut.RepositoryMetadata)
	assert.Equal(t, repoName, aws.ToString(createOut.RepositoryMetadata.RepositoryName))

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteRepository(cleanupCtx, &codecommitsdk.DeleteRepositoryInput{
			RepositoryName: aws.String(repoName),
		})
	})

	// GetRepository.
	getOut, err := client.GetRepository(ctx, &codecommitsdk.GetRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.RepositoryMetadata)
	assert.Equal(t, repoName, aws.ToString(getOut.RepositoryMetadata.RepositoryName))

	// ListRepositories should contain the new repo.
	listOut, err := client.ListRepositories(ctx, &codecommitsdk.ListRepositoriesInput{})
	require.NoError(t, err)

	foundRepo := false

	for _, r := range listOut.Repositories {
		if aws.ToString(r.RepositoryName) == repoName {
			foundRepo = true

			break
		}
	}

	assert.True(t, foundRepo, "newly created repository should be listed")
}
