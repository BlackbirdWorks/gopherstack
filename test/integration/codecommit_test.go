package integration_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	codecommittypes "github.com/aws/aws-sdk-go-v2/service/codecommit/types"
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

// TestIntegration_CodeCommit_MergeBranches exercises MergeBranchesBySquash and
// MergeBranchesByThreeWay via the real AWS SDK v2 client, verifying they
// produce distinct commit-graph shapes (one parent vs two) rather than both
// silently delegating to the fast-forward strategy, and that GetMergeConflicts
// reports mergeable rather than the previously-inverted always-false value.
func TestIntegration_CodeCommit_MergeBranches(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeCommitClient(t)
	ctx := t.Context()

	const repoName = "it-codecommit-merge-repo"

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	t.Cleanup(func() {
		cleanupCtx, cancel := cleanupContext(t)
		defer cancel()

		_, _ = client.DeleteRepository(cleanupCtx, &codecommitsdk.DeleteRepositoryInput{
			RepositoryName: aws.String(repoName),
		})
	})

	mainCommit, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("main"),
		AuthorName:     aws.String("it"),
		Email:          aws.String("it@example.com"),
		CommitMessage:  aws.String("initial"),
	})
	require.NoError(t, err)

	_, err = client.CreateBranch(ctx, &codecommitsdk.CreateBranchInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("feature"),
		CommitId:       mainCommit.CommitId,
	})
	require.NoError(t, err)

	_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("feature"),
		AuthorName:     aws.String("it"),
		Email:          aws.String("it@example.com"),
		CommitMessage:  aws.String("on feature"),
		ParentCommitId: mainCommit.CommitId,
	})
	require.NoError(t, err)

	conflicts, err := client.GetMergeConflicts(ctx, &codecommitsdk.GetMergeConflictsInput{
		RepositoryName:             aws.String(repoName),
		SourceCommitSpecifier:      aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
		MergeOption:                codecommittypes.MergeOptionTypeEnumThreeWayMerge,
	})
	require.NoError(t, err)
	assert.True(
		t,
		conflicts.Mergeable,
		"no content-diff engine backs this emulator, so a resolvable merge is always mergeable",
	)
	assert.NotEmpty(t, aws.ToString(conflicts.SourceCommitId))
	assert.NotEmpty(t, aws.ToString(conflicts.DestinationCommitId))

	squashOut, err := client.MergeBranchesBySquash(ctx, &codecommitsdk.MergeBranchesBySquashInput{
		RepositoryName:             aws.String(repoName),
		SourceCommitSpecifier:      aws.String("feature"),
		DestinationCommitSpecifier: aws.String("main"),
		CommitMessage:              aws.String("squash merge"),
	})
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(squashOut.CommitId))

	squashCommit, err := client.GetCommit(ctx, &codecommitsdk.GetCommitInput{
		RepositoryName: aws.String(repoName),
		CommitId:       squashOut.CommitId,
	})
	require.NoError(t, err)
	assert.Len(
		t,
		squashCommit.Commit.Parents,
		1,
		"squash merge commit must have exactly one parent",
	)

	threeWayOut, err := client.MergeBranchesByThreeWay(
		ctx,
		&codecommitsdk.MergeBranchesByThreeWayInput{
			RepositoryName:             aws.String(repoName),
			SourceCommitSpecifier:      aws.String("feature"),
			DestinationCommitSpecifier: aws.String("main"),
		},
	)
	require.NoError(t, err)
	require.NotEmpty(t, aws.ToString(threeWayOut.CommitId))

	threeWayCommit, err := client.GetCommit(ctx, &codecommitsdk.GetCommitInput{
		RepositoryName: aws.String(repoName),
		CommitId:       threeWayOut.CommitId,
	})
	require.NoError(t, err)
	assert.Len(t, threeWayCommit.Commit.Parents, 2, "three-way merge commit must have two parents")
}
