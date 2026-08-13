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
	assert.NotNil(
		t,
		conflicts.ConflictMetadataList,
		"conflictMetadataList is the real wire key (gopherstack-lx5h); an SDK client only "+
			"decodes it into a non-nil empty slice when the server actually names the field "+
			"conflictMetadataList — a wrong key like the previous \"conflicts\" leaves this nil",
	)

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

// TestIntegration_CodeCommit_EvaluatePullRequestApprovalRules drives
// EvaluatePullRequestApprovalRules via the real AWS SDK v2 client. The real
// EvaluatePullRequestApprovalRulesOutput wraps everything under a single
// "evaluation" object (types.Evaluation), not an "evaluationResults" array —
// the SDK silently drops an unrecognized top-level key and leaves
// out.Evaluation nil, so a nil-vs-populated assertion on Evaluation is the
// only way to prove the real wire key round-trips (gopherstack-lx5h).
func TestIntegration_CodeCommit_EvaluatePullRequestApprovalRules(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeCommitClient(t)
	ctx := t.Context()

	const repoName = "it-codecommit-evaluate-repo"

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

	prOut, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("evaluate approval rules"),
		Targets: []codecommittypes.Target{
			{
				RepositoryName:       aws.String(repoName),
				SourceReference:      aws.String("feature"),
				DestinationReference: aws.String("main"),
			},
		},
	})
	require.NoError(t, err)
	require.NotNil(t, prOut.PullRequest)
	prID := aws.ToString(prOut.PullRequest.PullRequestId)
	revisionID := aws.ToString(prOut.PullRequest.RevisionId)

	_, err = client.CreatePullRequestApprovalRule(ctx, &codecommitsdk.CreatePullRequestApprovalRuleInput{
		PullRequestId:    aws.String(prID),
		ApprovalRuleName: aws.String("it-rule"),
		ApprovalRuleContent: aws.String(
			`{"Version":"2018-11-08","Statements":[{"Type":"Approvers","NumberOfApprovalsNeeded":1}]}`,
		),
	})
	require.NoError(t, err)

	evalOut, err := client.EvaluatePullRequestApprovalRules(ctx, &codecommitsdk.EvaluatePullRequestApprovalRulesInput{
		PullRequestId: aws.String(prID),
		RevisionId:    aws.String(revisionID),
	})
	require.NoError(t, err)
	require.NotNil(
		t,
		evalOut.Evaluation,
		"real key is \"evaluation\" (singular object); a wrong key like the previous "+
			"\"evaluationResults\" array leaves this nil",
	)
	assert.Contains(t, evalOut.Evaluation.ApprovalRulesSatisfied, "it-rule")
	assert.Empty(t, evalOut.Evaluation.ApprovalRulesNotSatisfied)
	assert.True(t, evalOut.Evaluation.Approved)
	assert.False(t, evalOut.Evaluation.Overridden)
}
