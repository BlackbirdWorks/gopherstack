package integration_test

import (
	"testing"
	"time"

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

// TestIntegration_CodeCommit_CommentFamily drives the real AWS SDK v2
// CodeCommit client across every op that returns a Comment (gopherstack-gvkf).
// Comment.CreationDate/LastModifiedDate used to be stored and emitted as
// RFC3339 strings, but awsAwsjson11_deserializeDocumentComment
// (codecommit@v1.36.4 deserializers.go:20415,20430) requires a JSON number
// (epoch seconds, via smithytime.ParseEpochSeconds) -- every one of these
// calls failed client-side decode with status 200 and an unreadable body. A
// raw-body/status-code check can't see this class of bug; only a typed SDK
// decode can, which is why this belongs in integration coverage rather than
// the handler-level table tests.
//
// GetCommentsForComparedCommit/GetCommentsForPullRequest carry a second,
// independent bug caught here: the real response is []CommentsForComparedCommit
// / []CommentsForPullRequest, each wrapping a nested Comments list plus
// repositoryName/afterCommitId/beforeCommitId -- not a flat []Comment. Unknown
// top-level JSON keys are silently dropped by the JSON-RPC protocol (no
// error), so this failure mode is an empty Comments slice, not a decode
// error; the subtests below assert on populated content, not just err == nil.
func TestIntegration_CodeCommit_CommentFamily(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	client := createCodeCommitClient(t)

	tests := []struct {
		name string
		run  func(t *testing.T, client *codecommitsdk.Client)
	}{
		{name: "post_comment_for_compared_commit", run: testPostCommentForComparedCommit},
		{name: "post_comment_for_pull_request", run: testPostCommentForPullRequest},
		{name: "post_comment_reply", run: testPostCommentReply},
		{name: "get_comment", run: testGetComment},
		{name: "update_comment", run: testUpdateComment},
		{name: "get_comments_for_compared_commit", run: testGetCommentsForComparedCommit},
		{name: "get_comments_for_pull_request", run: testGetCommentsForPullRequest},
		{name: "delete_comment_content", run: testDeleteCommentContent},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t, client)
		})
	}
}

// createCommentTestRepo creates a uniquely-named repository for a comment
// subtest and registers its cleanup.
func createCommentTestRepo(t *testing.T, client *codecommitsdk.Client, repoName string) {
	t.Helper()

	_, err := client.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
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
}

// createCommentTestPR creates a repo with a main/feature branch pair and a
// pull request between them, for comment subtests that need a PR to comment on.
func createCommentTestPR(t *testing.T, client *codecommitsdk.Client, repoName string) string {
	t.Helper()
	ctx := t.Context()

	createCommentTestRepo(t, client, repoName)

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
		Title: aws.String("comment test pr"),
		Targets: []codecommittypes.Target{
			{
				RepositoryName:       aws.String(repoName),
				SourceReference:      aws.String("feature"),
				DestinationReference: aws.String("main"),
			},
		},
	})
	require.NoError(t, err)

	return aws.ToString(prOut.PullRequest.PullRequestId)
}

func testPostCommentForComparedCommit(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-pcc"
	createCommentTestRepo(t, client, repoName)

	before := time.Now().Add(-time.Minute)

	out, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-commit-1"),
		Content:        aws.String("compared commit comment"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Comment)
	require.NotNil(t, out.Comment.CreationDate)
	require.NotNil(t, out.Comment.LastModifiedDate)

	assert.WithinRange(t, *out.Comment.CreationDate, before, time.Now().Add(time.Minute))
	assert.Equal(t, *out.Comment.CreationDate, *out.Comment.LastModifiedDate)
	assert.Equal(t, "compared commit comment", aws.ToString(out.Comment.Content))
	assert.Equal(t, repoName, aws.ToString(out.RepositoryName))
	assert.Equal(t, "after-commit-1", aws.ToString(out.AfterCommitId))
}

func testPostCommentForPullRequest(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-pcpr"
	prID := createCommentTestPR(t, client, repoName)

	before := time.Now().Add(-time.Minute)

	out, err := client.PostCommentForPullRequest(ctx, &codecommitsdk.PostCommentForPullRequestInput{
		PullRequestId:  aws.String(prID),
		RepositoryName: aws.String(repoName),
		BeforeCommitId: aws.String("before-1"),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("pr comment"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Comment)
	require.NotNil(t, out.Comment.CreationDate)
	require.NotNil(t, out.Comment.LastModifiedDate)

	assert.WithinRange(t, *out.Comment.CreationDate, before, time.Now().Add(time.Minute))
	assert.Equal(t, prID, aws.ToString(out.PullRequestId))
	assert.Equal(t, repoName, aws.ToString(out.RepositoryName))
}

func testPostCommentReply(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-reply"
	createCommentTestRepo(t, client, repoName)

	parent, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("parent comment"),
	})
	require.NoError(t, err)

	before := time.Now().Add(-time.Minute)

	reply, err := client.PostCommentReply(ctx, &codecommitsdk.PostCommentReplyInput{
		InReplyTo: parent.Comment.CommentId,
		Content:   aws.String("reply comment"),
	})
	require.NoError(t, err)
	require.NotNil(t, reply.Comment)
	require.NotNil(t, reply.Comment.CreationDate)

	assert.WithinRange(t, *reply.Comment.CreationDate, before, time.Now().Add(time.Minute))
	assert.Equal(t, aws.ToString(parent.Comment.CommentId), aws.ToString(reply.Comment.InReplyTo))
}

func testGetComment(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-get"
	createCommentTestRepo(t, client, repoName)

	posted, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("get me"),
	})
	require.NoError(t, err)

	out, err := client.GetComment(ctx, &codecommitsdk.GetCommentInput{
		CommentId: posted.Comment.CommentId,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Comment)
	require.NotNil(t, out.Comment.CreationDate)
	assert.Equal(t, *posted.Comment.CreationDate, *out.Comment.CreationDate)
}

func testUpdateComment(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-update"
	createCommentTestRepo(t, client, repoName)

	posted, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("before update"),
	})
	require.NoError(t, err)

	out, err := client.UpdateComment(ctx, &codecommitsdk.UpdateCommentInput{
		CommentId: posted.Comment.CommentId,
		Content:   aws.String("after update"),
	})
	require.NoError(t, err)
	require.NotNil(t, out.Comment)
	require.NotNil(t, out.Comment.LastModifiedDate)
	assert.Equal(t, "after update", aws.ToString(out.Comment.Content))
	assert.False(t, out.Comment.LastModifiedDate.Before(*posted.Comment.LastModifiedDate))
}

func testGetCommentsForComparedCommit(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-gcc"
	createCommentTestRepo(t, client, repoName)

	_, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("compared comment 1"),
	})
	require.NoError(t, err)

	out, err := client.GetCommentsForComparedCommit(ctx, &codecommitsdk.GetCommentsForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-1"),
	})
	require.NoError(t, err)
	require.Len(
		t,
		out.CommentsForComparedCommitData,
		1,
		"real shape is []CommentsForComparedCommit wrapping a nested Comments list, not a flat "+
			"[]Comment; a wrong shape here decodes to zero elements, not an error",
	)

	group := out.CommentsForComparedCommitData[0]
	assert.Equal(t, repoName, aws.ToString(group.RepositoryName))
	assert.Equal(t, "after-1", aws.ToString(group.AfterCommitId))
	require.Len(t, group.Comments, 1)
	assert.Equal(t, "compared comment 1", aws.ToString(group.Comments[0].Content))
	assert.NotNil(t, group.Comments[0].CreationDate)
}

func testGetCommentsForPullRequest(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-gcpr"
	prID := createCommentTestPR(t, client, repoName)

	_, err := client.PostCommentForPullRequest(ctx, &codecommitsdk.PostCommentForPullRequestInput{
		PullRequestId:  aws.String(prID),
		RepositoryName: aws.String(repoName),
		BeforeCommitId: aws.String("before-1"),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("pr comment 1"),
	})
	require.NoError(t, err)

	out, err := client.GetCommentsForPullRequest(ctx, &codecommitsdk.GetCommentsForPullRequestInput{
		PullRequestId: aws.String(prID),
	})
	require.NoError(t, err)
	require.Len(
		t,
		out.CommentsForPullRequestData,
		1,
		"real shape is []CommentsForPullRequest wrapping a nested Comments list, not a flat "+
			"[]Comment; a wrong shape here decodes to zero elements, not an error",
	)

	group := out.CommentsForPullRequestData[0]
	assert.Equal(t, prID, aws.ToString(group.PullRequestId))
	require.Len(t, group.Comments, 1)
	assert.Equal(t, "pr comment 1", aws.ToString(group.Comments[0].Content))
	assert.NotNil(t, group.Comments[0].CreationDate)
}

func testDeleteCommentContent(t *testing.T, client *codecommitsdk.Client) {
	t.Helper()
	ctx := t.Context()

	const repoName = "it-cc-comment-delete"
	createCommentTestRepo(t, client, repoName)

	posted, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String(repoName),
		AfterCommitId:  aws.String("after-1"),
		Content:        aws.String("delete me"),
	})
	require.NoError(t, err)

	out, err := client.DeleteCommentContent(ctx, &codecommitsdk.DeleteCommentContentInput{
		CommentId: posted.Comment.CommentId,
	})
	require.NoError(t, err)
	require.NotNil(t, out.Comment)
	require.NotNil(t, out.Comment.LastModifiedDate)
	assert.True(t, out.Comment.Deleted)
	assert.Empty(t, aws.ToString(out.Comment.Content))
}
