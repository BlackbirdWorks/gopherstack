package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// TestReqFieldSlice3_CodeCommit proves, against the real typed
// aws-sdk-go-v2 codecommit client, that the 7 tier-1 findings this pass
// classified as DROPPED PARAMETER (gopherstack-xhu2t slice 3) are now read
// and actually applied. The other 25 tier-1 findings on this service
// (ConflictDetailLevel/ConflictResolutionStrategy on 10 merge/conflict ops,
// KeepEmptyFolders on 5 merge ops) are MISSING FEATURE -- this backend has no
// per-branch file identity to diff or merge content-wise (see PARITY.md
// items_still_open, pre-existing) -- and are recorded, not tested here.
func TestReqFieldSlice3_CodeCommit(t *testing.T) {
	t.Parallel()

	cases := []struct {
		run  func(t *testing.T)
		name string
	}{
		{testReqField3CCDescribePullRequestEventsMaxResults, "describe_pull_request_events_max_results"},
		{testReqField3CCGetCommentReactionsMaxResults, "get_comment_reactions_max_results"},
		{testReqField3CCGetCommentsForComparedCommitMaxResults, "get_comments_for_compared_commit_max_results"},
		{testReqField3CCGetCommentsForPullRequestMaxResults, "get_comments_for_pull_request_max_results"},
		{testReqField3CCGetDifferencesAfterPath, "get_differences_after_path"},
		{testReqField3CCCreateCommitKeepEmptyFolders, "create_commit_keep_empty_folders"},
		{testReqField3CCDeleteFileKeepEmptyFolders, "delete_file_keep_empty_folders"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			tc.run(t)
		})
	}
}

func newReqField3CCClient(t *testing.T) *codecommitsdk.Client {
	t.Helper()

	backend := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)

	return newTestCodeCommitClient(t, codecommit.NewHandler(backend))
}

func testReqField3CCDescribePullRequestEventsMaxResults(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	created, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("pr"),
		Targets: []types.Target{{
			RepositoryName:  aws.String("repo"),
			SourceReference: aws.String("refs/heads/feature"),
		}},
	})
	require.NoError(t, err)
	prID := aws.ToString(created.PullRequest.PullRequestId)

	// OverridePullRequestApprovalRules unconditionally appends a
	// PULL_REQUEST_APPROVAL_RULE_OVERRIDDEN event -- called twice to get two
	// events to paginate over. revisionId is client-side required by the SDK
	// but this backend doesn't consult it.
	for range 2 {
		_, err = client.OverridePullRequestApprovalRules(ctx, &codecommitsdk.OverridePullRequestApprovalRulesInput{
			PullRequestId:  aws.String(prID),
			OverrideStatus: types.OverrideStatusOverride,
			RevisionId:     created.PullRequest.RevisionId,
		})
		require.NoError(t, err)
	}

	page1, err := client.DescribePullRequestEvents(ctx, &codecommitsdk.DescribePullRequestEventsInput{
		PullRequestId: aws.String(prID),
		MaxResults:    aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.PullRequestEvents, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.DescribePullRequestEvents(ctx, &codecommitsdk.DescribePullRequestEventsInput{
		PullRequestId: aws.String(prID),
		MaxResults:    aws.Int32(1),
		NextToken:     page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.PullRequestEvents, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3CCGetCommentReactionsMaxResults(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	c1, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	comment, err := client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String("repo"),
		AfterCommitId:  aws.String(aws.ToString(c1.CommitId)),
		Content:        aws.String("hi"),
	})
	require.NoError(t, err)
	commentID := aws.ToString(comment.Comment.CommentId)

	for _, emoji := range []string{"THUMBSUP", "THUMBSDOWN"} {
		_, err = client.PutCommentReaction(ctx, &codecommitsdk.PutCommentReactionInput{
			CommentId:     aws.String(commentID),
			ReactionValue: aws.String(emoji),
		})
		require.NoError(t, err)
	}

	page1, err := client.GetCommentReactions(ctx, &codecommitsdk.GetCommentReactionsInput{
		CommentId:  aws.String(commentID),
		MaxResults: aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.ReactionsForComment, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.GetCommentReactions(ctx, &codecommitsdk.GetCommentReactionsInput{
		CommentId:  aws.String(commentID),
		MaxResults: aws.Int32(1),
		NextToken:  page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.ReactionsForComment, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3CCGetCommentsForComparedCommitMaxResults(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	c1, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)
	afterCommitID := aws.ToString(c1.CommitId)

	for range 2 {
		_, err = client.PostCommentForComparedCommit(ctx, &codecommitsdk.PostCommentForComparedCommitInput{
			RepositoryName: aws.String("repo"),
			AfterCommitId:  aws.String(afterCommitID),
			Content:        aws.String("comment"),
		})
		require.NoError(t, err)
	}

	page1, err := client.GetCommentsForComparedCommit(ctx, &codecommitsdk.GetCommentsForComparedCommitInput{
		RepositoryName: aws.String("repo"),
		AfterCommitId:  aws.String(afterCommitID),
		MaxResults:     aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.CommentsForComparedCommitData, 1)
	require.Len(t, page1.CommentsForComparedCommitData[0].Comments, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.GetCommentsForComparedCommit(ctx, &codecommitsdk.GetCommentsForComparedCommitInput{
		RepositoryName: aws.String("repo"),
		AfterCommitId:  aws.String(afterCommitID),
		MaxResults:     aws.Int32(1),
		NextToken:      page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.CommentsForComparedCommitData, 1)
	require.Len(t, page2.CommentsForComparedCommitData[0].Comments, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3CCGetCommentsForPullRequestMaxResults(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	created, err := client.CreatePullRequest(ctx, &codecommitsdk.CreatePullRequestInput{
		Title: aws.String("pr"),
		Targets: []types.Target{{
			RepositoryName:  aws.String("repo"),
			SourceReference: aws.String("refs/heads/feature"),
		}},
	})
	require.NoError(t, err)
	prID := aws.ToString(created.PullRequest.PullRequestId)

	// RepositoryName/BeforeCommitId/AfterCommitId are client-side required by
	// the SDK but this backend's PostCommentForPullRequest doesn't resolve
	// them to real commits.
	for range 2 {
		_, err = client.PostCommentForPullRequest(ctx, &codecommitsdk.PostCommentForPullRequestInput{
			PullRequestId:  aws.String(prID),
			RepositoryName: aws.String("repo"),
			BeforeCommitId: aws.String("before"),
			AfterCommitId:  aws.String("after"),
			Content:        aws.String("comment"),
		})
		require.NoError(t, err)
	}

	page1, err := client.GetCommentsForPullRequest(ctx, &codecommitsdk.GetCommentsForPullRequestInput{
		PullRequestId: aws.String(prID),
		MaxResults:    aws.Int32(1),
	})
	require.NoError(t, err)
	require.Len(t, page1.CommentsForPullRequestData, 1)
	require.Len(t, page1.CommentsForPullRequestData[0].Comments, 1)
	require.NotEmpty(t, aws.ToString(page1.NextToken))

	page2, err := client.GetCommentsForPullRequest(ctx, &codecommitsdk.GetCommentsForPullRequestInput{
		PullRequestId: aws.String(prID),
		MaxResults:    aws.Int32(1),
		NextToken:     page1.NextToken,
	})
	require.NoError(t, err)
	require.Len(t, page2.CommentsForPullRequestData, 1)
	require.Len(t, page2.CommentsForPullRequestData[0].Comments, 1)
	assert.Empty(t, aws.ToString(page2.NextToken))
}

func testReqField3CCGetDifferencesAfterPath(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	commit, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("keep/a.txt"), FileContent: []byte("a")},
			{FilePath: aws.String("other/b.txt"), FileContent: []byte("b")},
		},
	})
	require.NoError(t, err)

	all, err := client.GetDifferences(ctx, &codecommitsdk.GetDifferencesInput{
		RepositoryName:       aws.String("repo"),
		AfterCommitSpecifier: aws.String(aws.ToString(commit.CommitId)),
	})
	require.NoError(t, err)
	require.Len(t, all.Differences, 2)

	filtered, err := client.GetDifferences(ctx, &codecommitsdk.GetDifferencesInput{
		RepositoryName:       aws.String("repo"),
		AfterCommitSpecifier: aws.String(aws.ToString(commit.CommitId)),
		AfterPath:            aws.String("keep"),
	})
	require.NoError(t, err)
	require.Len(t, filtered.Differences, 1)
	assert.Equal(t, "keep/a.txt", aws.ToString(filtered.Differences[0].AfterBlob.Path))
}

func testReqField3CCCreateCommitKeepEmptyFolders(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	c1, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("dir/a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	_, err = client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName:   aws.String("repo"),
		BranchName:       aws.String("main"),
		ParentCommitId:   c1.CommitId,
		DeleteFiles:      []types.DeleteFileEntry{{FilePath: aws.String("dir/a.txt")}},
		KeepEmptyFolders: true,
	})
	require.NoError(t, err)

	folder, err := client.GetFolder(ctx, &codecommitsdk.GetFolderInput{
		RepositoryName: aws.String("repo"),
		FolderPath:     aws.String("dir"),
	})
	require.NoError(t, err)
	require.Len(t, folder.Files, 1)
	assert.Equal(t, "dir/.gitkeep", aws.ToString(folder.Files[0].AbsolutePath))
}

func testReqField3CCDeleteFileKeepEmptyFolders(t *testing.T) {
	t.Helper()

	client := newReqField3CCClient(t)
	ctx := t.Context()

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String("repo"),
	})
	require.NoError(t, err)

	c1, err := client.CreateCommit(ctx, &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("dir2/x.txt"), FileContent: []byte("x")}},
	})
	require.NoError(t, err)

	_, err = client.DeleteFile(ctx, &codecommitsdk.DeleteFileInput{
		RepositoryName:   aws.String("repo"),
		BranchName:       aws.String("main"),
		FilePath:         aws.String("dir2/x.txt"),
		ParentCommitId:   c1.CommitId,
		KeepEmptyFolders: true,
	})
	require.NoError(t, err)

	folder, err := client.GetFolder(ctx, &codecommitsdk.GetFolderInput{
		RepositoryName: aws.String("repo"),
		FolderPath:     aws.String("dir2"),
	})
	require.NoError(t, err)
	require.Len(t, folder.Files, 1)
	assert.Equal(t, "dir2/.gitkeep", aws.ToString(folder.Files[0].AbsolutePath))
}
