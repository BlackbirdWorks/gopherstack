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

func seedRepoWithCommit(t *testing.T, client *codecommitsdk.Client) string {
	t.Helper()

	_, err := client.CreateRepository(
		t.Context(),
		&codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")},
	)
	require.NoError(t, err)

	out, err := client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles:       []types.PutFileEntry{{FilePath: aws.String("a.txt"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	return aws.ToString(out.CommitId)
}

func postComparedComment(
	t *testing.T, c *codecommitsdk.Client, content, token string,
) (string, error) {
	t.Helper()

	out, err := c.PostCommentForComparedCommit(t.Context(), &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName:     aws.String("repo"),
		AfterCommitId:      aws.String("abc"),
		Content:            aws.String(content),
		ClientRequestToken: aws.String(token),
	})
	if err != nil {
		return "", err
	}

	return aws.ToString(out.Comment.CommentId), nil
}

func createPR(t *testing.T, c *codecommitsdk.Client, title, token string) (string, error) {
	t.Helper()

	out, err := c.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
		Title:              aws.String(title),
		ClientRequestToken: aws.String(token),
		Targets: []types.Target{
			{RepositoryName: aws.String("repo"), SourceReference: aws.String("main")},
		},
	})
	if err != nil {
		return "", err
	}

	return aws.ToString(out.PullRequest.PullRequestId), nil
}

func TestClientRequestTokenReplay(t *testing.T) {
	t.Parallel()

	tests := []struct {
		create func(t *testing.T, c *codecommitsdk.Client, variant string) (string, error)
		name   string
	}{
		{
			name: "compared_commit_comment",
			create: func(t *testing.T, c *codecommitsdk.Client, variant string) (string, error) {
				t.Helper()

				return postComparedComment(t, c, variant, "tok")
			},
		},
		{
			name: "pull_request",
			create: func(t *testing.T, c *codecommitsdk.Client, variant string) (string, error) {
				t.Helper()

				return createPR(t, c, variant, "tok")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			seedRepoWithCommit(t, client)

			first, err := tt.create(t, client, "same")
			require.NoError(t, err)

			replay, err := tt.create(t, client, "same")
			require.NoError(t, err)
			assert.Equal(t, first, replay)

			_, err = tt.create(t, client, "changed")
			require.ErrorContains(t, err, "IdempotencyParameterMismatchException")
		})
	}
}

func TestPostCommentLocationAndReplies(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	seedRepoWithCommit(t, client)

	post := func(path string, pos int64, content string) *codecommitsdk.PostCommentForComparedCommitOutput {
		out, err := client.PostCommentForComparedCommit(t.Context(), &codecommitsdk.PostCommentForComparedCommitInput{
			RepositoryName: aws.String("repo"),
			BeforeCommitId: aws.String("before"),
			AfterCommitId:  aws.String("after"),
			Content:        aws.String(content),
			Location: &types.Location{
				FilePath:            aws.String(path),
				FilePosition:        aws.Int64(pos),
				RelativeFileVersion: types.RelativeFileVersionEnumAfter,
			},
		})
		require.NoError(t, err)

		return out
	}

	first := post("a.txt", 3, "first")
	require.NotNil(t, first.Location)
	assert.Equal(t, "a.txt", aws.ToString(first.Location.FilePath))
	assert.Equal(t, "before", aws.ToString(first.BeforeCommitId))

	post("b.txt", 9, "second")

	_, err := client.PostCommentReply(t.Context(), &codecommitsdk.PostCommentReplyInput{
		InReplyTo: first.Comment.CommentId, Content: aws.String("reply"),
	})
	require.NoError(t, err)

	got, err := client.GetCommentsForComparedCommit(t.Context(), &codecommitsdk.GetCommentsForComparedCommitInput{
		RepositoryName: aws.String("repo"), AfterCommitId: aws.String("after"),
	})
	require.NoError(t, err)
	require.Len(t, got.CommentsForComparedCommitData, 2)

	byPath := map[string]int{}

	for _, g := range got.CommentsForComparedCommitData {
		require.NotNil(t, g.Location)
		byPath[aws.ToString(g.Location.FilePath)] = len(g.Comments)
		assert.Equal(t, "before", aws.ToString(g.BeforeCommitId))
	}

	assert.Equal(t, map[string]int{"a.txt": 2, "b.txt": 1}, byPath)

	filtered, err := client.GetCommentsForComparedCommit(t.Context(), &codecommitsdk.GetCommentsForComparedCommitInput{
		RepositoryName: aws.String("repo"), AfterCommitId: aws.String("after"), BeforeCommitId: aws.String("other"),
	})
	require.NoError(t, err)
	assert.Empty(t, filtered.CommentsForComparedCommitData)

	_, err = client.PostCommentForComparedCommit(t.Context(), &codecommitsdk.PostCommentForComparedCommitInput{
		RepositoryName: aws.String("repo"),
		AfterCommitId:  aws.String("after"),
		Content:        aws.String("x"),
		Location: &types.Location{
			FilePath:            aws.String("a"),
			RelativeFileVersion: types.RelativeFileVersionEnum("MIDDLE"),
		},
	})
	require.ErrorContains(t, err, "InvalidRelativeFileVersionEnumException")
}

func TestGetCommentsForPullRequestFilters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		before string
		after  string
		repo   string
		want   int
	}{
		{name: "no_filter", want: 2},
		{name: "after_commit", after: "a1", want: 1},
		{name: "before_commit", before: "b2", want: 1},
		{name: "repo", repo: "repo", want: 2},
		{name: "no_match", after: "zzz", want: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			seedRepoWithCommit(t, client)

			prID, err := createPR(t, client, "pr", "")
			require.NoError(t, err)

			for _, c := range [][2]string{{"b1", "a1"}, {"b2", "a2"}} {
				_, err = client.PostCommentForPullRequest(t.Context(), &codecommitsdk.PostCommentForPullRequestInput{
					PullRequestId:  aws.String(prID),
					RepositoryName: aws.String("repo"),
					BeforeCommitId: aws.String(c[0]),
					AfterCommitId:  aws.String(c[1]),
					Content:        aws.String("c"),
				})
				require.NoError(t, err)
			}

			in := &codecommitsdk.GetCommentsForPullRequestInput{PullRequestId: aws.String(prID)}
			if tt.before != "" {
				in.BeforeCommitId = aws.String(tt.before)
			}

			if tt.after != "" {
				in.AfterCommitId = aws.String(tt.after)
			}

			if tt.repo != "" {
				in.RepositoryName = aws.String(tt.repo)
			}

			got, err := client.GetCommentsForPullRequest(t.Context(), in)
			require.NoError(t, err)

			total := 0
			for _, g := range got.CommentsForPullRequestData {
				total += len(g.Comments)
			}

			assert.Equal(t, tt.want, total)
		})
	}
}

func TestCreateCommitSetFileModes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		path    string
		mode    types.FileModeTypeEnum
		wantErr string
	}{
		{name: "existing_file", path: "a.txt", mode: types.FileModeTypeEnumExecutable},
		{
			name: "missing_file", path: "nope.txt", mode: types.FileModeTypeEnumExecutable,
			wantErr: "FileDoesNotExistException",
		},
		{
			name: "invalid_mode", path: "a.txt", mode: types.FileModeTypeEnum("BOGUS"),
			wantErr: "InvalidFileModeException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			client := newRealClient(t)
			tip := seedRepoWithCommit(t, client)

			out, err := client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
				RepositoryName: aws.String("repo"),
				BranchName:     aws.String("main"),
				ParentCommitId: aws.String(tip),
				SetFileModes:   []types.SetFileModeEntry{{FilePath: aws.String(tt.path), FileMode: tt.mode}},
			})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			require.Len(t, out.FilesUpdated, 1)
			assert.Equal(t, types.FileModeTypeEnumExecutable, out.FilesUpdated[0].FileMode)

			file, err := client.GetFile(t.Context(), &codecommitsdk.GetFileInput{
				RepositoryName:  aws.String("repo"),
				FilePath:        aws.String(tt.path),
				CommitSpecifier: aws.String("main"),
			})
			require.NoError(t, err)
			assert.Equal(t, types.FileModeTypeEnumExecutable, file.FileMode)
		})
	}
}

func TestCreateCommitReportsUpdatedFiles(t *testing.T) {
	t.Parallel()

	client := newRealClient(t)
	tip := seedRepoWithCommit(t, client)

	out, err := client.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		ParentCommitId: aws.String(tip),
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("a.txt"), FileContent: []byte("changed")},
			{FilePath: aws.String("b.txt"), FileContent: []byte("new")},
		},
	})
	require.NoError(t, err)
	require.Len(t, out.FilesAdded, 1)
	assert.Equal(t, "b.txt", aws.ToString(out.FilesAdded[0].AbsolutePath))
	require.Len(t, out.FilesUpdated, 1)
	assert.Equal(t, "a.txt", aws.ToString(out.FilesUpdated[0].AbsolutePath))
}

func TestGetCommentReactionsUserFilter(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	client := newTestCodeCommitClient(t, codecommit.NewHandler(backend))
	seedRepoWithCommit(t, client)

	commentID, err := postComparedComment(t, client, "c", "")
	require.NoError(t, err)
	require.NoError(t, backend.PutCommentReaction(commentID, "THUMBSUP", "arn:aws:iam::1:user/alice"))
	require.NoError(t, backend.PutCommentReaction(commentID, "HEART", "arn:aws:iam::1:user/bob"))

	got, err := client.GetCommentReactions(t.Context(), &codecommitsdk.GetCommentReactionsInput{
		CommentId:       aws.String(commentID),
		ReactionUserArn: aws.String("arn:aws:iam::1:user/bob"),
	})
	require.NoError(t, err)
	require.Len(t, got.ReactionsForComment, 1)
	assert.Equal(t, "HEART", aws.ToString(got.ReactionsForComment[0].Reaction.Emoji))
}
