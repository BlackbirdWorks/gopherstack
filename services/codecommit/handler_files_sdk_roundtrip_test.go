package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// Test_SDKRoundTrip_PutFile_CommitMetadata proves PutFileInput's
// CommitMessage/Name/Email/FileMode reach the resulting commit: gopherstack's
// decode struct previously omitted all four (plus ParentCommitId), so the
// commit GetCommit returned always carried a synthetic "Add <path>" message
// and empty author/committer identity no matter what the client sent.
func Test_SDKRoundTrip_PutFile_CommitMetadata(t *testing.T) {
	t.Parallel()

	h := codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	client := newTestCodeCommitClient(t, h)
	ctx := t.Context()

	const repoName = "putfile-metadata-repo"

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	putOut, err := client.PutFile(ctx, &codecommitsdk.PutFileInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("main"),
		FilePath:       aws.String("hello.txt"),
		FileContent:    []byte("hello"),
		CommitMessage:  aws.String("initial import"),
		Name:           aws.String("Ada Lovelace"),
		Email:          aws.String("ada@example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, putOut.CommitId)

	getOut, err := client.GetCommit(ctx, &codecommitsdk.GetCommitInput{
		RepositoryName: aws.String(repoName),
		CommitId:       putOut.CommitId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Commit)

	require.Equal(t, "initial import", aws.ToString(getOut.Commit.Message),
		"PutFileInput.CommitMessage must reach the resulting commit, not a synthetic default")
	require.Equal(t, "Ada Lovelace", aws.ToString(getOut.Commit.Author.Name),
		"PutFileInput.Name must populate the commit's author identity")
	require.Equal(t, "ada@example.com", aws.ToString(getOut.Commit.Author.Email),
		"PutFileInput.Email must populate the commit's author identity")
}

// Test_SDKRoundTrip_DeleteFile_CommitMetadata proves DeleteFileInput's
// CommitMessage/Name/Email reach the resulting delete commit the same way
// PutFile's do.
func Test_SDKRoundTrip_DeleteFile_CommitMetadata(t *testing.T) {
	t.Parallel()

	h := codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	client := newTestCodeCommitClient(t, h)
	ctx := t.Context()

	const repoName = "deletefile-metadata-repo"

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	putOut, err := client.PutFile(ctx, &codecommitsdk.PutFileInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("main"),
		FilePath:       aws.String("todelete.txt"),
		FileContent:    []byte("bye"),
	})
	require.NoError(t, err)

	delOut, err := client.DeleteFile(ctx, &codecommitsdk.DeleteFileInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("main"),
		FilePath:       aws.String("todelete.txt"),
		ParentCommitId: putOut.CommitId,
		CommitMessage:  aws.String("cleanup"),
		Name:           aws.String("Grace Hopper"),
		Email:          aws.String("grace@example.com"),
	})
	require.NoError(t, err)

	getOut, err := client.GetCommit(ctx, &codecommitsdk.GetCommitInput{
		RepositoryName: aws.String(repoName),
		CommitId:       delOut.CommitId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Commit)

	require.Equal(t, "cleanup", aws.ToString(getOut.Commit.Message),
		"DeleteFileInput.CommitMessage must reach the resulting commit, not a synthetic default")
	require.Equal(t, "Grace Hopper", aws.ToString(getOut.Commit.Author.Name),
		"DeleteFileInput.Name must populate the commit's author identity")
	require.Equal(t, "grace@example.com", aws.ToString(getOut.Commit.Author.Email),
		"DeleteFileInput.Email must populate the commit's author identity")
}
