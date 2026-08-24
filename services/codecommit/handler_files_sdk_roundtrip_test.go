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

// Test_SDKRoundTrip_CreateUnreferencedMergeCommit_CommitMetadata proves
// CreateUnreferencedMergeCommitInput's CommitMessage/AuthorName/Email reach
// the resulting commit, the same way PutFile's and DeleteFile's do.
// gopherstack's decode struct omitted all three, so the commit GetCommit
// returned always carried the hardcoded "Unreferenced merge commit" message
// and empty author/committer identity no matter what the client sent.
func Test_SDKRoundTrip_CreateUnreferencedMergeCommit_CommitMetadata(t *testing.T) {
	t.Parallel()

	h := codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	client := newTestCodeCommitClient(t, h)
	ctx := t.Context()

	const repoName = "unref-merge-metadata-repo"

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	putOut, err := client.PutFile(ctx, &codecommitsdk.PutFileInput{
		RepositoryName: aws.String(repoName),
		BranchName:     aws.String("main"),
		FilePath:       aws.String("hello.txt"),
		FileContent:    []byte("hello"),
	})
	require.NoError(t, err)

	mergeOut, err := client.CreateUnreferencedMergeCommit(ctx, &codecommitsdk.CreateUnreferencedMergeCommitInput{
		RepositoryName:             aws.String(repoName),
		SourceCommitSpecifier:      aws.String(aws.ToString(putOut.CommitId)),
		DestinationCommitSpecifier: aws.String(aws.ToString(putOut.CommitId)),
		MergeOption:                "FAST_FORWARD_MERGE",
		CommitMessage:              aws.String("unref merge result"),
		AuthorName:                 aws.String("Katherine Johnson"),
		Email:                      aws.String("katherine@example.com"),
	})
	require.NoError(t, err)
	require.NotNil(t, mergeOut.CommitId)

	getOut, err := client.GetCommit(ctx, &codecommitsdk.GetCommitInput{
		RepositoryName: aws.String(repoName),
		CommitId:       mergeOut.CommitId,
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.Commit)

	require.Equal(t, "unref merge result", aws.ToString(getOut.Commit.Message),
		"CreateUnreferencedMergeCommitInput.CommitMessage must reach the resulting commit, not a hardcoded default")
	require.Equal(t, "Katherine Johnson", aws.ToString(getOut.Commit.Author.Name),
		"CreateUnreferencedMergeCommitInput.AuthorName must populate the commit's author identity")
	require.Equal(t, "katherine@example.com", aws.ToString(getOut.Commit.Author.Email),
		"CreateUnreferencedMergeCommitInput.Email must populate the commit's author identity")
}

// Test_SDKRoundTrip_CreateRepository_KmsKeyId proves CreateRepositoryInput's
// KmsKeyId reaches the created repository's metadata: gopherstack's decode
// struct previously omitted it, so a repository created with a customer key
// always reported an empty kmsKeyId no matter what the client sent, even
// though Repository.KmsKeyID is a real tracked field (populated correctly by
// UpdateRepositoryEncryptionKey).
func Test_SDKRoundTrip_CreateRepository_KmsKeyId(t *testing.T) {
	t.Parallel()

	h := codecommit.NewHandler(codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion))
	client := newTestCodeCommitClient(t, h)
	ctx := t.Context()

	const repoName = "create-repo-kms-key"
	const kmsKeyID = "arn:aws:kms:us-east-1:123456789012:key/my-key"

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
		KmsKeyId:       aws.String(kmsKeyID),
	})
	require.NoError(t, err)

	getOut, err := client.GetRepository(ctx, &codecommitsdk.GetRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)
	require.NotNil(t, getOut.RepositoryMetadata)

	require.Equal(t, kmsKeyID, aws.ToString(getOut.RepositoryMetadata.KmsKeyId),
		"CreateRepositoryInput.KmsKeyId must reach the created repository's metadata")
}
