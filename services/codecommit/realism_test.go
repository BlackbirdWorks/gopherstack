package codecommit_test

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/aws/aws-sdk-go-v2/service/codecommit/types"
	"github.com/aws/smithy-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

func newRealismClient(t *testing.T) *codecommitsdk.Client {
	t.Helper()

	return newTestCodeCommitClient(t, codecommit.NewHandler(
		codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion),
	))
}

func seedRealismRepo(t *testing.T, c *codecommitsdk.Client) {
	t.Helper()

	_, err := c.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	_, err = c.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"),
		BranchName:     aws.String("main"),
		PutFiles: []types.PutFileEntry{
			{FilePath: aws.String("top.txt"), FileContent: []byte("t")},
			{FilePath: aws.String("dir/a.txt"), FileContent: []byte("a")},
			{FilePath: aws.String("dir/sub/b.txt"), FileContent: []byte("b")},
		},
	})
	require.NoError(t, err)
}

func TestRealism_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		call     func(t *testing.T, c *codecommitsdk.Client) error
		name     string
		wantCode string
		wantMsg  string
	}{
		{
			name: "repo not found",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.GetRepository(
					t.Context(),
					&codecommitsdk.GetRepositoryInput{RepositoryName: aws.String("nope")},
				)

				return err
			},
			wantCode: "RepositoryDoesNotExistException",
			wantMsg:  "nope does not exist",
		},
		{
			name: "git suffix",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.CreateRepository(
					t.Context(),
					&codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("x.git")},
				)

				return err
			},
			wantCode: "InvalidRepositoryNameException",
			wantMsg:  ".git",
		},
		{
			name: "long description",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{
					RepositoryName: aws.String(
						"long-desc",
					), RepositoryDescription: aws.String(strings.Repeat("a", 1001)),
				})

				return err
			},
			wantCode: "InvalidRepositoryDescriptionException",
			wantMsg:  "1000",
		},
		{
			name: "bad sort by",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.ListRepositories(t.Context(), &codecommitsdk.ListRepositoriesInput{SortBy: "bogus"})

				return err
			},
			wantCode: "InvalidSortByException",
		},
		{
			name: "bad order",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.ListRepositories(t.Context(), &codecommitsdk.ListRepositoriesInput{Order: "bogus"})

				return err
			},
			wantCode: "InvalidOrderException",
		},
		{
			name: "missing folder",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.GetFolder(t.Context(), &codecommitsdk.GetFolderInput{
					RepositoryName: aws.String("repo"), FolderPath: aws.String("/ghost"),
				})

				return err
			},
			wantCode: "FolderDoesNotExistException",
		},
		{
			name: "pr missing source",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
					Title:   aws.String("t"),
					Targets: []types.Target{{RepositoryName: aws.String("repo"), SourceReference: aws.String("ghost")}},
				})

				return err
			},
			wantCode: "ReferenceDoesNotExistException",
		},
		{
			name: "pr same refs",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
					Title:   aws.String("t"),
					Targets: []types.Target{{RepositoryName: aws.String("repo"), SourceReference: aws.String("main")}},
				})

				return err
			},
			wantCode: "SourceAndDestinationAreSameException",
		},
		{
			name: "pr missing repo",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
					Title:   aws.String("t"),
					Targets: []types.Target{{RepositoryName: aws.String("ghost"), SourceReference: aws.String("main")}},
				})

				return err
			},
			wantCode: "RepositoryDoesNotExistException",
		},
		{
			name: "pr long title",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.CreatePullRequest(t.Context(), &codecommitsdk.CreatePullRequestInput{
					Title:   aws.String(strings.Repeat("t", 101)),
					Targets: []types.Target{{RepositoryName: aws.String("repo"), SourceReference: aws.String("main")}},
				})

				return err
			},
			wantCode: "InvalidTitleException",
		},
		{
			name: "pr bad id",
			call: func(t *testing.T, c *codecommitsdk.Client) error {
				t.Helper()
				_, err := c.GetPullRequest(
					t.Context(),
					&codecommitsdk.GetPullRequestInput{PullRequestId: aws.String("abc")},
				)

				return err
			},
			wantCode: "InvalidPullRequestIdException",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealismClient(t)
			seedRealismRepo(t, c)

			var apiErr smithy.APIError
			require.ErrorAs(t, tt.call(t, c), &apiErr)
			assert.Equal(t, tt.wantCode, apiErr.ErrorCode())
			assert.NotContains(t, apiErr.ErrorMessage(), "Exception")
			assert.Contains(t, apiErr.ErrorMessage(), tt.wantMsg)
		})
	}
}

func TestRealism_FolderListing(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		folder      string
		wantFiles   []string
		wantFolders []string
	}{
		{name: "root", folder: "/", wantFiles: []string{"top.txt"}, wantFolders: []string{"dir"}},
		{name: "nested", folder: "dir", wantFiles: []string{"dir/a.txt"}, wantFolders: []string{"dir/sub"}},
		{name: "leaf", folder: "/dir/sub", wantFiles: []string{"dir/sub/b.txt"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newRealismClient(t)
			seedRealismRepo(t, c)

			out, err := c.GetFolder(t.Context(), &codecommitsdk.GetFolderInput{
				RepositoryName: aws.String("repo"), FolderPath: aws.String(tt.folder),
			})
			require.NoError(t, err)
			assert.Regexp(t, `^[0-9a-f]{40}$`, aws.ToString(out.CommitId))

			var files, folders []string
			for _, f := range out.Files {
				files = append(files, aws.ToString(f.AbsolutePath))
			}

			for _, f := range out.SubFolders {
				folders = append(folders, aws.ToString(f.AbsolutePath))
			}

			assert.Equal(t, tt.wantFiles, files)
			assert.Equal(t, tt.wantFolders, folders)
		})
	}
}

func TestRealism_DefaultBranchAndIDs(t *testing.T) {
	t.Parallel()

	c := newRealismClient(t)

	_, err := c.CreateRepository(t.Context(), &codecommitsdk.CreateRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)

	got, err := c.GetRepository(t.Context(), &codecommitsdk.GetRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)
	assert.Empty(t, aws.ToString(got.RepositoryMetadata.DefaultBranch))

	_, err = c.CreateCommit(t.Context(), &codecommitsdk.CreateCommitInput{
		RepositoryName: aws.String("repo"), BranchName: aws.String("trunk"),
		PutFiles: []types.PutFileEntry{{FilePath: aws.String("a"), FileContent: []byte("a")}},
	})
	require.NoError(t, err)

	got, err = c.GetRepository(t.Context(), &codecommitsdk.GetRepositoryInput{RepositoryName: aws.String("repo")})
	require.NoError(t, err)
	assert.Equal(t, "trunk", aws.ToString(got.RepositoryMetadata.DefaultBranch))

	br, err := c.GetBranch(t.Context(), &codecommitsdk.GetBranchInput{
		RepositoryName: aws.String("repo"), BranchName: aws.String("trunk"),
	})
	require.NoError(t, err)
	assert.Regexp(t, `^[0-9a-f]{40}$`, aws.ToString(br.Branch.CommitId))
}
