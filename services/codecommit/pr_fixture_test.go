package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

func seedFeatureBranch(t *testing.T, c *codecommitsdk.Client, repo string) {
	t.Helper()

	br, err := c.GetBranch(t.Context(), &codecommitsdk.GetBranchInput{
		RepositoryName: aws.String(repo), BranchName: aws.String("main"),
	})
	require.NoError(t, err)

	_, err = c.CreateBranch(t.Context(), &codecommitsdk.CreateBranchInput{
		RepositoryName: aws.String(repo), BranchName: aws.String("feature"), CommitId: br.Branch.CommitId,
	})
	require.NoError(t, err)
}

func seedBackendFeatureBranch(t *testing.T, b *codecommit.InMemoryBackend, repo string) {
	t.Helper()

	_, _, _, err := b.CreateCommit(repo, "main", "a", "a@example.com", "init", "", nil, nil, false)
	require.NoError(t, err)

	br, err := b.GetBranch(repo, "main")
	require.NoError(t, err)
	require.NoError(t, b.CreateBranch(repo, "feature", br.CommitID))
}
