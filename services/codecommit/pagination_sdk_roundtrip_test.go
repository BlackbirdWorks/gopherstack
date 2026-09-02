package codecommit_test

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// TestListPullRequests_SDKRoundTrip_BoundaryWalk drives ListPullRequests
// through the real aws-sdk-go-v2 codecommit client (the shared paginateStrings
// helper, verified for pure arithmetic in pagination_arithmetic_internal_test.go,
// applied here on the real wire's MaxResults/NextToken -- unlike
// ListRepositories/ListBranches, whose real Input structs carry no
// MaxResults at all, so their maxResults handling is only reachable through
// gopherstack's own internal-only JSON field, not a real SDK client).
// Confirms the helper's page boundaries reproduce the full id set with no
// drops or duplicates when driven end-to-end through the typed client.
func TestListPullRequests_SDKRoundTrip_BoundaryWalk(t *testing.T) {
	t.Parallel()

	backend := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	h := codecommit.NewHandler(backend)
	client := newTestCodeCommitClient(t, h)
	ctx := t.Context()

	const repoName = "pr-pagination-repo"

	_, err := client.CreateRepository(ctx, &codecommitsdk.CreateRepositoryInput{
		RepositoryName: aws.String(repoName),
	})
	require.NoError(t, err)

	wantIDs := make(map[string]bool, 9)

	for i := range 9 {
		pr, createErr := backend.CreatePullRequest("title", "desc", "", []codecommit.PullRequestTarget{
			{RepositoryName: repoName, SourceReference: "refs/heads/feature", DestinationReference: "refs/heads/main"},
		})
		require.NoError(t, createErr)
		require.NotNil(t, pr)
		wantIDs[pr.PullRequestID] = true

		_ = i
	}

	collected := make(map[string]bool, 9)

	var nextToken *string

	for {
		out, listErr := client.ListPullRequests(ctx, &codecommitsdk.ListPullRequestsInput{
			RepositoryName: aws.String(repoName),
			MaxResults:     aws.Int32(4),
			NextToken:      nextToken,
		})
		require.NoError(t, listErr)

		for _, id := range out.PullRequestIds {
			require.False(t, collected[id], "duplicate id %q returned across pages", id)
			collected[id] = true
		}

		if out.NextToken == nil || aws.ToString(out.NextToken) == "" {
			break
		}

		nextToken = out.NextToken
	}

	require.Equal(t, wantIDs, collected)
}
