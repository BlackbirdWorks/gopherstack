package codecommit_test

import (
	"testing"
	"time"

	codecommitsdk "github.com/aws/aws-sdk-go-v2/service/codecommit"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/config"
	"github.com/blackbirdworks/gopherstack/services/codecommit"
)

// TestDescribePullRequestEvents_EventDateDecodesAsEpoch drives
// DescribePullRequestEvents through the real aws-sdk-go-v2 codecommit
// client. PullRequestEvent.EventDate deserializes from a json.Number via
// ParseEpochSeconds (deserializers.go, case "eventDate"); before this fix,
// gopherstack emitted an RFC3339 string there, which fails every real
// client's decode with "expected EventDate to be a JSON Number, got string
// instead" the moment any pull request event exists.
func TestDescribePullRequestEvents_EventDateDecodesAsEpoch(t *testing.T) {
	t.Parallel()

	b := codecommit.NewInMemoryBackend(config.DefaultAccountID, config.DefaultRegion)
	h := codecommit.NewHandler(b)
	client := newTestCodeCommitClient(t, h)

	pr, err := b.CreatePullRequest("Test PR", "", "", []codecommit.PullRequestTarget{
		{RepositoryName: "repo", SourceReference: "refs/heads/feature"},
	})
	require.NoError(t, err)

	require.NoError(
		t,
		b.OverridePullRequestApprovalRules(
			pr.PullRequestID,
			"OVERRIDE",
			"arn:aws:iam::123456789012:user/tester",
		),
	)

	out, err := client.DescribePullRequestEvents(
		t.Context(),
		&codecommitsdk.DescribePullRequestEventsInput{
			PullRequestId: &pr.PullRequestID,
		},
	)
	require.NoError(t, err, "real SDK client must decode DescribePullRequestEvents without error")
	require.Len(t, out.PullRequestEvents, 1)
	assert.Equal(
		t,
		"PULL_REQUEST_APPROVAL_RULE_OVERRIDDEN",
		string(out.PullRequestEvents[0].PullRequestEventType),
	)
	require.NotNil(t, out.PullRequestEvents[0].EventDate)
	assert.WithinDuration(t, time.Now(), *out.PullRequestEvents[0].EventDate, 10*time.Second)
}
