package detective_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

// TestCreateMembers_AlreadyInvited_ReturnsUnprocessed verifies that re-inviting
// an account that is already a member of the behavior graph is reported back
// via UnprocessedAccounts, matching the AWS-documented CreateMembers contract:
// "The accounts that CreateMembers was unable to process. This list includes
// accounts that were already invited to be member accounts in the behavior
// graph." A member silently re-appearing in the processed Members list would
// hide this AWS-observable outcome from real SDK clients.
func TestCreateMembers_AlreadyInvited_ReturnsUnprocessed(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("000000000000", "us-east-1")

	g, err := b.CreateGraph(nil)
	require.NoError(t, err)

	accounts := []detective.Account{{AccountID: "222233334444", EmailAddress: "member@example.com"}}

	members, unprocessed, err := b.CreateMembers(g.Arn, accounts, "")
	require.NoError(t, err)
	require.Len(t, members, 1)
	require.Empty(t, unprocessed)

	// Re-invite the same account.
	members2, unprocessed2, err := b.CreateMembers(g.Arn, accounts, "")
	require.NoError(t, err)
	assert.Empty(t, members2, "already-invited account must not be reported as newly processed")
	require.Len(t, unprocessed2, 1)
	assert.Equal(t, "222233334444", unprocessed2[0].AccountID)
	assert.NotEmpty(t, unprocessed2[0].Reason)
}
