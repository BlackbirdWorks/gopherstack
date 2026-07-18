package detective_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

// TestEnableOrganizationAdminAccount_CreatesGraphWhenMissing verifies AWS's
// documented behavior: "If the account does not have Detective enabled, then
// enables Detective for that account and creates a new behavior graph."
// Leaving GraphArn empty on the returned OrgAdmin would leave organization
// admin accounts permanently unable to discover their graph via
// ListOrganizationAdminAccounts.
func TestEnableOrganizationAdminAccount_CreatesGraphWhenMissing(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("000000000000", "us-east-1")

	require.NoError(t, b.EnableOrganizationAdminAccount("555566667777"))

	admins, _, err := b.ListOrganizationAdminAccounts(0, "")
	require.NoError(t, err)
	require.Len(t, admins, 1)
	assert.NotEmpty(t, admins[0].GraphARN, "EnableOrganizationAdminAccount must auto-create a behavior graph")

	graphs, _, err := b.ListGraphs(0, "")
	require.NoError(t, err)
	require.Len(t, graphs, 1)
	assert.Equal(t, graphs[0].Arn, admins[0].GraphARN)
}

// TestEnableOrganizationAdminAccount_ReusesExistingGraph verifies the account
// already having a behavior graph is reused, not replaced.
func TestEnableOrganizationAdminAccount_ReusesExistingGraph(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("000000000000", "us-east-1")

	g, err := b.CreateGraph(nil)
	require.NoError(t, err)

	require.NoError(t, b.EnableOrganizationAdminAccount("555566667777"))

	admins, _, err := b.ListOrganizationAdminAccounts(0, "")
	require.NoError(t, err)
	require.Len(t, admins, 1)
	assert.Equal(t, g.Arn, admins[0].GraphARN)

	graphs, _, err := b.ListGraphs(0, "")
	require.NoError(t, err)
	require.Len(t, graphs, 1, "must not create a second graph when one already exists")
}
