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

// TestEnableOrganizationAdminAccount_ReplacesPriorAdmin verifies AWS's
// singular Detective-administrator-account model: ListOrganizationAdminAccounts
// and the underlying Administrator SDK type both describe "the" administrator
// account for an organization/Region, not a collection. Calling
// EnableOrganizationAdminAccount a second time (even for a different account)
// must replace the existing designation, not append a duplicate entry.
func TestEnableOrganizationAdminAccount_ReplacesPriorAdmin(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("000000000000", "us-east-1")

	require.NoError(t, b.EnableOrganizationAdminAccount("111111111111"))
	require.NoError(t, b.EnableOrganizationAdminAccount("222222222222"))

	admins, _, err := b.ListOrganizationAdminAccounts(0, "")
	require.NoError(t, err)
	require.Len(t, admins, 1, "a second Enable call must replace, not accumulate, the administrator account")
	assert.Equal(t, "222222222222", admins[0].AccountID)
}

// TestDisableOrganizationAdminAccount_DeletesGraph verifies AWS's documented
// behavior: "Removes the Detective administrator account in the current
// Region. Deletes the organization behavior graph." A prior pass left this
// unimplemented because the emulator's single-graph model does not
// distinguish an org graph from a personal one; since EnableOrganizationAdminAccount
// always designates the account's one graph as the org graph, deleting it on
// Disable is the faithful behavior within that model.
func TestDisableOrganizationAdminAccount_DeletesGraph(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("000000000000", "us-east-1")

	require.NoError(t, b.EnableOrganizationAdminAccount("555566667777"))

	admins, _, err := b.ListOrganizationAdminAccounts(0, "")
	require.NoError(t, err)
	require.Len(t, admins, 1)
	graphARN := admins[0].GraphARN
	require.NotEmpty(t, graphARN)

	require.NoError(t, b.DisableOrganizationAdminAccount())

	admins, _, err = b.ListOrganizationAdminAccounts(0, "")
	require.NoError(t, err)
	assert.Empty(t, admins)

	graphs, _, err := b.ListGraphs(0, "")
	require.NoError(t, err)
	assert.Empty(t, graphs, "DisableOrganizationAdminAccount must delete the organization behavior graph")

	_, _, getErr := b.GetMembers(graphARN, []string{"111111111111"})
	assert.ErrorIs(t, getErr, detective.ErrGraphNotFound, "the deleted org graph must 404 on subsequent access")
}
