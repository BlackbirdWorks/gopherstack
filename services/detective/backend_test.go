package detective_test

import (
	"testing"
	"time"

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

// TestStartInvestigation_DerivesEntityType verifies EntityType is derived
// server-side from the EntityArn resource segment. The real StartInvestigation
// request shape has no EntityType input member at all -- Detective infers it
// from whether the ARN names an IAM role or an IAM user -- so a real SDK
// caller never sends one, and the emulator must not depend on client input to
// populate it.
func TestStartInvestigation_DerivesEntityType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		entityARN  string
		wantType   string
		wantErrLen bool
	}{
		{
			name:      "IAM role ARN",
			entityARN: "arn:aws:iam::123456789012:role/example-role",
			wantType:  "IAM_ROLE",
		},
		{
			name:      "IAM user ARN",
			entityARN: "arn:aws:iam::123456789012:user/example-user",
			wantType:  "IAM_USER",
		},
		{
			name:      "IAM role ARN with path",
			entityARN: "arn:aws:iam::123456789012:role/path/to/example-role",
			wantType:  "IAM_ROLE",
		},
		{
			name:       "non-IAM ARN rejected",
			entityARN:  "arn:aws:s3:::somebucket",
			wantErrLen: true,
		},
		{
			name:       "IAM group ARN rejected",
			entityARN:  "arn:aws:iam::123456789012:group/example-group",
			wantErrLen: true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b := detective.NewInMemoryBackend("000000000000", "us-east-1")

			g, err := b.CreateGraph(nil)
			require.NoError(t, err)

			id, startErr := b.StartInvestigation(
				g.Arn, tc.entityARN, time.Now().Add(-time.Hour).UTC(), time.Now().UTC(),
			)
			if tc.wantErrLen {
				require.Error(t, startErr)

				return
			}

			require.NoError(t, startErr)

			inv, getErr := b.GetInvestigation(g.Arn, id)
			require.NoError(t, getErr)
			assert.Equal(t, tc.wantType, inv.EntityType)
		})
	}
}

// TestDeleteGraph_CleansUpDependentState verifies DeleteGraph removes
// investigations, datasource package state, and org-config state scoped to
// the deleted graph, not just members and tags. Orphaned entries keyed by a
// deleted graph's ARN would never be observable again through the API (a
// fresh CreateGraph always mints a new ARN) but would leak memory forever.
func TestDeleteGraph_CleansUpDependentState(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("000000000000", "us-east-1")
	ctx := t.Context()

	g, err := b.CreateGraph(nil)
	require.NoError(t, err)

	_, startErr := b.StartInvestigation(
		g.Arn, "arn:aws:iam::123456789012:role/example",
		time.Now().Add(-time.Hour).UTC(), time.Now().UTC(),
	)
	require.NoError(t, startErr)

	require.NoError(t, b.UpdateDatasourcePackages(g.Arn, []string{"ASFF_SECURITYHUB_FINDING"}))
	require.NoError(t, b.UpdateOrganizationConfiguration(g.Arn, true))

	before := b.Snapshot(ctx)
	require.Contains(t, string(before), g.Arn)

	require.NoError(t, b.DeleteGraph(g.Arn))

	_, _, listErr := b.ListInvestigations(g.Arn, 0, "")
	require.ErrorIs(t, listErr, detective.ErrGraphNotFound)

	after := b.Snapshot(ctx)
	assert.NotContains(t, string(after), g.Arn,
		"datasources/orgConfigs/investigations must not retain the deleted graph's ARN")
}
