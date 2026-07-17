package detective_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

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
