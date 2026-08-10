package detective_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/detective"
)

// TestInMemoryBackend_RestoreInvalidData verifies that malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("111111111111", "us-east-1")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend is discarded cleanly rather than
// partially decoded: the backend resets to empty state (every store.Table on
// the registry, plus the raw tags/datasources/orgConfigs/orgAdmins fields)
// and Restore returns no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("111111111111", "us-east-1")
	ctx := t.Context()

	g, err := b.CreateGraph(map[string]string{"env": "test"})
	require.NoError(t, err)

	// A syntactically valid but version-mismatched snapshot.
	err = b.Restore(ctx, []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	graphs, _, err := b.ListGraphs(0, "")
	require.NoError(t, err)
	assert.Empty(t, graphs)

	_, _, err = b.GetMembers(g.Arn, []string{"222233334444"})
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero verifies that a
// snapshot with no version field at all decodes with Version == 0, which
// mismatches detectiveSnapshotVersion and is discarded the same way any other
// incompatible version is -- not partially applied. This also covers the
// pre-Phase-3.3 on-disk shape (one JSON field per resource map, no
// "version"/"tables" keys at all).
func TestInMemoryBackend_RestoreOldSnapshotDecodesAsZero(t *testing.T) {
	t.Parallel()

	b := detective.NewInMemoryBackend("111111111111", "us-east-1")
	ctx := t.Context()

	_, err := b.CreateGraph(nil)
	require.NoError(t, err)

	oldShape := `{"graphs":{"arn:aws:detective:us-east-1:111111111111:graph:old":{}}}`
	err = b.Restore(ctx, []byte(oldShape))
	require.NoError(t, err)

	graphs, _, err := b.ListGraphs(0, "")
	require.NoError(t, err)
	assert.Empty(t, graphs)
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every store.Table-backed resource family the Phase 3.3
// conversion touched (graphs, members, investigations -- all "clean" tables,
// see store_setup.go), plus the raw maps/slice left unconverted (tags,
// datasources, orgConfigs, orgAdmins). It also exercises the byGraph
// secondary indexes (via DeleteGraph's member cascade after restore),
// confirming they are correctly rebuilt by Restore rather than left stale.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := detective.NewInMemoryBackend("111111111111", "us-east-1")
	ctx := t.Context()

	g, err := original.CreateGraph(map[string]string{"env": "prod"})
	require.NoError(t, err)

	members, unprocessed, err := original.CreateMembers(g.Arn, []detective.Account{
		{AccountID: "222233334444", EmailAddress: "member@example.com"},
	}, "")
	require.NoError(t, err)
	require.Empty(t, unprocessed)
	require.Len(t, members, 1)

	require.NoError(t, original.TagResource(g.Arn, map[string]string{"extra": "tag"}))

	investigationID, err := original.StartInvestigation(
		g.Arn, "arn:aws:iam::111111111111:role/example",
		time.Now().Add(-time.Hour).UTC(), time.Now().UTC(),
	)
	require.NoError(t, err)
	require.NoError(t, original.UpdateInvestigationState(g.Arn, investigationID, "ARCHIVED"))

	require.NoError(t, original.UpdateDatasourcePackages(g.Arn, []string{"ASFF_SECURITYHUB_FINDING"}))

	require.NoError(t, original.EnableOrganizationAdminAccount("555566667777"))
	require.NoError(t, original.UpdateOrganizationConfiguration(g.Arn, true))

	data := original.Snapshot(ctx)
	require.NotNil(t, data)

	restored := detective.NewInMemoryBackend("000000000000", "")
	require.NoError(t, restored.Restore(ctx, data))

	// graphs ("clean").
	graphs, _, err := restored.ListGraphs(0, "")
	require.NoError(t, err)
	require.Len(t, graphs, 1)
	assert.Equal(t, g.Arn, graphs[0].Arn)

	// members ("clean", graph-composite).
	gotMembers, _, err := restored.ListMembers(g.Arn, 0, "")
	require.NoError(t, err)
	require.Len(t, gotMembers, 1)
	assert.Equal(t, "222233334444", gotMembers[0].AccountID)
	assert.Equal(t, "member@example.com", gotMembers[0].EmailAddress)

	// investigations ("clean", graph-composite).
	gotInv, err := restored.GetInvestigation(g.Arn, investigationID)
	require.NoError(t, err)
	assert.Equal(t, "ARCHIVED", gotInv.State)
	assert.Equal(t, "IAM_ROLE", gotInv.EntityType)

	// tags (raw, unconverted map).
	gotTags, err := restored.ListTagsForResource(g.Arn)
	require.NoError(t, err)
	assert.Equal(t, "tag", gotTags["extra"])
	assert.Equal(t, "prod", gotTags["env"])

	// datasources (raw, unconverted nested map).
	pkgs, _, err := restored.ListDatasourcePackages(g.Arn, 0, "")
	require.NoError(t, err)
	require.Contains(t, pkgs, "ASFF_SECURITYHUB_FINDING")
	assert.Equal(t, "STARTED", pkgs["ASFF_SECURITYHUB_FINDING"].IngestState)

	// orgAdmins (raw, order-sensitive slice).
	admins, _, err := restored.ListOrganizationAdminAccounts(0, "")
	require.NoError(t, err)
	require.Len(t, admins, 1)
	assert.Equal(t, "555566667777", admins[0].AccountID)

	// orgConfigs (raw, unconverted map).
	autoEnable, err := restored.DescribeOrganizationConfiguration(g.Arn)
	require.NoError(t, err)
	assert.True(t, autoEnable)

	// DeleteGraph cascade-deletes every graph-nested member (exercising the
	// byGraph index rebuilt by Restore); if the index were left stale, the
	// member would still be findable via ListMembers/GetMembers after the
	// cascade instead of returning ErrGraphNotFound.
	require.NoError(t, restored.DeleteGraph(g.Arn))
	_, _, err = restored.ListMembers(g.Arn, 0, "")
	require.Error(t, err)
}

// TestInMemoryBackend_SnapshotRestore_DatasourceIngestHistory verifies the
// per-package ingest-state-change timestamp added this pass (gopherstack-c902,
// backing MembershipDatasources.IngestHistory) survives a snapshot/restore
// round trip instead of resetting to the zero time.
func TestInMemoryBackend_SnapshotRestore_DatasourceIngestHistory(t *testing.T) {
	t.Parallel()

	original := detective.NewInMemoryBackend("111111111111", "us-east-1")
	ctx := t.Context()

	g, err := original.CreateGraph(nil)
	require.NoError(t, err)

	_, unprocessed, err := original.CreateMembers(g.Arn, []detective.Account{
		{AccountID: "222233334444", EmailAddress: "member@example.com"},
	}, "")
	require.NoError(t, err)
	require.Empty(t, unprocessed)

	require.NoError(t, original.UpdateDatasourcePackages(g.Arn, []string{"DETECTIVE_CORE"}))

	before, _, err := original.BatchGetGraphMemberDatasources(g.Arn, []string{"222233334444"})
	require.NoError(t, err)
	require.Len(t, before, 1)
	beforeTime := before[0].IngestHistory["DETECTIVE_CORE"]["STARTED"]
	require.False(t, beforeTime.IsZero())

	data := original.Snapshot(ctx)
	require.NotNil(t, data)

	restored := detective.NewInMemoryBackend("000000000000", "")
	require.NoError(t, restored.Restore(ctx, data))

	after, _, err := restored.BatchGetGraphMemberDatasources(g.Arn, []string{"222233334444"})
	require.NoError(t, err)
	require.Len(t, after, 1)
	afterTime := after[0].IngestHistory["DETECTIVE_CORE"]["STARTED"]
	assert.True(t, beforeTime.Equal(afterTime),
		"ingest-state-change timestamp must survive snapshot/restore unchanged")
}
