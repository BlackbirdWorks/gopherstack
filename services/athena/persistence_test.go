package athena_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/athena"
)

// Test_InMemoryBackend_SnapshotRestore exercises a full Snapshot->Restore
// round trip across every resource kind the backend supports: every
// store.Table-backed "clean" collection registered in store_setup.go, the
// two "dirty" collections (databases, tables) persistence.go round-trips
// through an ephemeral DTO registry, and the three plain-map fields
// (queryResults, tableData, resourceTags) persistence.go handles explicitly.
//
// This is a single sequential lifecycle test (populate -> snapshot -> restore
// -> verify), not a table of independent cases, so it does not use t.Run --
// see .claude/memories/parity-principles.md's test-isolation note: a
// sequential lifecycle asserted end-to-end in one function is the documented
// exception to the table-test convention.
func Test_InMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	const catalog = "AwsDataCatalog"

	ctx := t.Context()
	b := athena.NewInMemoryBackend("", "")
	fx := athena.PopulateEveryTable(t, b)

	data := b.Snapshot(ctx)
	require.NotNil(t, data)

	restored := athena.NewInMemoryBackend("", "")
	require.NoError(t, restored.Restore(ctx, data))

	// --- "clean" store.Table-backed collections ---

	wgs, _, err := restored.ListWorkGroups("", 0)
	require.NoError(t, err)

	var haveWG1 bool

	for _, wg := range wgs {
		if wg.Name == fx.WorkGroup {
			haveWG1 = true
		}
	}

	assert.True(t, haveWG1, "wg1 workgroup must survive restore")

	nq, err := restored.GetNamedQuery(fx.NamedQueryID)
	require.NoError(t, err)
	assert.Equal(t, "nq1", nq.Name)

	dc, err := restored.GetDataCatalog(fx.DataCatalog)
	require.NoError(t, err)
	assert.Equal(t, "GLUE", dc.Type)

	qe, err := restored.GetQueryExecution(fx.SelectExecID)
	require.NoError(t, err)
	assert.Equal(t, "SELECT * FROM mydb.mytable", qe.Query)

	ps, err := restored.GetPreparedStatement(fx.PreparedStatementName, fx.WorkGroup)
	require.NoError(t, err)
	assert.Equal(t, "SELECT 1", ps.QueryStatement)

	cr, err := restored.GetCapacityReservation(fx.CapacityReservation)
	require.NoError(t, err)
	assert.Equal(t, int32(24), cr.TargetDpus)

	nb, err := restored.GetNotebookMetadata(fx.NotebookID)
	require.NoError(t, err)
	assert.Equal(t, "nb1", nb.Name)

	sess, err := restored.GetSession(fx.SessionID)
	require.NoError(t, err)
	assert.Equal(t, fx.WorkGroup, sess.WorkGroup)

	calc, err := restored.GetCalculationExecution(fx.CalculationID)
	require.NoError(t, err)
	assert.Equal(t, "print(1)", calc.CodeBlock)

	cac, err := restored.GetCapacityAssignmentConfiguration(fx.CapacityReservation)
	require.NoError(t, err)
	require.Len(t, cac.CapacityAssignments, 1)
	assert.Equal(t, []string{fx.WorkGroup}, cac.CapacityAssignments[0].WorkGroupNames)

	// --- "dirty" collections (databases, tables) ---

	db, err := restored.GetDatabase(catalog, fx.Database)
	require.NoError(t, err)
	assert.Equal(t, fx.Database, db.Name)

	tm, err := restored.GetTableMetadata(catalog, fx.Database, fx.Table)
	require.NoError(t, err)
	assert.Equal(t, fx.Table, tm.Name)

	// The default AwsDataCatalog/default/sample_table NewInMemoryBackend seeds
	// must also survive, proving the seeded rows aren't special-cased.
	assert.True(t, restored.HasDatabase(catalog, "default"))
	assert.True(t, restored.HasTable(catalog, "default", "sample_table"))

	// --- plain-map fields: queryResults, tableData, resourceTags ---

	page, err := restored.GetQueryResults(fx.SelectExecID, "", 0)
	require.NoError(t, err)
	require.Len(t, page.Rows, 1, "queryResults must survive restore")
	// Every successful query execution stores a (possibly empty) result set --
	// see finalizeExecution -- so the count covers the two DDL statements
	// (CREATE DATABASE, CREATE TABLE) plus the SELECT, not just the SELECT.
	assert.Equal(t, 3, restored.QueryResultCount())

	assert.Equal(t, 1, restored.TableRowCount(catalog, fx.Database, fx.Table),
		"tableData must survive restore")

	tags, _, err := restored.ListTagsForResource(fx.WorkGroupARN, "", 0)
	require.NoError(t, err)
	require.Len(t, tags, 1)
	assert.Equal(t, "env", tags[0].Key)
	assert.Equal(t, "test", tags[0].Value)
}

// Test_InMemoryBackend_Restore_IncompatibleVersion verifies that a snapshot
// whose version doesn't match the current backend is discarded cleanly
// rather than partially decoded: the backend resets to exactly the state a
// fresh NewInMemoryBackend would have (dropping every populated resource,
// but re-seeding the default "primary" workgroup and AwsDataCatalog/default
// metadata), and Restore returns no error.
func Test_InMemoryBackend_Restore_IncompatibleVersion(t *testing.T) {
	t.Parallel()

	const catalog = "AwsDataCatalog"

	tests := []struct {
		name string
		data []byte
	}{
		{
			name: "future version",
			data: []byte(`{"version":999,"tables":{}}`),
		},
		{
			name: "no version field (pre-persistence.go shape decodes as zero)",
			data: []byte(`{"tables":{}}`),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ctx := t.Context()
			b := athena.NewInMemoryBackend("", "")
			fx := athena.PopulateEveryTable(t, b)

			err := b.Restore(ctx, tt.data)
			require.NoError(t, err)

			_, err = b.GetNamedQuery(fx.NamedQueryID)
			require.Error(t, err, "populated named query must be dropped")

			wgs, _, err := b.ListWorkGroups("", 0)
			require.NoError(t, err)
			require.Len(t, wgs, 1, "only the default primary workgroup must remain")
			assert.Equal(t, "primary", wgs[0].Name)

			assert.True(t, b.HasDatabase(catalog, "default"), "default metadata must be re-seeded")
			assert.True(t, b.HasTable(catalog, "default", "sample_table"))
			assert.False(t, b.HasDatabase(catalog, fx.Database), "populated database must be dropped")

			assert.Equal(t, 0, b.QueryResultCount())
		})
	}
}

// Test_InMemoryBackend_Restore_V1CalculationProgressDiscarded proves
// gopherstack-hjdd's fix: a v1 snapshot holding CalculationStatistics.Progress
// in the pre-d83f4b5d3 numeric shape must be discarded cleanly now that
// athenaSnapshotVersion is 2, rather than erroring Restore outright when the
// registered "calculations" table's string-typed field can't decode a JSON
// number.
func Test_InMemoryBackend_Restore_V1CalculationProgressDiscarded(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := athena.NewInMemoryBackend("", "")

	v1Snapshot := []byte(`{
		"version": 1,
		"tables": {
			"calculations": [{
				"CalculationExecutionId": "calc-1",
				"SessionId": "session-1",
				"Statistics": {"Progress": 100}
			}]
		}
	}`)

	require.NoError(t, b.Restore(ctx, v1Snapshot),
		"a v1 snapshot must be discarded via the version guard, not error out of RestoreAll")

	_, err := b.GetCalculationExecution("calc-1")
	require.ErrorIs(t, err, athena.ErrResourceNotFound,
		"incompatible-version snapshot must reset to empty, not partially decode")
}

// Test_InMemoryBackend_Restore_InvalidJSON verifies malformed JSON is
// reported as an error rather than silently discarded or partially applied.
func Test_InMemoryBackend_Restore_InvalidJSON(t *testing.T) {
	t.Parallel()

	b := athena.NewInMemoryBackend("", "")
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// Test_InMemoryBackend_Snapshot_Empty verifies Snapshot on a fresh backend
// (holding only NewInMemoryBackend's default seed data) produces a non-nil
// blob that Restore can consume cleanly.
func Test_InMemoryBackend_Snapshot_Empty(t *testing.T) {
	t.Parallel()

	const catalog = "AwsDataCatalog"

	ctx := t.Context()
	b := athena.NewInMemoryBackend("", "")

	data := b.Snapshot(ctx)
	require.NotNil(t, data)

	restored := athena.NewInMemoryBackend("", "")
	require.NoError(t, restored.Restore(ctx, data))

	wgs, _, err := restored.ListWorkGroups("", 0)
	require.NoError(t, err)
	require.Len(t, wgs, 1)
	assert.Equal(t, "primary", wgs[0].Name)

	assert.True(t, restored.HasDatabase(catalog, "default"))
	assert.True(t, restored.HasTable(catalog, "default", "sample_table"))
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore delegate to
// the backend (the shape persistence.Manager drives -- see setupPersistence
// in cli.go, which registers any service.Registerable satisfying this
// Snapshot(ctx)/Restore(ctx, []byte) interface automatically, with no
// per-service registration needed).
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := athena.NewInMemoryBackend("", "")
	h := athena.NewHandler(b)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	require.NoError(t, b.CreateWorkGroup("wg1", "", "", athena.WorkGroupConfiguration{}, nil))

	data := h.Snapshot(ctx)
	require.NotNil(t, data)

	restoredBackend := athena.NewInMemoryBackend("", "")
	restoredHandler := athena.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	wgs, _, err := restoredBackend.ListWorkGroups("", 0)
	require.NoError(t, err)

	var haveWG1 bool

	for _, wg := range wgs {
		if wg.Name == "wg1" {
			haveWG1 = true
		}
	}

	assert.True(t, haveWG1)
}
