package timestreamwrite_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/timestreamwrite"
)

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	err := b.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

// TestInMemoryBackend_RestoreVersionMismatch verifies that a snapshot whose
// version doesn't match the current backend (including the pre-Phase-3.3
// format, which decodes with Version == 0) is discarded cleanly rather than
// partially decoded: the backend resets to empty state and Restore returns
// no error.
func TestInMemoryBackend_RestoreVersionMismatch(t *testing.T) {
	t.Parallel()

	b := timestreamwrite.NewInMemoryBackend()
	_, err := b.CreateDatabase("seed-db", nil)
	require.NoError(t, err)

	// A syntactically valid but version-less/mismatched snapshot.
	err = b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, timestreamwrite.DatabaseCount(b))
	assert.Equal(t, 0, timestreamwrite.TableCount(b))
	assert.Equal(t, 0, timestreamwrite.BatchLoadTaskCount(b))
	assert.Equal(t, 0, timestreamwrite.TagCount(b))
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched: the three store.Table-backed collections (databases,
// tables -- flattened from the old nested map via the composite
// "database|table" key plus its byDatabase index -- and batchLoadTasks),
// plus every plain map left un-converted (records, tags) and the nextTaskID
// counter.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := timestreamwrite.NewInMemoryBackend()

	db1Created, err := original.CreateDatabase("db1", map[string]string{"env": "test"})
	require.NoError(t, err)
	_, err = original.CreateDatabase("db2", nil)
	require.NoError(t, err)

	_, err = original.CreateTable("db1", "tbl1", map[string]string{"team": "obs"}, &timestreamwrite.CreateTableInput{
		RetentionProperties: &timestreamwrite.RetentionProperties{
			MemoryStoreRetentionPeriodInHours:  12,
			MagneticStoreRetentionPeriodInDays: 30,
		},
	})
	require.NoError(t, err)

	_, err = original.CreateTable("db1", "tbl2", nil, nil)
	require.NoError(t, err)

	_, err = original.CreateTable("db2", "tbl3", nil, nil)
	require.NoError(t, err)

	_, err = original.WriteRecords("db1", "tbl1", []timestreamwrite.Record{
		{
			MeasureName:      "cpu",
			MeasureValue:     "42.5",
			MeasureValueType: "DOUBLE",
			Time:             "1700000000",
			TimeUnit:         "SECONDS",
			Dimensions:       []timestreamwrite.Dimension{{Name: "host", Value: "a"}},
		},
	})
	require.NoError(t, err)

	require.NoError(t, original.TagResource(db1Created.ARN, map[string]string{"k": "v"}))

	task1, err := original.CreateBatchLoadTask("db1", "tbl1", nil, nil)
	require.NoError(t, err)

	snap, err := original.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	fresh := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	db1, err := fresh.DescribeDatabase("db1")
	require.NoError(t, err)
	assert.Equal(t, 2, db1.TableCount)

	_, err = fresh.DescribeDatabase("db2")
	require.NoError(t, err)

	tbl1, err := fresh.DescribeTable("db1", "tbl1")
	require.NoError(t, err)
	require.NotNil(t, tbl1.RetentionProperties)
	assert.Equal(t, int64(12), tbl1.RetentionProperties.MemoryStoreRetentionPeriodInHours)

	tables, err := fresh.ListTables("db1")
	require.NoError(t, err)
	require.Len(t, tables, 2)

	tables2, err := fresh.ListTables("db2")
	require.NoError(t, err)
	require.Len(t, tables2, 1)
	assert.Equal(t, "tbl3", tables2[0].TableName)

	assert.Equal(t, 1, timestreamwrite.RecordCount(fresh, "db1", "tbl1"))

	tags := fresh.ListTagsForResource(db1Created.ARN)
	assert.Equal(t, "v", tags["k"])

	task, err := fresh.DescribeBatchLoadTask(task1.TaskID)
	require.NoError(t, err)
	assert.Equal(t, "db1", task.TargetDatabaseName)

	// nextTaskID must have survived the round trip so a post-restore task ID
	// continues the sequence instead of colliding with task1's.
	task2, err := fresh.CreateBatchLoadTask("db1", "tbl1", nil, nil)
	require.NoError(t, err)
	assert.NotEqual(t, task1.TaskID, task2.TaskID)

	// Writing a new, higher-version record after restore must still exercise
	// the per-table dedup index correctly (rebuilt fresh by Restore).
	_, err = fresh.WriteRecords("db1", "tbl1", []timestreamwrite.Record{
		{
			MeasureName:      "cpu",
			MeasureValue:     "43.1",
			MeasureValueType: "DOUBLE",
			Time:             "1700000000",
			TimeUnit:         "SECONDS",
			Dimensions:       []timestreamwrite.Dimension{{Name: "host", Value: "a"}},
			Version:          2,
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, timestreamwrite.RecordCount(fresh, "db1", "tbl1"))
}

// TestInMemoryBackend_SnapshotRestore_EmptyState verifies that a backend with
// no resources at all round-trips cleanly (every store.Table and raw map
// left empty, not nil).
func TestInMemoryBackend_SnapshotRestore_EmptyState(t *testing.T) {
	t.Parallel()

	original := timestreamwrite.NewInMemoryBackend()

	snap, err := original.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	fresh := timestreamwrite.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 0, timestreamwrite.DatabaseCount(fresh))
	assert.Equal(t, 0, timestreamwrite.TableCount(fresh))
	assert.Equal(t, 0, timestreamwrite.BatchLoadTaskCount(fresh))
	assert.Equal(t, 0, timestreamwrite.TagCount(fresh))

	_, err = fresh.CreateDatabase("db", nil)
	require.NoError(t, err)
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing Snapshot()([]byte,error)/Restore(ctx,[]byte)error
// is not enough on its own, since Handler.Backend's methods are never
// promoted (Backend is a named field, not embedded), and the shapes differ
// (no ctx, returns an error) so Handler.Snapshot must adapt. Mirrors
// services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	backend := timestreamwrite.NewInMemoryBackend()
	h := timestreamwrite.NewHandler(backend)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	_, err := backend.CreateDatabase("handler-db", map[string]string{"env": "test"})
	require.NoError(t, err)

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := timestreamwrite.NewInMemoryBackend()
	restoredHandler := timestreamwrite.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	assert.Equal(t, 1, timestreamwrite.DatabaseCount(restoredBackend))

	db, err := restoredBackend.DescribeDatabase("handler-db")
	require.NoError(t, err)
	assert.Equal(t, "handler-db", db.DatabaseName)
}
