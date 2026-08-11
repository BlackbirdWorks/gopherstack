// Package dynamodb_test covers three PITR guarantees: snapshots must survive a
// persistence round-trip (Table.PITRSnapshots must be exported for
// encoding/json), the ring's ~1-hour coverage window must hold at the janitor's
// PITR ticker cadence, and an out-of-window RestoreTableToPointInTime must
// return InvalidRestoreTimeException rather than silently produce an empty table.
package dynamodb_test

import (
	"context"
	"net/http"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// enablePITR flips PointInTimeRecoveryEnabled on for tableName via the wire
// UpdateContinuousBackups handler, matching how a real client would do it.
func enablePITR(t *testing.T, h *dynamodb.DynamoDBHandler, tableName string) {
	t.Helper()

	code, _ := doBackupRequest(t, h, "DynamoDB_20120810.UpdateContinuousBackups", map[string]any{
		"TableName": tableName,
		"PointInTimeRecoverySpecification": map[string]any{
			"PointInTimeRecoveryEnabled": true,
		},
	})
	require.Equal(t, http.StatusOK, code, "UpdateContinuousBackups: enable PITR")
}

// TestPITR_SnapshotsSurvivePersistenceRoundTrip is a regression test for bug 1:
// Table.pitrSnapshots (unexported) was silently dropped by encoding/json, so a
// server restart reported PITR as ENABLED with zero restore points. The field
// is now Table.PITRSnapshots (exported, tagged); this asserts it actually
// survives Snapshot -> Restore.
func TestPITR_SnapshotsSurvivePersistenceRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	db := newInMemoryTestDB(t)
	h := dynamodb.NewHandler(db)
	createSimpleTestTable(t, db, "PITRPersistTable")
	enablePITR(t, h, "PITRPersistTable")

	putTestItem(t, db, "PITRPersistTable", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	// SweepOnce runs a full synchronous janitor pass, including a PITR
	// snapshot of every PITR-enabled table (see Janitor.runOnce).
	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(ctx)

	tbl, ok := db.GetTableInRegion("PITRPersistTable", "us-east-1")
	require.True(t, ok)
	require.NotEmpty(t, tbl.PITRSnapshots, "sanity: a snapshot must exist before persisting")
	wantSnapshotCount := len(tbl.PITRSnapshots)

	data := db.Snapshot(ctx)
	require.NotEmpty(t, data)

	restored := dynamodb.NewInMemoryDB()
	t.Cleanup(restored.Close)
	restored.SetDefaultRegion("us-east-1")
	require.NoError(t, restored.Restore(ctx, data))

	restoredTbl, ok := restored.GetTableInRegion("PITRPersistTable", "us-east-1")
	require.True(t, ok)
	assert.Len(
		t,
		restoredTbl.PITRSnapshots,
		wantSnapshotCount,
		"PITR snapshots must survive a persistence round-trip (bug 1 regression)",
	)
	require.NotEmpty(t, restoredTbl.PITRSnapshots)
	assert.True(t, tbl.PITRSnapshots[0].Taken.Equal(restoredTbl.PITRSnapshots[0].Taken))

	// PITREnabled surviving alone (the pre-fix behavior) is not sufficient --
	// DescribeContinuousBackups would then lie about having restore points.
	assert.True(t, restoredTbl.PITREnabled)
}

// TestPITR_SnapshotCadenceDecoupledFromMainSweep proves PITR snapshotting runs
// on its own ~1-minute ticker (defaultPITRSnapshotInterval), independent of the
// 500ms housekeeping sweep, by driving the housekeeping ticker very fast (2ms)
// for well over 60 fires and asserting no PITR snapshot was taken.
func TestPITR_SnapshotCadenceDecoupledFromMainSweep(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(t.Context())

	db := newInMemoryTestDB(t)
	h := dynamodb.NewHandler(db)
	createSimpleTestTable(t, db, "PITRCadenceTable")
	enablePITR(t, h, "PITRCadenceTable")

	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: 2 * time.Millisecond})

	done := make(chan struct{})
	go func() {
		defer close(done)
		j.Run(ctx)
	}()

	// 300ms at a 2ms housekeeping interval is >100 fast-ticker fires -- far
	// more than the 60-slot ring's capacity -- while the PITR ticker
	// (1 minute) cannot have fired even once.
	time.Sleep(300 * time.Millisecond)
	cancel()
	<-done

	tbl, ok := db.GetTableInRegion("PITRCadenceTable", "us-east-1")
	require.True(t, ok)
	assert.Empty(
		t,
		tbl.PITRSnapshots,
		"PITR snapshot must not be taken by the fast housekeeping ticker; "+
			"it must only fire on its own slower, decoupled ticker",
	)
}

// TestPITR_RestoreOutsideWindow_ReturnsInvalidRestoreTimeException is a
// regression test for bug 3: requesting a RestoreDateTime older than every
// retained snapshot used to silently install an empty target table. Real AWS
// returns InvalidRestoreTimeException (confirmed against aws-sdk-go-v2
// service/dynamodb@v1.62.0's types/errors.go and the
// deserializeOpErrorRestoreTableToPointInTime switch in deserializers.go).
func TestPITR_RestoreOutsideWindow_ReturnsInvalidRestoreTimeException(t *testing.T) {
	t.Parallel()
	ctx := t.Context()

	db := newInMemoryTestDB(t)
	h := dynamodb.NewHandler(db)
	createSimpleTestTable(t, db, "PITROOWTable")
	enablePITR(t, h, "PITROOWTable")

	putTestItem(t, db, "PITROOWTable", map[string]types.AttributeValue{
		"pk": &types.AttributeValueMemberS{Value: "pk1"},
		"sk": &types.AttributeValueMemberS{Value: "sk1"},
	})

	j := dynamodb.NewJanitor(db, dynamodb.Settings{JanitorInterval: time.Hour})
	j.SweepOnce(ctx) // the only snapshot taken is "now"

	tbl, ok := db.GetTableInRegion("PITROOWTable", "us-east-1")
	require.True(t, ok)
	require.NotEmpty(t, tbl.PITRSnapshots)
	earliest := tbl.PITRSnapshots[0].Taken

	// Well before the only available snapshot -- outside the recovery window.
	// Sent as a raw JSON number (Unix epoch seconds), not through
	// models.RestoreTableToPointInTimeInput: the real SDK's awsjson1_0 protocol
	// serializes RestoreDateTime as a JSON number via smithytime.FormatEpochSeconds,
	// never a string, and building the request through our own Go struct would
	// make the test tautological against a wrongly-typed field.
	restoreTime := float64(earliest.Add(-1*time.Hour).UTC().UnixNano()) / float64(time.Second)

	code, resp := doBackupRequest(
		t,
		h,
		"DynamoDB_20120810.RestoreTableToPointInTime",
		map[string]any{
			"SourceTableName": "PITROOWTable",
			"TargetTableName": "PITROOWRestored",
			"RestoreDateTime": restoreTime,
		},
	)

	require.Equal(t, http.StatusBadRequest, code)
	assert.Contains(t, resp["__type"], "InvalidRestoreTimeException")

	// The empty-table bug: the target must not have been created at all.
	_, exists := db.GetTableInRegion("PITROOWRestored", "us-east-1")
	assert.False(t, exists, "target table must not be created on an out-of-window restore")
}
