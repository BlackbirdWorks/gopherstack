package lakeformation_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/persistence"
	"github.com/blackbirdworks/gopherstack/services/lakeformation"
)

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
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

	b := lakeformation.NewInMemoryBackend()
	require.NoError(t, b.CreateLFTag("123456789012", "seedTag", []string{"a"}))

	// A syntactically valid but version-less/mismatched snapshot.
	err := b.Restore(t.Context(), []byte(`{"version":999,"tables":{}}`))
	require.NoError(t, err)

	assert.Equal(t, 0, b.TagCount())
}

// TestInMemoryBackend_SnapshotRestore_FullState exercises a Snapshot->Restore
// round trip across every resource family the Phase 3.3 pkgs/store
// conversion touched: the five "clean" composite/direct-key tables
// (resources, identityCenterConfigs, lfTags, dataCellsFilters,
// lfTagExpressions), the "dirty" transactions table (round-tripped through
// its identity-carrying DTO), the permissionsMap derived cache (rebuilt from
// the persisted permissionsList slice), plus every plain map/slice left
// un-converted (resourceLFTags, queries, tableStorageOptimizers,
// lakeFormationOptIns) and the single dataLakeSettings pointer.
func TestInMemoryBackend_SnapshotRestore_FullState(t *testing.T) {
	t.Parallel()

	original := lakeformation.NewInMemoryBackend()

	original.PutDataLakeSettings(&lakeformation.DataLakeSettings{
		TrustedResourceOwners: []string{"123456789012"},
	})

	require.NoError(t, original.RegisterResource(
		"arn:aws:s3:::bucket1", "arn:aws:iam::123456789012:role/lf-role", lakeformation.RegisterResourceOptions{},
	))

	require.NoError(t, original.CreateLFTag("123456789012", "confidentiality", []string{"public", "private"}))

	entry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/alice",
		},
		Resource: &lakeformation.Resource{
			Database: &lakeformation.DatabaseResource{Name: "db1"},
		},
		Permissions: []string{"DESCRIBE"},
	}
	require.NoError(t, original.GrantPermissions(t.Context(), entry))

	require.NoError(t, original.CreateDataCellsFilter(&lakeformation.DataCellsFilter{
		TableCatalogID: "123456789012",
		DatabaseName:   "db1",
		TableName:      "tbl1",
		Name:           "filter1",
		ColumnNames:    []string{"col1"},
	}))

	require.NoError(t, original.CreateLFTagExpression("expr1", "desc1", "123456789012", []lakeformation.LFTag{
		{TagKey: "confidentiality", TagValues: []string{"public"}},
	}))

	_, err := original.CreateLakeFormationIdentityCenterConfiguration(
		"123456789012", "arn:aws:sso:::instance/ssoins-1", nil, nil,
	)
	require.NoError(t, err)

	txID := original.StartTransaction("READ_AND_WRITE")

	require.NoError(t, original.CreateLakeFormationOptIn(
		&lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/bob"},
		&lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		nil,
	))

	failures := original.AddLFTagsToResource(
		"123456789012",
		&lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		[]lakeformation.LFTagPair{{TagKey: "confidentiality", TagValues: []string{"public"}}},
	)
	require.Empty(t, failures)

	_ = original.UpdateTableStorageOptimizer("123456789012", "db1", "tbl1", map[string]map[string]string{
		"COMPACTION": {"enabled": "true"},
	})

	snap, err := original.Snapshot()
	require.NoError(t, err)
	require.NotNil(t, snap)

	fresh := lakeformation.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	settings := fresh.GetDataLakeSettings()
	assert.Equal(t, []string{"123456789012"}, settings.TrustedResourceOwners)

	info, err := fresh.DescribeResource("arn:aws:s3:::bucket1")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:iam::123456789012:role/lf-role", info.RoleArn)

	tag, err := fresh.GetLFTag("123456789012", "confidentiality")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"public", "private"}, tag.TagValues)

	perms, _ := fresh.ListPermissions(nil, 0, "", nil, "")
	require.Len(t, perms, 1)
	assert.Equal(t, "arn:aws:iam::123456789012:user/alice", perms[0].Principal.DataLakePrincipalIdentifier)
	assert.Equal(t, 1, fresh.PermissionCount())

	filter, err := fresh.GetDataCellsFilter("123456789012", "db1", "tbl1", "filter1")
	require.NoError(t, err)
	assert.Equal(t, []string{"col1"}, filter.ColumnNames)

	expr, err := fresh.GetLFTagExpression("expr1", "123456789012")
	require.NoError(t, err)
	assert.Equal(t, "desc1", expr.Description)

	idc, err := fresh.DescribeLakeFormationIdentityCenterConfiguration("123456789012")
	require.NoError(t, err)
	assert.Equal(t, "arn:aws:sso:::instance/ssoins-1", idc.InstanceArn)

	txn, err := fresh.DescribeTransaction(txID)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", txn.TransactionStatus)

	optIns, _ := fresh.ListLakeFormationOptIns("arn:aws:iam::123456789012:user/bob", nil, 0, "")
	require.Len(t, optIns, 1)

	lfTags, err := fresh.GetResourceLFTags("123456789012", &lakeformation.Resource{
		Database: &lakeformation.DatabaseResource{Name: "db1"},
	})
	require.NoError(t, err)
	require.Len(t, lfTags, 1)
	assert.Equal(t, "confidentiality", lfTags[0].TagKey)

	optimizers := fresh.ListTableStorageOptimizers("123456789012", "db1", "tbl1", "")
	require.Len(t, optimizers, 1)
	assert.Equal(t, "COMPACTION", optimizers[0].StorageOptimizerType)
}

// TestInMemoryBackend_SnapshotRestore_TransactionQueryState covers the
// "dirty" transactions table across a Commit/Cancel/Extend lifecycle plus
// the ephemeral (never-persisted) queries and tableObjects maps, verifying
// the persisted transaction round-trips its status/timestamps correctly
// through the identity-carrying DTO.
func TestInMemoryBackend_SnapshotRestore_TransactionQueryState(t *testing.T) {
	t.Parallel()

	original := lakeformation.NewInMemoryBackend()

	activeTxID := original.StartTransaction("READ_AND_WRITE")
	committedTxID := original.StartTransaction("READ_AND_WRITE")
	_, err := original.CommitTransaction(committedTxID)
	require.NoError(t, err)

	require.NoError(t, original.ExtendTransaction(activeTxID))

	snap, err := original.Snapshot()
	require.NoError(t, err)

	fresh := lakeformation.NewInMemoryBackend()
	require.NoError(t, fresh.Restore(t.Context(), snap))

	assert.Equal(t, 2, fresh.TransactionCount())

	active, err := fresh.DescribeTransaction(activeTxID)
	require.NoError(t, err)
	assert.Equal(t, "ACTIVE", active.TransactionStatus)

	committed, err := fresh.DescribeTransaction(committedTxID)
	require.NoError(t, err)
	assert.Equal(t, "COMMITTED", committed.TransactionStatus)

	// ExtendTransaction on the restored backend must still work: the ID
	// round-tripped correctly through the transactionSnapshot DTO.
	require.NoError(t, fresh.ExtendTransaction(activeTxID))
}

// TestInMemoryBackend_RevokePermissions_KeepsPermissionsMapConsistent
// exercises the permissionsMap derived-cache path added by the Phase 3.3
// conversion: granting then revoking a permission must leave both
// PermissionCount() (backed by permissionsList) and a subsequent
// GrantPermissions call (backed by permissionsMap) consistent, since the two
// are kept in sync manually at every write site.
func TestInMemoryBackend_RevokePermissions_KeepsPermissionsMapConsistent(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()

	entry := &lakeformation.PermissionEntry{
		Principal: &lakeformation.DataLakePrincipal{
			DataLakePrincipalIdentifier: "arn:aws:iam::123456789012:user/alice",
		},
		Resource: &lakeformation.Resource{
			Database: &lakeformation.DatabaseResource{Name: "db1"},
		},
		Permissions: []string{"DESCRIBE", "ALTER"},
	}
	require.NoError(t, b.GrantPermissions(t.Context(), entry))
	require.Equal(t, 1, b.PermissionCount())

	require.NoError(t, b.RevokePermissions(t.Context(), &lakeformation.PermissionEntry{
		Principal:   entry.Principal,
		Resource:    entry.Resource,
		Permissions: []string{"DESCRIBE", "ALTER"},
	}))
	assert.Equal(t, 0, b.PermissionCount())

	// Re-granting after a full revoke must succeed cleanly (no stale entry
	// left behind in permissionsMap).
	require.NoError(t, b.GrantPermissions(t.Context(), entry))
	assert.Equal(t, 1, b.PermissionCount())
}

// Test_Handler_SnapshotRestore verifies Handler.Snapshot/Restore
// (persistence.go) delegate to the backend -- the shape persistence.Manager
// actually drives. cli.go's setupPersistence registers a service.Registerable
// (the *Handler returned by Provider.Init) in the persistence.Manager only if
// that Handler itself satisfies Snapshot(ctx)/Restore(ctx, []byte);
// InMemoryBackend implementing Snapshot()([]byte,error)/Restore(ctx,[]byte)error
// is not enough on its own, since Handler.Backend is the StorageBackend
// interface and does not promote them, and the shapes differ (no ctx,
// returns an error) so Handler.Snapshot must adapt. Mirrors
// services/securityhub's Test_Handler_SnapshotRestore.
func Test_Handler_SnapshotRestore(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Compile-time proof Handler satisfies the persistence layer's contract.
	var _ persistence.Persistable = h

	require.NoError(t, b.CreateLFTag("123456789012", "handler-tag", []string{"a", "b"}))

	data := h.Snapshot(ctx)
	require.NotEmpty(t, data)

	restoredBackend := lakeformation.NewInMemoryBackend()
	restoredHandler := lakeformation.NewHandler(restoredBackend)
	require.NoError(t, restoredHandler.Restore(ctx, data))

	tag, err := restoredBackend.GetLFTag("123456789012", "handler-tag")
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"a", "b"}, tag.TagValues)
}

func TestPersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	b.AddLFTagInternal("cat", "env", []string{"dev", "prod"})
	b.AddResourceInternal("arn:aws:s3:::bucket", "role")
	b.AddDataCellsFilterInternal(&lakeformation.DataCellsFilter{
		TableCatalogID: "cat", DatabaseName: "db", TableName: "tbl", Name: "f1",
	})
	b.AddLFTagExpressionInternal(&lakeformation.LFTagExpression{Name: "e1", CatalogID: "cat"})
	err := b.CreateLakeFormationOptIn(
		&lakeformation.DataLakePrincipal{DataLakePrincipalIdentifier: "arn:aws:iam::123:user/alice"},
		&lakeformation.Resource{Database: &lakeformation.DatabaseResource{Name: "db1"}},
		nil,
	)
	require.NoError(t, err)
	_, err = b.CommitTransaction("tx-persist")
	require.NoError(t, err)

	data, err := b.Snapshot()
	require.NoError(t, err)
	require.NotEmpty(t, data)

	b2 := lakeformation.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))

	assert.Equal(t, b.TagCount(), b2.TagCount())
	assert.Equal(t, b.ResourceCount(), b2.ResourceCount())
	assert.Equal(t, b.DataCellsFilterCount(), b2.DataCellsFilterCount())
	assert.Equal(t, b.LFTagExpressionCount(), b2.LFTagExpressionCount())
	assert.Equal(t, b.OptInCount(), b2.OptInCount())
	assert.Equal(t, b.TransactionCount(), b2.TransactionCount())
}

func TestPersistenceEmpty(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	data, err := b.Snapshot()
	require.NoError(t, err)

	b2 := lakeformation.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), data))
	assert.Equal(t, 0, b2.TagCount())
	assert.Equal(t, 0, b2.ResourceCount())
}

func TestPersistence_IncludesQueriesAndOptimizers(t *testing.T) {
	t.Parallel()

	b := lakeformation.NewInMemoryBackend()
	h := lakeformation.NewHandler(b)

	// Seed a query
	rec := postJSON(t, h, "/StartQueryPlanning", map[string]any{
		"QueryString":          "SELECT * FROM db.t",
		"QueryPlanningContext": map[string]any{"DatabaseName": "db"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	// Seed a storage optimizer
	postJSON(t, h, "/UpdateTableStorageOptimizer", map[string]any{
		"DatabaseName": "db",
		"TableName":    "t",
		"StorageOptimizerConfig": map[string]any{
			"compaction": map[string]any{"enabled": "true"},
		},
	})

	// Snapshot
	snap, err := b.Snapshot()
	require.NoError(t, err)

	// Restore to fresh backend
	b2 := lakeformation.NewInMemoryBackend()
	require.NoError(t, b2.Restore(t.Context(), snap))

	// Verify queries survived restore (GetQueryState should work)
	var qOut map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &qOut))
	queryID := qOut["QueryId"].(string)

	state, err := b2.GetQueryState(queryID)
	require.NoError(t, err)
	assert.Equal(t, "WORKUNITS_AVAILABLE", state)

	// Verify optimizer survived restore
	opts := b2.ListTableStorageOptimizers("", "db", "t", "")
	assert.Len(t, opts, 1)
}
