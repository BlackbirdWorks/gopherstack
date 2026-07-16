package rds_test

import (
	"fmt"
	"net/http"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateTenantDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErrIs    error
		name         string
		instanceID   string
		tenantDBName string
		wantErr      bool
	}{
		{name: "success", instanceID: "db-1", tenantDBName: "tenantdb"},
		{
			name:         "empty instanceID",
			instanceID:   "",
			tenantDBName: "t1",
			wantErr:      true,
			wantErrIs:    rds.ErrInvalidParameter,
		},
		{
			name:         "empty tenantDBName",
			instanceID:   "db-1",
			tenantDBName: "",
			wantErr:      true,
			wantErrIs:    rds.ErrInvalidParameter,
		},
		{
			name:         "duplicate",
			instanceID:   "db-dup",
			tenantDBName: "tdup",
			wantErr:      true,
			wantErrIs:    rds.ErrTenantDatabaseAlreadyExists,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			b := newTestBackend(t)

			if tt.name == "duplicate" {
				_, err := b.CreateTenantDatabase(tt.instanceID, tt.tenantDBName, "admin")
				require.NoError(t, err)
			}

			tdb, err := b.CreateTenantDatabase(tt.instanceID, tt.tenantDBName, "admin")
			if tt.wantErr {
				require.Error(t, err)
				if tt.wantErrIs != nil {
					require.ErrorIs(t, err, tt.wantErrIs)
				}

				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.instanceID, tdb.DBInstanceIdentifier)
			assert.Equal(t, tt.tenantDBName, tdb.TenantDBName)
			assert.Equal(t, "admin", tdb.MasterUsername)
			assert.Equal(t, "available", tdb.Status)
			assert.NotEmpty(t, tdb.TenantDatabaseARN)
		})
	}
}

func TestDeleteTenantDatabase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateTenantDatabase("db-1", "tdb-del", "admin")
		require.NoError(t, err)

		tdb, err := b.DeleteTenantDatabase("db-1", "tdb-del")
		require.NoError(t, err)
		assert.Equal(t, "deleting", tdb.Status)

		tdbs, err := b.DescribeTenantDatabases("db-1", "tdb-del")
		require.NoError(t, err)
		assert.Empty(t, tdbs)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.DeleteTenantDatabase("db-1", "missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrTenantDatabaseNotFound)
	})
}

func TestDescribeTenantDatabases(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		tdbs, err := b.DescribeTenantDatabases("", "")
		require.NoError(t, err)
		assert.Empty(t, tdbs)
	})

	t.Run("filter by instance", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		for _, tname := range []string{"t1", "t2"} {
			_, err := b.CreateTenantDatabase("db-1", tname, "admin")
			require.NoError(t, err)
		}
		_, err := b.CreateTenantDatabase("db-2", "t3", "admin")
		require.NoError(t, err)

		tdbs, err := b.DescribeTenantDatabases("db-1", "")
		require.NoError(t, err)
		assert.Len(t, tdbs, 2)
		for _, tdb := range tdbs {
			assert.Equal(t, "db-1", tdb.DBInstanceIdentifier)
		}
	})

	t.Run("filter by name", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateTenantDatabase("db-1", "ta", "admin")
		require.NoError(t, err)
		_, err = b.CreateTenantDatabase("db-1", "tb", "admin")
		require.NoError(t, err)

		tdbs, err := b.DescribeTenantDatabases("", "ta")
		require.NoError(t, err)
		require.Len(t, tdbs, 1)
		assert.Equal(t, "ta", tdbs[0].TenantDBName)
	})
}

func TestModifyTenantDatabase(t *testing.T) {
	t.Parallel()

	t.Run("success", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.CreateTenantDatabase("db-1", "tdb-mod", "admin")
		require.NoError(t, err)

		tdb, err := b.ModifyTenantDatabase("db-1", "tdb-mod")
		require.NoError(t, err)
		assert.Equal(t, "tdb-mod", tdb.TenantDBName)
	})

	t.Run("not found", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		_, err := b.ModifyTenantDatabase("db-1", "missing")
		require.Error(t, err)
		require.ErrorIs(t, err, rds.ErrTenantDatabaseNotFound)
	})
}

func TestDescribeDBSnapshotTenantDatabases(t *testing.T) {
	t.Parallel()

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		entries := b.DescribeDBSnapshotTenantDatabases("", "")
		assert.Empty(t, entries)
	})

	t.Run("filter by snapshot", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		b.AddDBSnapshotTenantDatabase("snap-1", "db-1", "tdb-a", "postgres")
		b.AddDBSnapshotTenantDatabase("snap-1", "db-1", "tdb-b", "postgres")
		b.AddDBSnapshotTenantDatabase("snap-2", "db-2", "tdb-c", "postgres")

		entries := b.DescribeDBSnapshotTenantDatabases("snap-1", "")
		require.Len(t, entries, 2)
		for _, e := range entries {
			assert.Equal(t, "snap-1", e.DBSnapshotIdentifier)
		}
	})

	t.Run("filter by instance", func(t *testing.T) {
		t.Parallel()
		b := newTestBackend(t)
		b.AddDBSnapshotTenantDatabase("snap-1", "db-1", "tdb-a", "postgres")
		b.AddDBSnapshotTenantDatabase("snap-2", "db-2", "tdb-b", "postgres")

		entries := b.DescribeDBSnapshotTenantDatabases("", "db-1")
		require.Len(t, entries, 1)
		assert.Equal(t, "snap-1", entries[0].DBSnapshotIdentifier)
	})
}

func TestHandler_TenantDatabaseCRUD(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Create
	rec := postRDSForm(t, h,
		"Action=CreateTenantDatabase&Version=2014-10-31"+
			"&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb&MasterUsername=admin")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mytenantdb")

	// Describe all
	rec = postRDSForm(t, h, "Action=DescribeTenantDatabases&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mytenantdb")

	// Describe filtered
	rec = postRDSForm(
		t,
		h,
		"Action=DescribeTenantDatabases&Version=2014-10-31&DBInstanceIdentifier=db-1",
	)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "mytenantdb")

	// Modify
	rec = postRDSForm(
		t,
		h,
		"Action=ModifyTenantDatabase&Version=2014-10-31&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Delete
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteTenantDatabase&Version=2014-10-31&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb",
	)
	assert.Equal(t, http.StatusOK, rec.Code)

	// Confirm gone
	rec = postRDSForm(
		t,
		h,
		"Action=DeleteTenantDatabase&Version=2014-10-31&DBInstanceIdentifier=db-1&TenantDBName=mytenantdb",
	)
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_DBSnapshotTenantDatabases(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	// Describe empty
	rec := postRDSForm(t, h, "Action=DescribeDBSnapshotTenantDatabases&Version=2014-10-31")
	assert.Equal(t, http.StatusOK, rec.Code)

	// Describe with filter
	rec = postRDSForm(t, h,
		"Action=DescribeDBSnapshotTenantDatabases&Version=2014-10-31&DBSnapshotIdentifier=snap-1")
	assert.Equal(t, http.StatusOK, rec.Code)
}

func TestHandler_TenantDatabase_DuplicateError(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	rec := postRDSForm(t, h,
		"Action=CreateTenantDatabase&Version=2014-10-31"+
			"&DBInstanceIdentifier=db-1&TenantDBName=tdup&MasterUsername=admin")
	require.Equal(t, http.StatusOK, rec.Code)

	rec = postRDSForm(t, h,
		"Action=CreateTenantDatabase&Version=2014-10-31"+
			"&DBInstanceIdentifier=db-1&TenantDBName=tdup&MasterUsername=admin")
	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "TenantDatabaseAlreadyExists")
}

func TestTenantDatabase_ConcurrentReadWrite(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)

	var wg sync.WaitGroup
	errs := make(chan error, 20)

	for i := range 10 {
		wg.Go(func() {
			if _, err := b.CreateTenantDatabase(fmt.Sprintf("db-%d", i), "tenantdb", "admin"); err != nil {
				errs <- err
			}
		})
	}

	for range 10 {
		wg.Go(func() {
			if _, err := b.DescribeTenantDatabases("", ""); err != nil {
				errs <- err
			}
		})
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		require.NoError(t, err)
	}
}

func TestHandler_DescribeTenantDatabases_Pagination(t *testing.T) {
	t.Parallel()
	h := newRDSHandler()

	for i := range 5 {
		rec := postRDSForm(t, h, fmt.Sprintf(
			"Action=CreateTenantDatabase&Version=2014-10-31"+
				"&DBInstanceIdentifier=db-1&TenantDBName=t%02d&MasterUsername=admin", i,
		))
		require.Equal(t, http.StatusOK, rec.Code)
	}

	rec := postRDSForm(t, h, "Action=DescribeTenantDatabases&Version=2014-10-31&MaxRecords=2")
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Contains(t, rec.Body.String(), "Marker")
}

func TestCreateTenantDatabase_ARNFormat(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)

	tdb, err := b.CreateTenantDatabase("db-1", "tdb-1", "admin")
	require.NoError(t, err)
	assert.Contains(t, tdb.TenantDatabaseARN, "arn:aws:rds:")
	assert.Contains(t, tdb.TenantDatabaseARN, ":tenant-database:")
	assert.NotEmpty(t, tdb.DbiResourceID)
}

func TestBatch2_Persistence_TenantAndAutomatedBackups(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	_, err := b.CreateTenantDatabase("inst-1", "tenantdb", "admin")
	require.NoError(t, err)

	_, err = b.StartDBInstanceAutomatedBackupsReplication(
		"arn:aws:rds:us-west-2:123:db:src", 14,
	)
	require.NoError(t, err)

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	tenants, err := b2.DescribeTenantDatabases("inst-1", "")
	require.NoError(t, err)
	assert.Len(t, tenants, 1)
	assert.Equal(t, "tenantdb", tenants[0].TenantDBName)

	// Attempting to stop replication of the restored backup should work
	_, err = b2.StopDBInstanceAutomatedBackupsReplication(
		"arn:aws:rds:us-west-2:123:db:src",
	)
	require.NoError(t, err)
}

func TestBatch2_Persistence_SnapshotTenantDatabases(t *testing.T) {
	t.Parallel()

	b := newBatch2Backend()

	b.AddDBSnapshotTenantDatabase("snap-1", "inst-1", "tenantA", "postgres")
	b.AddDBSnapshotTenantDatabase("snap-1", "inst-1", "tenantB", "postgres")

	snap := b.Snapshot(t.Context())
	require.NotNil(t, snap)

	b2 := rds.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, b2.Restore(t.Context(), snap))

	tenants := b2.DescribeDBSnapshotTenantDatabases("snap-1", "")
	assert.Len(t, tenants, 2)
}
