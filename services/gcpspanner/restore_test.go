package gcpspanner_test

import (
	"errors"
	"testing"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func TestRestoreDatabase(t *testing.T) {
	t.Parallel()

	const backup = parent + "/instances/rs/backups/b1"

	tests := []struct {
		mutate func(*databasepb.RestoreDatabaseRequest)
		name   string
		want   codes.Code
	}{
		{func(*databasepb.RestoreDatabaseRequest) {}, "ok", codes.OK},
		{func(r *databasepb.RestoreDatabaseRequest) { r.DatabaseId = "BAD" }, "bad id", codes.InvalidArgument},
		{func(r *databasepb.RestoreDatabaseRequest) { r.Source = nil }, "no backup", codes.InvalidArgument},
		{func(r *databasepb.RestoreDatabaseRequest) { r.DatabaseId = "app" }, "exists", codes.AlreadyExists},
		{
			func(r *databasepb.RestoreDatabaseRequest) { r.Parent = parent + "/instances/none" },
			"no instance",
			codes.NotFound,
		},
		{func(r *databasepb.RestoreDatabaseRequest) {
			r.Source = &databasepb.RestoreDatabaseRequest_Backup{Backup: backup + "-missing"}
		}, "unknown backup", codes.NotFound},
		{func(r *databasepb.RestoreDatabaseRequest) {
			r.EncryptionConfig = &databasepb.RestoreDatabaseEncryptionConfig{KmsKeyName: "k"}
		}, "key without type", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			fake, addr := newFakeEmulator(t)
			e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
			e.createInstance(t, "rs")
			e.createDB(t, "rs", "CREATE TABLE t (id INT64) PRIMARY KEY (id)")
			fake.addBackup(backup, parent+"/instances/rs/databases/app")

			req := &databasepb.RestoreDatabaseRequest{
				Parent: parent + "/instances/rs", DatabaseId: "restored",
				Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: backup},
			}
			tt.mutate(req)

			op, err := e.databases.RestoreDatabase(t.Context(), req)
			require.Equal(t, tt.want, status.Code(err))

			if tt.want != codes.OK {
				return
			}

			db, err := await(t, op)
			require.NoError(t, err)
			assert.Equal(t, parent+"/instances/rs/databases/restored", db.GetName())
			assert.Equal(t, databasepb.RestoreSourceType_BACKUP, db.GetRestoreInfo().GetSourceType())
			assert.Equal(t, backup, db.GetRestoreInfo().GetBackupInfo().GetBackup())
			assert.Equal(
				t,
				parent+"/instances/rs/databases/app",
				db.GetRestoreInfo().GetBackupInfo().GetSourceDatabase(),
			)

			ddl, err := e.databases.GetDatabaseDdl(
				t.Context(),
				&databasepb.GetDatabaseDdlRequest{Database: db.GetName()},
			)
			require.NoError(t, err)
			assert.Equal(t, []string{"CREATE TABLE t (id INT64) PRIMARY KEY (id)"}, ddl.GetStatements())

			got, err := e.databases.GetDatabase(t.Context(), &databasepb.GetDatabaseRequest{Name: db.GetName()})
			require.NoError(t, err)
			assert.NotNil(t, got.GetRestoreInfo())
		})
	}
}

func TestRestoreDatabase_NoDataPlane(t *testing.T) {
	t.Parallel()

	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseNoRuntime() })
	e.createInstance(t, "rs")

	_, err := e.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: parent + "/instances/rs", DatabaseId: "restored",
		Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: parent + "/instances/rs/backups/b1"},
	})
	assert.Equal(t, codes.Unavailable, status.Code(err))
}

func TestRestoreDatabase_Persists(t *testing.T) {
	t.Parallel()

	const backup = parent + "/instances/rs/backups/b1"

	fake, addr := newFakeEmulator(t)
	src := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	src.createInstance(t, "rs")
	src.createDB(t, "rs", "CREATE TABLE t (id INT64) PRIMARY KEY (id)")
	fake.addBackup(backup, parent+"/instances/rs/databases/app")

	op, err := src.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: parent + "/instances/rs", DatabaseId: "restored",
		Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: backup},
	})
	require.NoError(t, err)

	_, err = await(t, op)
	require.NoError(t, err)

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	got, err := dst.databases.GetDatabase(
		t.Context(),
		&databasepb.GetDatabaseRequest{Name: parent + "/instances/rs/databases/restored"},
	)
	require.NoError(t, err)
	assert.Equal(t, backup, got.GetRestoreInfo().GetBackupInfo().GetBackup())
}

func TestListDatabaseOperations(t *testing.T) {
	t.Parallel()

	const typePrefix = "type.googleapis.com/google.spanner.admin.database.v1."

	e := newEnv(t)
	e.createInstance(t, "ops")
	e.createDB(t, "ops", "CREATE TABLE t (id INT64) PRIMARY KEY (id)")

	ddl, err := e.databases.UpdateDatabaseDdl(t.Context(), &databasepb.UpdateDatabaseDdlRequest{
		Database: parent + "/instances/ops/databases/app", Statements: []string{"CREATE INDEX i ON t (id)"},
	})
	require.NoError(t, err)
	require.NoError(t, ddl.Wait(t.Context()))

	tests := []struct {
		name    string
		parent  string
		filter  string
		want    int
		wantErr codes.Code
	}{
		{"all", parent + "/instances/ops", "", 2, codes.OK},
		{"create", parent + "/instances/ops", "metadata.@type=" + typePrefix + "CreateDatabaseMetadata", 1, codes.OK},
		{
			"ddl",
			parent + "/instances/ops",
			"metadata.@type:UpdateDatabaseDdlMetadata AND metadata.database:app",
			1,
			codes.OK,
		},
		{
			"restore none",
			parent + "/instances/ops",
			"metadata.@type=" + typePrefix + "RestoreDatabaseMetadata",
			0,
			codes.OK,
		},
		{"no instance", parent + "/instances/none", "", 0, codes.NotFound},
		{"bad filter", parent + "/instances/ops", "(", 0, codes.InvalidArgument},
		{"bad parent", "garbage", "", 0, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.databases.ListDatabaseOperations(
				t.Context(),
				&databasepb.ListDatabaseOperationsRequest{Parent: tt.parent, Filter: tt.filter},
			)

			n := 0

			for {
				_, nerr := it.Next()
				if errors.Is(nerr, iterator.Done) {
					break
				}

				if nerr != nil {
					require.Equal(t, tt.wantErr, status.Code(nerr))

					return
				}

				n++
			}

			require.Equal(t, codes.OK, tt.wantErr)
			assert.Equal(t, tt.want, n)
		})
	}
}
