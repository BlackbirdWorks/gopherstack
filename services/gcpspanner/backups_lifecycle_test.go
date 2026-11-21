package gcpspanner_test

import (
	"errors"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

func TestBackup_ResetAndCloseWithOperationsInFlight(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(b *gcpspanner.InMemoryBackend)
		name string
	}{
		{name: "reset", stop: func(b *gcpspanner.InMemoryBackend) { b.Reset() }},
		{name: "close", stop: func(b *gcpspanner.InMemoryBackend) { b.Close() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newBackupEnv(t)
			started, release := make(chan struct{}), make(chan struct{})
			e.handler.Backend.SetBackupHook(func() {
				close(started)
				<-release
			})

			_, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
				Parent: bkInst, BackupId: "inflight",
				Backup: &databasepb.Backup{Database: bkDB, ExpireTime: e.expire(48 * time.Hour)},
			})
			require.NoError(t, err)

			<-started

			done := make(chan struct{})

			go func() {
				tt.stop(e.handler.Backend)
				close(done)
			}()

			close(release)
			<-done
		})
	}
}

func TestBackup_OperationMetadataTypes(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.createInstance(t, "bk2")
	e.clock.Advance(2 * time.Hour)

	src := e.createBackup(t, "b1", 48*time.Hour)

	cop, err := e.databases.CopyBackup(t.Context(), &databasepb.CopyBackupRequest{
		Parent: parent + "/instances/bk2", BackupId: "c1", SourceBackup: src.GetName(),
		ExpireTime: e.expire(96 * time.Hour),
	})
	require.NoError(t, err)

	_, err = cop.Wait(t.Context())
	require.NoError(t, err)

	rop, err := e.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: bkInst, DatabaseId: "restored",
		Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: src.GetName()},
	})
	require.NoError(t, err)

	_, err = rop.Wait(t.Context())
	require.NoError(t, err)

	restored := bkInst + "/databases/restored"

	got := map[string]proto.Message{}

	collect := func(it opIterator) {
		for {
			o, nerr := it.Next()
			if errors.Is(nerr, iterator.Done) {
				return
			}

			require.NoError(t, nerr)

			m, uerr := anypb.UnmarshalNew(o.GetMetadata(), proto.UnmarshalOptions{})
			require.NoError(t, uerr)

			got[string(m.ProtoReflect().Descriptor().Name())] = m
		}
	}

	collect(e.databases.ListDatabaseOperations(t.Context(), &databasepb.ListDatabaseOperationsRequest{Parent: bkInst}))
	collect(e.databases.ListBackupOperations(t.Context(), &databasepb.ListBackupOperationsRequest{Parent: bkInst}))
	collect(e.databases.ListBackupOperations(
		t.Context(), &databasepb.ListBackupOperationsRequest{Parent: parent + "/instances/bk2"}))

	create, _ := got["CreateBackupMetadata"].(*databasepb.CreateBackupMetadata)
	require.NotNil(t, create)
	assert.Equal(t, src.GetName(), create.GetName())
	assert.Equal(t, bkDB, create.GetDatabase())
	assert.Equal(t, int32(100), create.GetProgress().GetProgressPercent())

	cp, _ := got["CopyBackupMetadata"].(*databasepb.CopyBackupMetadata)
	require.NotNil(t, cp)
	assert.Equal(t, src.GetName(), cp.GetSourceBackup())
	assert.Equal(t, parent+"/instances/bk2/backups/c1", cp.GetName())

	rest, _ := got["RestoreDatabaseMetadata"].(*databasepb.RestoreDatabaseMetadata)
	require.NotNil(t, rest)
	assert.Equal(t, restored, rest.GetName())
	assert.Equal(t, databasepb.RestoreSourceType_BACKUP, rest.GetSourceType())
	assert.Equal(t, src.GetName(), rest.GetBackupInfo().GetBackup())

	opt, _ := got["OptimizeRestoredDatabaseMetadata"].(*databasepb.OptimizeRestoredDatabaseMetadata)
	require.NotNil(t, opt)
	assert.Equal(t, restored, opt.GetName())
}

func TestBackup_ListFilterHostileInput(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.createBackup(t, "b1", 48*time.Hour)

	filters := []string{
		strings.Repeat("(", 10_000), strings.Repeat("NOT ", 5_000) + "name:b", strings.Repeat("name:b AND ", 2_000),
		"\x00", `name:"\xff"`, "size_bytes > 99999999999999999999999", "create_time < \"not a time\"",
		"state:*", "a.b.c.d.e:1", "((name:b)", "name:b))", "-", "NOT", "AND OR",
	}

	for _, f := range filters {
		assert.NotPanics(t, func() {
			it := e.databases.ListBackups(t.Context(), &databasepb.ListBackupsRequest{Parent: bkInst, Filter: f})
			for {
				if _, err := it.Next(); err != nil {
					if !errors.Is(err, iterator.Done) {
						assert.Equal(t, codes.InvalidArgument, status.Code(err), "%.40q", f)
					}

					return
				}
			}
		})
	}
}

func TestBackup_SnapshotWithoutBackupsRestoresCleanly(t *testing.T) {
	t.Parallel()

	old := newEnvWith(t, nil)
	old.createInstance(t, "bk")
	old.createDB(t, "bk", "CREATE TABLE Singers (id INT64 NOT NULL) PRIMARY KEY (id)")

	snap := old.handler.Snapshot(t.Context())
	require.NotContains(t, string(snap), "backups", "a database without backups writes no backups field")

	e := newBackupEnv(t)
	e.createBackup(t, "b1", 48*time.Hour)

	require.NoError(t, e.handler.Restore(t.Context(), snap))

	_, err := e.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: bkInst + "/backups/b1"})
	assert.Equal(t, codes.NotFound, status.Code(err))

	_, err = e.databases.GetDatabase(t.Context(), &databasepb.GetDatabaseRequest{Name: bkDB})
	require.NoError(t, err)
}

type opIterator interface {
	Next() (*longrunningpb.Operation, error)
}
