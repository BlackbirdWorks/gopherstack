package gcpspanner_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/apiv1/spannerpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/iterator"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpspanner"
)

const (
	bkInst = parent + "/instances/bk"
	bkDB   = bkInst + "/databases/app"
)

type testClock struct {
	now time.Time
	mu  sync.Mutex
}

func (c *testClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.now
}

func (c *testClock) Advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.now = c.now.Add(d)
}

type bkEnv struct {
	*env
	fake  *fakeEmulator
	clock *testClock
}

func bkTables() []fakeTable {
	return []fakeTable{{
		name: "Singers", columns: []string{"id", "name"},
		rows: [][]*structpb.Value{{sv("1"), sv("Ann")}, {sv("2"), sv("Bo")}},
	}}
}

func newBackupEnv(t *testing.T) *bkEnv {
	t.Helper()

	fake, addr := newFakeEmulator(t)
	clock := &testClock{now: time.Date(2030, 1, 1, 0, 0, 0, 0, time.UTC)}
	e := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) {
		b.SetClock(clock.Now)
		b.UseEmulatorAt(addr, nil)
	})
	e.createInstance(t, "bk")
	e.createDB(t, "bk", "CREATE TABLE Singers (id INT64 NOT NULL, name STRING(MAX)) PRIMARY KEY (id)")
	fake.seed(bkDB, bkTables()...)

	return &bkEnv{env: e, fake: fake, clock: clock}
}

func (e *bkEnv) expire(d time.Duration) *timestamppb.Timestamp {
	return timestamppb.New(e.clock.Now().Add(d))
}

func (e *bkEnv) createBackup(t *testing.T, id string, expire time.Duration) *databasepb.Backup {
	t.Helper()

	op, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
		Parent: bkInst, BackupId: id, Backup: &databasepb.Backup{Database: bkDB, ExpireTime: e.expire(expire)},
	})
	require.NoError(t, err)

	bk, err := op.Wait(t.Context())
	require.NoError(t, err)

	return bk
}

func (e *bkEnv) getBackup(t *testing.T, name string) *databasepb.Backup {
	t.Helper()

	bk, err := e.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: name})
	require.NoError(t, err)

	return bk
}

func TestBackup_CreateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(e *bkEnv, r *databasepb.CreateBackupRequest)
		name   string
		want   codes.Code
	}{
		{func(*bkEnv, *databasepb.CreateBackupRequest) {}, "ok", codes.OK},
		{func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.BackupId = "B" }, "bad id", codes.InvalidArgument},
		{func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.BackupId = "a" }, "short id", codes.InvalidArgument},
		{
			func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.Parent = "garbage" },
			"bad parent",
			codes.InvalidArgument,
		},
		{
			func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.Parent = parent + "/instances/none" },
			"no instance",
			codes.InvalidArgument,
		},
		{func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.Backup = nil }, "no backup", codes.InvalidArgument},
		{
			func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.Backup.ExpireTime = nil },
			"no expire time",
			codes.InvalidArgument,
		},
		{
			func(e *bkEnv, r *databasepb.CreateBackupRequest) { r.Backup.ExpireTime = e.expire(5 * time.Hour) },
			"expires too soon",
			codes.InvalidArgument,
		},
		{func(e *bkEnv, r *databasepb.CreateBackupRequest) {
			r.Backup.ExpireTime = e.expire(367 * 24 * time.Hour)
		}, "expires too late", codes.InvalidArgument},
		{
			func(_ *bkEnv, r *databasepb.CreateBackupRequest) { r.Backup.Database = bkInst + "/databases/nope" },
			"no database",
			codes.NotFound,
		},
		{func(_ *bkEnv, r *databasepb.CreateBackupRequest) {
			r.Backup.Database = parent + "/instances/other/databases/app"
		}, "other instance", codes.InvalidArgument},
		{
			func(e *bkEnv, r *databasepb.CreateBackupRequest) { r.Backup.VersionTime = e.expire(time.Hour) },
			"future version time",
			codes.InvalidArgument,
		},
		{
			func(e *bkEnv, r *databasepb.CreateBackupRequest) { r.Backup.VersionTime = e.expire(-24 * time.Hour) },
			"version time before the database",
			codes.InvalidArgument,
		},
		{func(_ *bkEnv, r *databasepb.CreateBackupRequest) {
			r.EncryptionConfig = &databasepb.CreateBackupEncryptionConfig{KmsKeyName: "k"}
		}, "key without type", codes.InvalidArgument},
		{func(_ *bkEnv, r *databasepb.CreateBackupRequest) {
			r.EncryptionConfig = &databasepb.CreateBackupEncryptionConfig{
				EncryptionType: databasepb.CreateBackupEncryptionConfig_CUSTOMER_MANAGED_ENCRYPTION, KmsKeyName: "k",
			}
		}, "customer managed without a key service", codes.Unimplemented},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newBackupEnv(t)
			e.clock.Advance(2 * time.Hour)

			req := &databasepb.CreateBackupRequest{
				Parent: bkInst, BackupId: "b1",
				Backup: &databasepb.Backup{Database: bkDB, ExpireTime: e.expire(48 * time.Hour)},
			}
			tt.mutate(e, req)

			op, err := e.databases.CreateBackup(t.Context(), req)
			require.Equal(t, tt.want, status.Code(err), "%v", err)

			if tt.want == codes.OK {
				_, err = op.Wait(t.Context())
				require.NoError(t, err)
			}
		})
	}
}

func TestBackup_CreateCapturesRowsAndMetadata(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.clock.Advance(2 * time.Hour)

	created := e.clock.Now()
	version := created.Add(-30 * time.Minute)

	op, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
		Parent: bkInst, BackupId: "b1",
		Backup: &databasepb.Backup{
			Database: bkDB, ExpireTime: e.expire(48 * time.Hour), VersionTime: timestamppb.New(version),
		},
	})
	require.NoError(t, err)

	bk, err := op.Wait(t.Context())
	require.NoError(t, err)

	assert.Equal(t, bkInst+"/backups/b1", bk.GetName())
	assert.Equal(t, bkDB, bk.GetDatabase())
	assert.Equal(t, databasepb.Backup_READY, bk.GetState())
	assert.Equal(t, databasepb.DatabaseDialect_GOOGLE_STANDARD_SQL, bk.GetDatabaseDialect())
	assert.True(t, created.Equal(bk.GetCreateTime().AsTime()))
	assert.True(t, version.Equal(bk.GetVersionTime().AsTime()))
	assert.True(t, created.Add(48*time.Hour).Equal(bk.GetExpireTime().AsTime()))
	assert.True(t, created.Add(366*24*time.Hour).Equal(bk.GetMaxExpireTime().AsTime()))
	assert.Positive(t, bk.GetSizeBytes())
	assert.Equal(t, bk.GetSizeBytes(), bk.GetExclusiveSizeBytes())
	assert.Equal(t, databasepb.EncryptionInfo_GOOGLE_DEFAULT_ENCRYPTION, bk.GetEncryptionInfo().GetEncryptionType())
	assert.Empty(t, bk.GetReferencingDatabases())

	meta, err := op.Metadata()
	require.NoError(t, err)
	assert.Equal(t, bk.GetName(), meta.GetName())
	assert.Equal(t, bkDB, meta.GetDatabase())
	assert.EqualValues(t, 100, meta.GetProgress().GetProgressPercent())

	dup := &databasepb.CreateBackupRequest{
		Parent:   bkInst,
		BackupId: "b1",
		Backup:   &databasepb.Backup{Database: bkDB, ExpireTime: e.expire(48 * time.Hour)},
	}
	_, err = e.databases.CreateBackup(t.Context(), dup)
	assert.Equal(t, codes.AlreadyExists, status.Code(err))
}

func TestBackup_RestoreReplaysRowsAndTracksReferences(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.clock.Advance(2 * time.Hour)

	release := make(chan struct{})
	e.handler.Backend.SetOptimizeHook(func() { <-release })

	src := e.createBackup(t, "b1", 48*time.Hour)

	op, err := e.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent:     bkInst,
		DatabaseId: "restored",
		Source:     &databasepb.RestoreDatabaseRequest_Backup{Backup: src.GetName()},
	})
	require.NoError(t, err)

	db, err := op.Wait(t.Context())
	require.NoError(t, err)

	restored := bkInst + "/databases/restored"
	assert.Equal(t, restored, db.GetName())
	assert.Equal(t, databasepb.Database_READY_OPTIMIZING, db.GetState())
	assert.Equal(t, src.GetName(), db.GetRestoreInfo().GetBackupInfo().GetBackup())
	assert.Equal(t, bkDB, db.GetRestoreInfo().GetBackupInfo().GetSourceDatabase())
	assert.True(t, src.GetVersionTime().AsTime().Equal(db.GetRestoreInfo().GetBackupInfo().GetVersionTime().AsTime()))

	ddl, err := e.databases.GetDatabaseDdl(t.Context(), &databasepb.GetDatabaseDdlRequest{Database: restored})
	require.NoError(t, err)
	assert.Equal(
		t,
		[]string{"CREATE TABLE Singers (id INT64 NOT NULL, name STRING(MAX)) PRIMARY KEY (id)"},
		ddl.GetStatements(),
	)

	var rows []string

	for _, c := range e.fake.committed() {
		if c.db == restored {
			for _, r := range c.rows {
				rows = append(rows, c.table+":"+r[0]+":"+r[1])
			}
		}
	}

	assert.Equal(t, []string{"Singers:1:Ann", "Singers:2:Bo"}, rows)

	held := e.getBackup(t, src.GetName())
	assert.Equal(t, []string{restored}, held.GetReferencingDatabases())

	err = e.databases.DeleteBackup(t.Context(), &databasepb.DeleteBackupRequest{Name: src.GetName()})
	assert.Equal(t, codes.FailedPrecondition, status.Code(err), "a referenced backup cannot be deleted")

	close(release)

	for it := e.databases.ListDatabaseOperations(t.Context(), &databasepb.ListDatabaseOperationsRequest{
		Parent: bkInst, Filter: "metadata.@type:OptimizeRestoredDatabaseMetadata",
	}); ; {
		o, nerr := it.Next()
		if errors.Is(nerr, iterator.Done) {
			break
		}

		require.NoError(t, nerr)

		done, werr := e.databases.LROClient.WaitOperation(
			t.Context(),
			&longrunningpb.WaitOperationRequest{Name: o.GetName()},
		)
		require.NoError(t, werr)
		assert.True(t, done.GetDone())
	}

	after, err := e.databases.GetDatabase(t.Context(), &databasepb.GetDatabaseRequest{Name: restored})
	require.NoError(t, err)
	assert.Equal(t, databasepb.Database_READY, after.GetState())
	assert.Empty(t, e.getBackup(t, src.GetName()).GetReferencingDatabases())

	require.NoError(t, e.databases.DeleteBackup(t.Context(), &databasepb.DeleteBackupRequest{Name: src.GetName()}))

	_, err = e.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: src.GetName()})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func TestBackup_UpdateExpireTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(e *bkEnv, r *databasepb.UpdateBackupRequest)
		name   string
		want   codes.Code
	}{
		{func(*bkEnv, *databasepb.UpdateBackupRequest) {}, "ok", codes.OK},
		{func(_ *bkEnv, r *databasepb.UpdateBackupRequest) { r.UpdateMask = nil }, "no mask", codes.InvalidArgument},
		{func(_ *bkEnv, r *databasepb.UpdateBackupRequest) {
			r.UpdateMask = &fieldmaskpb.FieldMask{Paths: []string{"database"}}
		}, "immutable field", codes.InvalidArgument},
		{
			func(e *bkEnv, r *databasepb.UpdateBackupRequest) { r.Backup.ExpireTime = e.expire(time.Hour) },
			"too soon",
			codes.InvalidArgument,
		},
		{func(e *bkEnv, r *databasepb.UpdateBackupRequest) {
			r.Backup.ExpireTime = e.expire(400 * 24 * time.Hour)
		}, "past the max expire time", codes.InvalidArgument},
		{
			func(_ *bkEnv, r *databasepb.UpdateBackupRequest) { r.Backup.Name = bkInst + "/backups/nope" },
			"unknown",
			codes.NotFound,
		},
		{
			func(_ *bkEnv, r *databasepb.UpdateBackupRequest) { r.Backup.Name = "bad" },
			"bad name",
			codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newBackupEnv(t)
			e.clock.Advance(2 * time.Hour)

			bk := e.createBackup(t, "b1", 48*time.Hour)
			req := &databasepb.UpdateBackupRequest{
				Backup:     &databasepb.Backup{Name: bk.GetName(), ExpireTime: e.expire(96 * time.Hour)},
				UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"expire_time"}},
			}
			tt.mutate(e, req)

			got, err := e.databases.UpdateBackup(t.Context(), req)
			require.Equal(t, tt.want, status.Code(err), "%v", err)

			if tt.want == codes.OK {
				assert.True(t, e.clock.Now().Add(96*time.Hour).Equal(got.GetExpireTime().AsTime()))
				assert.True(
					t,
					e.clock.Now().Add(96*time.Hour).Equal(e.getBackup(t, bk.GetName()).GetExpireTime().AsTime()),
				)
			}
		})
	}
}

func TestBackup_ListFilters(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.clock.Advance(2 * time.Hour)

	e.createBackup(t, "howl", 48*time.Hour)
	e.clock.Advance(time.Hour)
	e.createBackup(t, "daily-1", 72*time.Hour)

	cutoff := e.clock.Now().Add(-30 * time.Minute).UTC().Format(time.RFC3339)

	tests := []struct {
		name   string
		filter string
		want   []string
		code   codes.Code
	}{
		{"all", "", []string{"daily-1", "howl"}, codes.OK},
		{"name contains", "name:Howl", []string{"howl"}, codes.OK},
		{"state", "state:READY", []string{"daily-1", "howl"}, codes.OK},
		{"state none", "state:CREATING", nil, codes.OK},
		{"database", "database:app", []string{"daily-1", "howl"}, codes.OK},
		{"created before", "create_time < \"" + cutoff + "\"", []string{"howl"}, codes.OK},
		{
			"expires before",
			"expire_time < \"" + e.clock.Now().Add(48*time.Hour).UTC().Format(time.RFC3339) + "\"",
			[]string{"howl"},
			codes.OK,
		},
		{"size", "size_bytes > 1", []string{"daily-1", "howl"}, codes.OK},
		{"size none", "size_bytes > 1000000000", nil, codes.OK},
		{"and not", "(name:daily) AND NOT (name:howl)", []string{"daily-1"}, codes.OK},
		{"or", "name:howl OR name:daily", []string{"daily-1", "howl"}, codes.OK},
		{"bad filter", "(", nil, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.databases.ListBackups(
				t.Context(),
				&databasepb.ListBackupsRequest{Parent: bkInst, Filter: tt.filter},
			)

			var got []string

			for {
				bk, err := it.Next()
				if errors.Is(err, iterator.Done) {
					break
				}

				if err != nil {
					require.Equal(t, tt.code, status.Code(err))

					return
				}

				got = append(got, bk.GetName()[len(bkInst+"/backups/"):])
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, got)
		})
	}
}

func TestBackup_ListPaging(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.clock.Advance(2 * time.Hour)

	for _, id := range []string{"b1", "b2", "b3"} {
		e.createBackup(t, id, 48*time.Hour)
	}

	resp, err := gcpspannerListBackups(t, e, &databasepb.ListBackupsRequest{Parent: bkInst, PageSize: 2})
	require.NoError(t, err)
	require.Len(t, resp.GetBackups(), 2)
	require.NotEmpty(t, resp.GetNextPageToken())

	next, err := gcpspannerListBackups(t, e, &databasepb.ListBackupsRequest{
		Parent: bkInst, PageSize: 2, PageToken: resp.GetNextPageToken(),
	})
	require.NoError(t, err)
	require.Len(t, next.GetBackups(), 1)
	assert.Empty(t, next.GetNextPageToken())

	_, err = gcpspannerListBackups(t, e, &databasepb.ListBackupsRequest{Parent: bkInst, PageToken: "!"})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = gcpspannerListBackups(t, e, &databasepb.ListBackupsRequest{Parent: bkInst, PageSize: -1})
	assert.Equal(t, codes.InvalidArgument, status.Code(err))

	_, err = gcpspannerListBackups(t, e, &databasepb.ListBackupsRequest{Parent: parent + "/instances/none"})
	assert.Equal(t, codes.NotFound, status.Code(err))
}

func gcpspannerListBackups(
	t *testing.T, e *bkEnv, req *databasepb.ListBackupsRequest,
) (*databasepb.ListBackupsResponse, error) {
	t.Helper()

	return e.handler.API().Databases.ListBackups(context.Background(), req)
}

func TestBackup_CopyAcrossInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(e *bkEnv, r *databasepb.CopyBackupRequest)
		name   string
		want   codes.Code
	}{
		{func(*bkEnv, *databasepb.CopyBackupRequest) {}, "ok", codes.OK},
		{func(_ *bkEnv, r *databasepb.CopyBackupRequest) { r.Parent = bkInst }, "same instance", codes.OK},
		{func(_ *bkEnv, r *databasepb.CopyBackupRequest) { r.BackupId = "B" }, "bad id", codes.InvalidArgument},
		{
			func(_ *bkEnv, r *databasepb.CopyBackupRequest) { r.SourceBackup = bkInst + "/backups/nope" },
			"no source",
			codes.NotFound,
		},
		{
			func(_ *bkEnv, r *databasepb.CopyBackupRequest) { r.SourceBackup = "bad" },
			"bad source",
			codes.InvalidArgument,
		},
		{
			func(_ *bkEnv, r *databasepb.CopyBackupRequest) { r.Parent = parent + "/instances/none" },
			"no destination",
			codes.NotFound,
		},
		{func(_ *bkEnv, r *databasepb.CopyBackupRequest) {
			r.SourceBackup = "projects/other/instances/bk/backups/b1"
		}, "other project", codes.InvalidArgument},
		{
			func(e *bkEnv, r *databasepb.CopyBackupRequest) { r.ExpireTime = e.expire(time.Hour) },
			"too soon",
			codes.InvalidArgument,
		},
		{
			func(e *bkEnv, r *databasepb.CopyBackupRequest) { r.ExpireTime = e.expire(400 * 24 * time.Hour) },
			"too late",
			codes.InvalidArgument,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newBackupEnv(t)
			e.createInstance(t, "bk2")
			e.clock.Advance(2 * time.Hour)

			src := e.createBackup(t, "b1", 48*time.Hour)
			req := &databasepb.CopyBackupRequest{
				Parent: parent + "/instances/bk2", BackupId: "copy1", SourceBackup: src.GetName(),
				ExpireTime: e.expire(96 * time.Hour),
			}
			tt.mutate(e, req)

			op, err := e.databases.CopyBackup(t.Context(), req)
			require.Equal(t, tt.want, status.Code(err), "%v", err)

			if tt.want != codes.OK {
				return
			}

			got, err := op.Wait(t.Context())
			require.NoError(t, err)

			assert.Equal(t, req.GetParent()+"/backups/copy1", got.GetName())
			assert.Equal(t, databasepb.Backup_READY, got.GetState())
			assert.Equal(t, src.GetDatabase(), got.GetDatabase())
			assert.True(
				t,
				src.GetVersionTime().AsTime().Equal(got.GetVersionTime().AsTime()),
				"version_time is the source's",
			)
			assert.Equal(t, src.GetSizeBytes(), got.GetSizeBytes())
			assert.True(t, req.GetExpireTime().AsTime().Equal(got.GetExpireTime().AsTime()))
			assert.Empty(
				t,
				e.getBackup(t, src.GetName()).GetReferencingBackups(),
				"the copy reference ends with the copy",
			)

			meta, err := op.Metadata()
			require.NoError(t, err)
			assert.Equal(t, src.GetName(), meta.GetSourceBackup())
		})
	}
}

func TestBackup_ListBackupOperations(t *testing.T) {
	t.Parallel()

	const typePrefix = "type.googleapis.com/google.spanner.admin.database.v1."

	e := newBackupEnv(t)
	e.createInstance(t, "bk2")
	e.clock.Advance(2 * time.Hour)

	src := e.createBackup(t, "b1", 48*time.Hour)

	op, err := e.databases.CopyBackup(t.Context(), &databasepb.CopyBackupRequest{
		Parent:       parent + "/instances/bk2",
		BackupId:     "c1",
		SourceBackup: src.GetName(),
		ExpireTime:   e.expire(96 * time.Hour),
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	tests := []struct {
		name   string
		parent string
		filter string
		want   int
		code   codes.Code
	}{
		{"create", bkInst, "", 1, codes.OK},
		{"copy", parent + "/instances/bk2", "", 1, codes.OK},
		{"type", bkInst, "metadata.@type=" + typePrefix + "CreateBackupMetadata", 1, codes.OK},
		{"other type", bkInst, "metadata.@type=" + typePrefix + "CopyBackupMetadata", 0, codes.OK},
		{
			"database",
			bkInst,
			"(metadata.@type=" + typePrefix + "CreateBackupMetadata) AND metadata.database:app",
			1,
			codes.OK,
		},
		{"done", bkInst, "done:true", 1, codes.OK},
		{"not done", bkInst, "done:false", 0, codes.OK},
		{"no instance", parent + "/instances/none", "", 0, codes.NotFound},
		{"bad filter", bkInst, "(", 0, codes.InvalidArgument},
		{"bad parent", "garbage", "", 0, codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			it := e.databases.ListBackupOperations(t.Context(), &databasepb.ListBackupOperationsRequest{
				Parent: tt.parent, Filter: tt.filter,
			})

			n := 0

			for {
				_, nerr := it.Next()
				if errors.Is(nerr, iterator.Done) {
					break
				}

				if nerr != nil {
					require.Equal(t, tt.code, status.Code(nerr))

					return
				}

				n++
			}

			require.Equal(t, codes.OK, tt.code)
			assert.Equal(t, tt.want, n)
		})
	}
}

func TestBackup_RestoreValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		mutate func(r *databasepb.RestoreDatabaseRequest)
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
			r.Source = &databasepb.RestoreDatabaseRequest_Backup{Backup: bkInst + "/backups/missing"}
		}, "unknown backup", codes.NotFound},
		{func(r *databasepb.RestoreDatabaseRequest) {
			r.EncryptionConfig = &databasepb.RestoreDatabaseEncryptionConfig{KmsKeyName: "k"}
		}, "key without type", codes.InvalidArgument},
		{func(r *databasepb.RestoreDatabaseRequest) {
			r.Source = &databasepb.RestoreDatabaseRequest_Backup{Backup: "projects/other/instances/bk/backups/b1"}
		}, "other project", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newBackupEnv(t)
			e.clock.Advance(2 * time.Hour)

			bk := e.createBackup(t, "b1", 48*time.Hour)
			req := &databasepb.RestoreDatabaseRequest{
				Parent: bkInst, DatabaseId: "restored",
				Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: bk.GetName()},
			}
			tt.mutate(req)

			op, err := e.databases.RestoreDatabase(t.Context(), req)
			require.Equal(t, tt.want, status.Code(err), "%v", err)

			if tt.want == codes.OK {
				_, err = op.Wait(t.Context())
				require.NoError(t, err)
			}
		})
	}
}

func TestBackup_RestoreWithoutDataPlaneKeepsRowsForLater(t *testing.T) {
	t.Parallel()

	src := newBackupEnv(t)
	src.clock.Advance(2 * time.Hour)
	bk := src.createBackup(t, "b1", 48*time.Hour)

	snap := src.handler.Snapshot(t.Context())

	fake, addr := newFakeEmulator(t)
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseNoRuntime() })
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	op, err := dst.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: bkInst, DatabaseId: "later", Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: bk.GetName()},
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.NoError(t, err)

	snap2 := dst.handler.Snapshot(t.Context())

	live := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) { b.UseEmulatorAt(addr, nil) })
	require.NoError(t, live.handler.Restore(t.Context(), snap2))

	_, err = spannerClient(
		t,
		live,
	).CreateSession(t.Context(), &spannerpb.CreateSessionRequest{Database: bkInst + "/databases/later"})
	require.NoError(t, err)

	var rows int

	for _, c := range fake.committed() {
		if c.db == bkInst+"/databases/later" {
			rows += len(c.rows)
		}
	}

	assert.Equal(t, 2, rows, "rows restored while no emulator ran are loaded when one starts")
}

func TestBackup_PersistsAcrossSnapshot(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)
	e.createInstance(t, "bk2")
	e.clock.Advance(2 * time.Hour)

	release := make(chan struct{})
	e.handler.Backend.SetOptimizeHook(func() { <-release })
	t.Cleanup(func() { close(release) })

	b1 := e.createBackup(t, "b1", 48*time.Hour)

	cp, err := e.databases.CopyBackup(t.Context(), &databasepb.CopyBackupRequest{
		Parent:       parent + "/instances/bk2",
		BackupId:     "c1",
		SourceBackup: b1.GetName(),
		ExpireTime:   e.expire(96 * time.Hour),
	})
	require.NoError(t, err)

	c1, err := cp.Wait(t.Context())
	require.NoError(t, err)

	rop, err := e.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: bkInst, DatabaseId: "restored", Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: b1.GetName()},
	})
	require.NoError(t, err)

	_, err = rop.Wait(t.Context())
	require.NoError(t, err)

	pending := make(chan struct{})
	started := make(chan struct{})
	e.handler.Backend.SetBackupHook(func() {
		close(started)
		<-pending
	})

	pop, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
		Parent:   bkInst,
		BackupId: "pending",
		Backup:   &databasepb.Backup{Database: bkDB, ExpireTime: e.expire(48 * time.Hour)},
	})
	require.NoError(t, err)

	<-started

	snap := e.handler.Snapshot(t.Context())

	close(pending)

	_, err = pop.Wait(t.Context())
	require.NoError(t, err)

	fake, addr := newFakeEmulator(t)
	dstRelease := make(chan struct{})
	dst := newEnvWith(t, func(b *gcpspanner.InMemoryBackend) {
		b.SetClock(e.clock.Now)
		b.UseEmulatorAt(addr, nil)
		b.SetOptimizeHook(func() { <-dstRelease })
	})
	require.NoError(t, dst.handler.Restore(t.Context(), snap))

	got1 := (&bkEnv{env: dst, fake: fake, clock: e.clock}).getBackup(t, b1.GetName())
	want1 := e.getBackup(t, b1.GetName())
	assert.True(t, proto.Equal(want1, got1) || sameBackup(want1, got1), "backup survives the snapshot")
	assert.Equal(t, []string{bkInst + "/databases/restored"}, got1.GetReferencingDatabases(), "references survive")
	assert.Positive(t, got1.GetSizeBytes())

	gotC1, err := dst.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: c1.GetName()})
	require.NoError(t, err)
	assert.True(t, c1.GetVersionTime().AsTime().Equal(gotC1.GetVersionTime().AsTime()))
	assert.True(t, c1.GetExpireTime().AsTime().Equal(gotC1.GetExpireTime().AsTime()))
	assert.Equal(t, c1.GetSizeBytes(), gotC1.GetSizeBytes())

	_, err = dst.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: bkInst + "/backups/pending"})
	assert.Equal(t, codes.NotFound, status.Code(err), "a backup caught mid-creation is dropped")

	pendingOp := findBackupOp(t, dst, "pending")
	assert.True(t, pendingOp.GetDone())
	assert.Equal(t, int32(codes.Aborted), pendingOp.GetError().GetCode(), "its operation is aborted, not left running")

	db, err := dst.databases.GetDatabase(
		t.Context(),
		&databasepb.GetDatabaseRequest{Name: bkInst + "/databases/restored"},
	)
	require.NoError(t, err)
	assert.Equal(t, databasepb.Database_READY_OPTIMIZING, db.GetState())

	rop2, err := dst.databases.RestoreDatabase(t.Context(), &databasepb.RestoreDatabaseRequest{
		Parent: parent + "/instances/bk2", DatabaseId: "from-copy",
		Source: &databasepb.RestoreDatabaseRequest_Backup{Backup: c1.GetName()},
	})
	require.NoError(t, err)

	_, err = rop2.Wait(t.Context())
	require.NoError(t, err)

	rows := 0

	for _, c := range fake.committed() {
		if c.db == parent+"/instances/bk2/databases/from-copy" {
			rows += len(c.rows)
		}
	}

	assert.Equal(t, 2, rows, "the restored backup still holds its rows")

	close(dstRelease)
}

func sameBackup(a, b *databasepb.Backup) bool {
	return a.GetName() == b.GetName() && a.GetState() == b.GetState() && a.GetSizeBytes() == b.GetSizeBytes() &&
		a.GetVersionTime().AsTime().Equal(b.GetVersionTime().AsTime()) &&
		a.GetExpireTime().AsTime().Equal(b.GetExpireTime().AsTime()) &&
		a.GetCreateTime().AsTime().Equal(b.GetCreateTime().AsTime()) && a.GetDatabase() == b.GetDatabase()
}

func findBackupOp(t *testing.T, e *env, backup string) *longrunningpb.Operation {
	t.Helper()

	it := e.databases.ListBackupOperations(t.Context(), &databasepb.ListBackupOperationsRequest{
		Parent: bkInst, Filter: "metadata.name:" + backup,
	})

	op, err := it.Next()
	require.NoError(t, err)

	return op
}
