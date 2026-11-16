package gcpbigtable_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigtable"
	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

const (
	tableAdminInstance = "projects/" + project + "/instances/" + instance
	srcBackup          = tableAdminInstance + "/clusters/" + cluster + "/backups/src-b"
)

func TestCopyBackup(t *testing.T) {
	t.Parallel()

	in := func(d time.Duration) *timestamppb.Timestamp { return timestamppb.New(time.Now().Add(d)) }
	dst := tableAdminInstance + "/clusters/" + cluster

	tests := []struct {
		mutate func(*adminpb.CopyBackupRequest)
		name   string
		want   codes.Code
	}{
		{func(*adminpb.CopyBackupRequest) {}, "ok", codes.OK},
		{func(r *adminpb.CopyBackupRequest) { r.BackupId = "bad id!" }, "bad id", codes.InvalidArgument},
		{func(r *adminpb.CopyBackupRequest) { r.BackupId = "src-b" }, "duplicate", codes.AlreadyExists},
		{func(r *adminpb.CopyBackupRequest) { r.SourceBackup += "-missing" }, "unknown source", codes.NotFound},
		{func(r *adminpb.CopyBackupRequest) { r.Parent += "-missing" }, "unknown cluster", codes.NotFound},
		{func(r *adminpb.CopyBackupRequest) { r.ExpireTime = in(time.Hour) }, "expiry too soon", codes.InvalidArgument},
		{
			func(r *adminpb.CopyBackupRequest) { r.ExpireTime = in(31 * 24 * time.Hour) },
			"expiry too far",
			codes.InvalidArgument,
		},
		{func(r *adminpb.CopyBackupRequest) { r.ExpireTime = nil }, "no expiry", codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newEnv(t)
			tbl := e.seeded(t, "src", "cf")
			put(t, tbl, seedCell{"r1", "cf", "q", "v1", 1000})
			require.NoError(t, e.ac.CreateBackup(t.Context(), "src", cluster, "src-b", time.Now().Add(72*time.Hour)))

			tc := adminpb.NewBigtableTableAdminClient(e.conn)
			req := &adminpb.CopyBackupRequest{
				Parent: dst, BackupId: "copy-b", SourceBackup: srcBackup, ExpireTime: in(48 * time.Hour),
			}
			tt.mutate(req)

			op, err := tc.CopyBackup(t.Context(), req)
			require.Equal(t, tt.want, code(err))

			if tt.want != codes.OK {
				return
			}

			bk, err := tc.GetBackup(t.Context(), &adminpb.GetBackupRequest{Name: dst + "/backups/copy-b"})
			require.NoError(t, err)
			assert.Equal(t, srcBackup, bk.GetSourceBackup())
			assert.Equal(t, adminpb.Backup_READY, bk.GetState())
			assert.Equal(t, tableAdminInstance+"/tables/src", bk.GetSourceTable())
			assert.Positive(t, bk.GetSizeBytes())
			assert.True(t, op.GetDone())
			assert.NotNil(t, op.GetMetadata())

			_, err = tc.RestoreTable(t.Context(), &adminpb.RestoreTableRequest{
				Parent: tableAdminInstance, TableId: "from-copy",
				Source: &adminpb.RestoreTableRequest_Backup{Backup: bk.GetName()},
			})
			require.NoError(t, err)

			row, err := e.client(t).Open("from-copy").ReadRow(t.Context(), "r1")
			require.NoError(t, err)
			require.Len(t, row["cf"], 1)
			assert.Equal(t, "v1", string(row["cf"][0].Value))

			again, err := tc.CopyBackup(t.Context(), &adminpb.CopyBackupRequest{
				Parent: dst, BackupId: "copy-of-copy", SourceBackup: bk.GetName(), ExpireTime: in(48 * time.Hour),
			})
			assert.Equal(t, codes.InvalidArgument, code(err), "copying a copied backup")
			assert.Nil(t, again)
		})
	}
}

func TestCopyBackupAcrossInstances(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "src", "cf")
	put(t, tbl, seedCell{"r1", "cf", "q", "v1", 1000})
	require.NoError(t, e.ac.CreateBackup(t.Context(), "src", cluster, "src-b", time.Now().Add(72*time.Hour)))
	require.NoError(t, e.iac.CreateInstance(t.Context(), &bigtable.InstanceConf{
		InstanceId:   "other",
		DisplayName:  "other",
		ClusterId:    "oc",
		Zone:         zone,
		NumNodes:     1,
		InstanceType: bigtable.PRODUCTION,
	}))

	tc := adminpb.NewBigtableTableAdminClient(e.conn)
	parent := "projects/" + project + "/instances/other/clusters/oc"

	_, err := tc.CopyBackup(t.Context(), &adminpb.CopyBackupRequest{
		Parent:       parent,
		BackupId:     "x",
		SourceBackup: srcBackup,
		ExpireTime:   timestamppb.New(time.Now().Add(48 * time.Hour)),
	})
	require.NoError(t, err)

	got, err := tc.GetBackup(t.Context(), &adminpb.GetBackupRequest{Name: parent + "/backups/x"})
	require.NoError(t, err)
	assert.Equal(t, srcBackup, got.GetSourceBackup())

	lst, err := tc.ListBackups(t.Context(), &adminpb.ListBackupsRequest{Parent: parent})
	require.NoError(t, err)
	assert.Len(t, lst.GetBackups(), 1)
}

func TestCopyBackupPersists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "src", "cf")
	require.NoError(t, e.ac.CreateBackup(t.Context(), "src", cluster, "src-b", time.Now().Add(72*time.Hour)))

	tc := adminpb.NewBigtableTableAdminClient(e.conn)
	dst := tableAdminInstance + "/clusters/" + cluster

	_, err := tc.CopyBackup(t.Context(), &adminpb.CopyBackupRequest{
		Parent:       dst,
		BackupId:     "copy-b",
		SourceBackup: srcBackup,
		ExpireTime:   timestamppb.New(time.Now().Add(48 * time.Hour)),
	})
	require.NoError(t, err)

	dstEnv := newEnv(t)
	require.NoError(t, dstEnv.handler.Restore(t.Context(), e.handler.Snapshot(t.Context())))

	got, err := adminpb.NewBigtableTableAdminClient(dstEnv.conn).
		GetBackup(t.Context(), &adminpb.GetBackupRequest{Name: dst + "/backups/copy-b"})
	require.NoError(t, err)
	assert.Equal(t, srcBackup, got.GetSourceBackup())
}

func TestUndeleteTable(t *testing.T) {
	t.Parallel()

	const week = 7 * 24 * time.Hour

	tests := []struct {
		setup func(t *testing.T, e *env, now *time.Time)
		name  string
		want  codes.Code
	}{
		{nil, "ok", codes.OK},
		{
			func(_ *testing.T, _ *env, now *time.Time) { *now = now.Add(week - time.Minute) },
			"just inside the window",
			codes.OK,
		},
		{
			func(_ *testing.T, _ *env, now *time.Time) { *now = now.Add(week + time.Minute) },
			"past the window",
			codes.NotFound,
		},
		{func(t *testing.T, e *env, _ *time.Time) {
			t.Helper()
			require.NoError(t, e.ac.CreateTableFromConf(t.Context(), tableConf("t", "cf")))
		}, "name taken", codes.AlreadyExists},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			b := gcpbigtable.NewInMemoryBackend()
			b.SetClock(func() time.Time { return now })

			e := newEnvWithBackend(t, b)
			tbl := e.seeded(t, "t", "cf")
			put(t, tbl, seedCell{"r1", "cf", "q", "v1", 1000})

			tc := adminpb.NewBigtableTableAdminClient(e.conn)
			path := tableAdminInstance + "/tables/t"

			_, err := tc.SetIamPolicy(t.Context(), &iampb.SetIamPolicyRequest{
				Resource: path,
				Policy: &iampb.Policy{
					Bindings: []*iampb.Binding{{Role: "roles/bigtable.reader", Members: []string{"user:a@b.c"}}},
				},
			})
			require.NoError(t, err)
			require.NoError(t, e.ac.DeleteTable(t.Context(), "t"))

			if tt.setup != nil {
				tt.setup(t, e, &now)
			}

			op, err := tc.UndeleteTable(t.Context(), &adminpb.UndeleteTableRequest{Name: path})
			require.Equal(t, tt.want, code(err))

			if tt.want != codes.OK {
				return
			}

			assert.True(t, op.GetDone())

			got, err := tc.GetTable(t.Context(), &adminpb.GetTableRequest{Name: path})
			require.NoError(t, err)
			assert.True(t, got.GetDeletionProtection(), "an undeleted table has deletion protection enabled")

			row, err := e.client(t).Open("t").ReadRow(t.Context(), "r1")
			require.NoError(t, err)
			assert.Len(t, row["cf"], 1, "data comes back")

			pol, err := tc.GetIamPolicy(t.Context(), &iampb.GetIamPolicyRequest{Resource: path})
			require.NoError(t, err)
			assert.Empty(t, pol.GetBindings(), "IAM bindings are not restored")
		})
	}
}

func TestUndeleteTableRejections(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "t", "cf")

	tc := adminpb.NewBigtableTableAdminClient(e.conn)

	for name, table := range map[string]string{
		"never existed": tableAdminInstance + "/tables/none", "exists": tableAdminInstance + "/tables/t",
	} {
		_, err := tc.UndeleteTable(t.Context(), &adminpb.UndeleteTableRequest{Name: table})
		assert.Contains(t, []codes.Code{codes.NotFound, codes.AlreadyExists}, code(err), name)
		assert.NotEqual(t, codes.OK, code(err), name)
	}

	_, err := tc.UndeleteTable(t.Context(), &adminpb.UndeleteTableRequest{Name: "garbage"})
	assert.Equal(t, codes.InvalidArgument, code(err))

	_, err = tc.UndeleteTable(t.Context(), &adminpb.UndeleteTableRequest{Name: "projects/p/instances/none/tables/t"})
	assert.Equal(t, codes.NotFound, code(err))
}

func TestUndeleteTablePersists(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "t", "cf")
	put(t, tbl, seedCell{"r1", "cf", "q", "v1", 1000})
	require.NoError(t, e.ac.DeleteTable(t.Context(), "t"))

	dst := newEnv(t)
	require.NoError(t, dst.handler.Restore(t.Context(), e.handler.Snapshot(t.Context())))

	_, err := adminpb.NewBigtableTableAdminClient(dst.conn).
		UndeleteTable(t.Context(), &adminpb.UndeleteTableRequest{Name: tableAdminInstance + "/tables/t"})
	require.NoError(t, err)

	row, err := dst.client(t).Open("t").ReadRow(t.Context(), "r1")
	require.NoError(t, err)
	assert.Len(t, row["cf"], 1)
}
