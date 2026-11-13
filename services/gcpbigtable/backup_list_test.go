package gcpbigtable_test

import (
	"testing"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func TestListBackupsFilterAndOrder(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	tbl := e.seeded(t, "src", "cf")
	put(t, tbl, seedCell{"r1", "cf", "q", "v1", 1000})
	require.NoError(t, e.ac.CreateTableFromConf(t.Context(), tableConf("other", "cf")))

	ctx := t.Context()
	now := time.Now()

	require.NoError(t, e.ac.CreateBackup(ctx, "src", cluster, "alpha", now.Add(72*time.Hour)))
	require.NoError(t, e.ac.CreateBackup(ctx, "src", cluster, "beta", now.Add(48*time.Hour)))
	require.NoError(t, e.ac.CreateBackup(ctx, "other", cluster, "gamma", now.Add(96*time.Hour)))

	tc := adminpb.NewBigtableTableAdminClient(e.conn)
	parent := "projects/" + project + "/instances/" + instance + "/clusters/" + cluster

	tests := []struct {
		name    string
		filter  string
		orderBy string
		want    []string
		code    codes.Code
	}{
		{name: "default newest first", want: []string{"gamma", "beta", "alpha"}},
		{name: "order name", orderBy: "name", want: []string{"alpha", "beta", "gamma"}},
		{name: "order expire desc", orderBy: "expire_time desc", want: []string{"gamma", "alpha", "beta"}},
		{name: "order expire", orderBy: "expire_time", want: []string{"beta", "alpha", "gamma"}},
		{name: "name contains", filter: "name:alp", orderBy: "name", want: []string{"alpha"}},
		{name: "name quoted", filter: `name:"eta"`, want: []string{"beta"}},
		{name: "source table", filter: "source_table:other", want: []string{"gamma"}},
		{name: "state", filter: "state:READY", orderBy: "name", want: []string{"alpha", "beta", "gamma"}},
		{name: "state none", filter: "state:CREATING", want: nil},
		{name: "size", filter: "size_bytes > 0 AND source_table:src", orderBy: "name", want: []string{"alpha", "beta"}},
		{name: "implicit and", filter: "(name:a) (source_table:src)", orderBy: "name", want: []string{"alpha", "beta"}},
		{name: "or", filter: "name:alpha OR name:gamma", orderBy: "name", want: []string{"alpha", "gamma"}},
		{name: "not", filter: "NOT name:alpha", orderBy: "name", want: []string{"beta", "gamma"}},
		{name: "minus", filter: "-name:alpha", orderBy: "name", want: []string{"beta", "gamma"}},
		{
			name:    "expire before",
			filter:  `expire_time < "` + now.Add(60*time.Hour).UTC().Format(time.RFC3339) + `"`,
			orderBy: "name", want: []string{"beta"},
		},
		{name: "start in future", filter: `start_time > "2999-01-01T00:00:00Z"`, want: nil},
		{name: "bad field", filter: "colour:red", code: codes.InvalidArgument},
		{name: "bad syntax", filter: "name:", code: codes.InvalidArgument},
		{name: "bare word", filter: "x", code: codes.InvalidArgument},
		{name: "bad time", filter: "start_time > yesterday", code: codes.InvalidArgument},
		{name: "bad state", filter: "state:BOGUS", code: codes.InvalidArgument},
		{name: "bad order", orderBy: "colour", code: codes.InvalidArgument},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			resp, err := tc.ListBackups(t.Context(), &adminpb.ListBackupsRequest{
				Parent: parent, Filter: tt.filter, OrderBy: tt.orderBy,
			})
			if tt.code != codes.OK {
				assert.Equal(t, tt.code, code(err))

				return
			}

			require.NoError(t, err)

			var got []string
			for _, b := range resp.GetBackups() {
				got = append(got, lastPart(b.GetName()))
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func lastPart(s string) string {
	for i := len(s) - 1; i >= 0; i-- {
		if s[i] == '/' {
			return s[i+1:]
		}
	}

	return s
}

func TestHotBackupConversion(t *testing.T) {
	t.Parallel()

	e := newEnv(t)
	e.seeded(t, "src", "cf")

	tc := adminpb.NewBigtableTableAdminClient(e.conn)
	parent := "projects/" + project + "/instances/" + instance + "/clusters/" + cluster
	table := "projects/" + project + "/instances/" + instance + "/tables/src"
	expire := timestamppb.New(time.Now().Add(72 * time.Hour))

	create := func(id string, bk *adminpb.Backup) error {
		bk.SourceTable, bk.ExpireTime = table, expire
		_, err := tc.CreateBackup(t.Context(), &adminpb.CreateBackupRequest{Parent: parent, BackupId: id, Backup: bk})

		return err
	}

	require.NoError(t, create("hot", &adminpb.Backup{
		BackupType:        adminpb.Backup_HOT,
		HotToStandardTime: timestamppb.New(time.Now().Add(48 * time.Hour)),
	}))

	got, err := tc.GetBackup(t.Context(), &adminpb.GetBackupRequest{Name: parent + "/backups/hot"})
	require.NoError(t, err)
	assert.Equal(t, adminpb.Backup_HOT, got.GetBackupType())
	assert.NotNil(t, got.GetHotToStandardTime())

	assert.Equal(t, codes.InvalidArgument, code(create("std", &adminpb.Backup{
		HotToStandardTime: timestamppb.New(time.Now().Add(48 * time.Hour)),
	})), "standard backup")
	assert.Equal(t, codes.InvalidArgument, code(create("soon", &adminpb.Backup{
		BackupType:        adminpb.Backup_HOT,
		HotToStandardTime: timestamppb.New(time.Now().Add(time.Hour)),
	})), "under 24 hours")
}
