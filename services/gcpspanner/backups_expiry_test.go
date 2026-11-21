package gcpspanner_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/structpb"
)

func TestBackup_ExpiredBackupsAreDeleted(t *testing.T) {
	t.Parallel()

	tests := []struct {
		access func(t *testing.T, e *bkEnv, name string) error
		name   string
	}{
		{name: "get", access: func(t *testing.T, e *bkEnv, name string) error {
			t.Helper()

			_, err := e.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: name})

			return err
		}},
		{name: "delete", access: func(t *testing.T, e *bkEnv, name string) error {
			t.Helper()

			return e.databases.DeleteBackup(t.Context(), &databasepb.DeleteBackupRequest{Name: name})
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := newBackupEnv(t)
			keep := e.createBackup(t, "keep", 96*time.Hour)
			gone := e.createBackup(t, "gone", 12*time.Hour)

			e.clock.Advance(13 * time.Hour)

			require.Equal(t, codes.NotFound, status.Code(tt.access(t, e, gone.GetName())))

			list := e.databases.ListBackups(t.Context(), &databasepb.ListBackupsRequest{Parent: bkInst})
			var names []string

			for {
				b, err := list.Next()
				if err != nil {
					break
				}

				names = append(names, b.GetName())
			}

			assert.Equal(t, []string{keep.GetName()}, names)
		})
	}
}

func TestBackup_SizeCapStopsCaptureEarly(t *testing.T) {
	t.Parallel()

	e := newBackupEnv(t)

	big := make([][]*structpb.Value, 200)
	for i := range big {
		big[i] = []*structpb.Value{sv("1"), sv(strings.Repeat("x", 40))}
	}

	e.fake.seed(bkDB,
		fakeTable{name: "A", columns: []string{"id", "name"}, rows: big},
		fakeTable{name: "B", columns: []string{"id", "name"}, rows: bkTables()[0].rows},
		fakeTable{name: "C", columns: []string{"id", "name"}, rows: bkTables()[0].rows},
	)
	e.handler.Backend.SetBackupCap(400)

	op, err := e.databases.CreateBackup(t.Context(), &databasepb.CreateBackupRequest{
		Parent: bkInst, BackupId: "big",
		Backup: &databasepb.Backup{Database: bkDB, ExpireTime: e.expire(48 * time.Hour)},
	})
	require.NoError(t, err)

	_, err = op.Wait(t.Context())
	require.Error(t, err)
	assert.Equal(t, codes.ResourceExhausted, status.Code(err))

	assert.NotContains(t, e.fake.tablesRead(), "B", "capture stops at the table that exceeds the cap")
	assert.NotContains(t, e.fake.tablesRead(), "C")

	_, err = e.databases.GetBackup(t.Context(), &databasepb.GetBackupRequest{Name: bkInst + "/backups/big"})
	assert.Equal(t, codes.NotFound, status.Code(err), "the partial backup is removed")
}
