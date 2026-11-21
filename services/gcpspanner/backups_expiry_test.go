package gcpspanner_test

import (
	"testing"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
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
