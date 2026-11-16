package gcpbigtable_test

import (
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"

	"github.com/blackbirdworks/gopherstack/services/gcpbigtable"
)

func TestUndeleteTable_RetainedDataIsBounded(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want map[string]codes.Code
		name string
		size int
	}{
		{map[string]codes.Code{"a": codes.OK, "b": codes.OK, "c": codes.OK}, "fits", 1000},
		{map[string]codes.Code{"a": codes.NotFound, "b": codes.OK, "c": codes.OK}, "oldest evicted", 2000},
		{
			map[string]codes.Code{"a": codes.NotFound, "b": codes.NotFound, "c": codes.NotFound},
			"too large to keep", 6000,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			now := time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)
			b := gcpbigtable.NewInMemoryBackend()
			b.SetClock(func() time.Time { return now })
			b.SetDeletedTableCap(5000)

			e := newEnvWithBackend(t, b)
			e.createInstance(t, instance)

			for _, id := range []string{"a", "b", "c"} {
				require.NoError(t, e.ac.CreateTableFromConf(t.Context(), tableConf(id, "cf")))
				put(t, e.client(t).Open(id), seedCell{"r", "cf", "q", strings.Repeat("x", tt.size), 1000})
				require.NoError(t, e.ac.DeleteTable(t.Context(), id))

				now = now.Add(time.Minute)
			}

			tc := adminpb.NewBigtableTableAdminClient(e.conn)

			for id, want := range tt.want {
				_, err := tc.UndeleteTable(
					t.Context(),
					&adminpb.UndeleteTableRequest{Name: tableAdminInstance + "/tables/" + id},
				)
				assert.Equal(t, want, code(err), id)
			}
		})
	}
}
