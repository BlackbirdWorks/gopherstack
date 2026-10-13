package rdsdata

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRestoreReplaysNamedDatabase(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		database string
		wantRows int
	}{
		{name: "named", database: "alpha", wantRows: 1},
		{name: "other", database: "beta", wantRows: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			src := NewInMemoryBackend(persistTestAccountID, persistTestRegion)
			t.Cleanup(src.Close)

			ctx := withRequestTarget(t.Context(), "s", "alpha")
			for _, stmt := range []string{"CREATE TABLE t (id INTEGER)", "INSERT INTO t (id) VALUES (1)"} {
				_, _, _, _, err := src.ExecuteStatement(ctx, "arn:c", stmt, "")
				require.NoError(t, err)
			}

			dst := NewInMemoryBackend(persistTestAccountID, persistTestRegion)
			t.Cleanup(dst.Close)
			require.NoError(t, dst.Restore(t.Context(), src.Snapshot(t.Context())))

			recs, _, _, _, err := dst.ExecuteStatement(
				withRequestTarget(t.Context(), "s", tt.database), "arn:c", "SELECT id FROM t", "")
			require.NoError(t, err)
			assert.Len(t, recs, tt.wantRows)
		})
	}
}
