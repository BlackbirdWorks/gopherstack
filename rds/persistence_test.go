package rds_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/rds"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryBackend_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(b *rds.InMemoryBackend) string
		verify func(t *testing.T, b *rds.InMemoryBackend, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(b *rds.InMemoryBackend) string {
				inst, err := b.CreateDBInstance("test-db", "mysql", "db.t3.micro", "testdb", "admin", 20)
				if err != nil {
					return ""
				}

				return inst.DBInstanceIdentifier
			},
			verify: func(t *testing.T, b *rds.InMemoryBackend, id string) {
				t.Helper()

				instances, err := b.DescribeDBInstances(id)
				require.NoError(t, err)
				require.Len(t, instances, 1)
				assert.Equal(t, id, instances[0].DBInstanceIdentifier)
				assert.Equal(t, "mysql", instances[0].Engine)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *rds.InMemoryBackend) string { return "" },
			verify: func(t *testing.T, b *rds.InMemoryBackend, _ string) {
				t.Helper()

				instances, err := b.DescribeDBInstances("")
				require.NoError(t, err)
				assert.Empty(t, instances)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := rds.NewInMemoryBackend("000000000000", "us-east-1")
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := rds.NewInMemoryBackend("000000000000", "us-east-1")
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryBackend_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	b := rds.NewInMemoryBackend("000000000000", "us-east-1")
	err := b.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
