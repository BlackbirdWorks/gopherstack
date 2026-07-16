package glue_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/glue"
)

func TestRefinement1_BatchDeleteConnection_MixedResults(t *testing.T) {
	t.Parallel()

	b := glue.NewInMemoryBackend("000000000000", "us-east-1")
	b.AddConnectionInternal(&glue.Connection{Name: "conn1"})
	b.AddConnectionInternal(&glue.Connection{Name: "conn2"})

	succeeded, errs := b.BatchDeleteConnection([]string{"conn1", "missing", "conn2"})

	assert.Len(t, succeeded, 2)
	assert.Contains(t, succeeded, "conn1")
	assert.Contains(t, succeeded, "conn2")
	assert.Len(t, errs, 1)
	assert.Equal(t, "EntityNotFoundException", errs[0].ErrorCode)
	assert.Equal(t, 0, glue.ConnectionCount(b))
}

// TestRefinement2_ConnectionPersistence tests snapshot/restore for connections.
func TestRefinement2_ConnectionPersistence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		connProps map[string]string
		wantType  string
		wantProp  string
		wantKey   string
	}{
		{
			name:      "jdbc_connection_round_trip",
			connProps: map[string]string{"JDBC_CONNECTION_URL": "jdbc:mysql://localhost:3306"},
			wantType:  "JDBC",
			wantKey:   "JDBC_CONNECTION_URL",
			wantProp:  "jdbc:mysql://localhost:3306",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := glue.NewInMemoryBackend(testAccountID, testRegion)
			b.AddConnectionInternal(&glue.Connection{
				Name:                 "persist-conn",
				ConnectionType:       tt.wantType,
				ConnectionProperties: tt.connProps,
			})

			snap := b.Snapshot(t.Context())
			require.NotNil(t, snap)

			b2 := glue.NewInMemoryBackend(testAccountID, testRegion)
			require.NoError(t, b2.Restore(t.Context(), snap))
			assert.Equal(t, 1, glue.ConnectionCount(b2))

			conn, err := b2.GetConnection("persist-conn")
			require.NoError(t, err)
			assert.Equal(t, tt.wantType, conn.ConnectionType)
			assert.Equal(t, tt.wantProp, conn.ConnectionProperties[tt.wantKey])
		})
	}
}
