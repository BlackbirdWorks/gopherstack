package dynamodb_test

import (
	"context"
	"testing"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/dynamodb/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInMemoryDB_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(db *dynamodb.InMemoryDB) string
		verify func(t *testing.T, db *dynamodb.InMemoryDB, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(db *dynamodb.InMemoryDB) string {
				input := models.ToSDKCreateTableInput(&models.CreateTableInput{
					TableName: "test-table",
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "id", KeyType: models.KeyTypeHash},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "id", AttributeType: "S"},
					},
				})
				_, err := db.CreateTable(context.Background(), input)
				if err != nil {
					return ""
				}

				return "test-table"
			},
			verify: func(t *testing.T, db *dynamodb.InMemoryDB, id string) {
				t.Helper()

				table, ok := db.GetTable(id)
				require.True(t, ok)
				assert.Equal(t, id, table.Name)
			},
		},
		{
			name:  "empty_backend_round_trip",
			setup: func(_ *dynamodb.InMemoryDB) string { return "" },
			verify: func(t *testing.T, db *dynamodb.InMemoryDB, _ string) {
				t.Helper()

				tables := db.ListAllTables()
				assert.Empty(t, tables)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := dynamodb.NewInMemoryDB()
			id := tt.setup(original)

			snap := original.Snapshot()
			require.NotNil(t, snap)

			fresh := dynamodb.NewInMemoryDB()
			require.NoError(t, fresh.Restore(snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryDB_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	err := db.Restore([]byte("not-valid-json"))
	require.Error(t, err)
}
