package dynamodb_test

import (
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Operations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(*testing.T, *dynamodb.InMemoryDB)
		check func(*testing.T, *dynamodb.InMemoryDB)
		name  string
	}{
		{
			name: "ListEmpty",
			check: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				tables := db.ListAllTables()
				assert.Empty(t, tables)
			},
		},
		{
			name: "CreateTableAndList",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "id")
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				tables := db.ListAllTables()
				require.Len(t, tables, 1)
				assert.Equal(t, "Table1", tables[0].Name)
			},
		},
		{
			name: "GetTable",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "Table1", "id")
			},
			check: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				table, ok := db.GetTable("Table1")
				require.True(t, ok)
				assert.Equal(t, "Table1", table.Name)

				_, ok = db.GetTable("Missing")
				assert.False(t, ok)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			db := dynamodb.NewInMemoryDB()

			if tt.setup != nil {
				tt.setup(t, db)
			}

			if tt.check != nil {
				tt.check(t, db)
			}
		})
	}
}
