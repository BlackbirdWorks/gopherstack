package dynamodb_test

import (
	"testing"

	"Gopherstack/dynamodb"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStore_Operations(t *testing.T) {
	t.Parallel()
	db := dynamodb.NewInMemoryDB()

	// List empty
	tables := db.ListAllTables()
	assert.Empty(t, tables)

	// Create table
	createTableHelper(t, db, "Table1", "id")

	tables = db.ListAllTables()
	assert.Len(t, tables, 1)
	assert.Equal(t, "Table1", tables[0].Name)

	// Get table
	table, ok := db.GetTable("Table1")
	require.True(t, ok)
	assert.Equal(t, "Table1", table.Name)

	_, ok = db.GetTable("Missing")
	assert.False(t, ok)
}
