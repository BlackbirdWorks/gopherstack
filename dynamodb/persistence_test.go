package dynamodb_test

import (
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
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

func TestDynamoDBHandler_Persistence(t *testing.T) {
t.Parallel()

db := dynamodb.NewInMemoryDB()
h := dynamodb.NewHandler(db, slog.Default())

// Create a table via the DB
input := models.ToSDKCreateTableInput(&models.CreateTableInput{
TableName: "handler-test-table",
KeySchema: []models.KeySchemaElement{
{AttributeName: "id", KeyType: models.KeyTypeHash},
},
AttributeDefinitions: []models.AttributeDefinition{
{AttributeName: "id", AttributeType: "S"},
},
})
_, err := db.CreateTable(t.Context(), input)
require.NoError(t, err)

snap := h.Snapshot()
require.NotNil(t, snap)

freshDB := dynamodb.NewInMemoryDB()
freshH := dynamodb.NewHandler(freshDB, slog.Default())
require.NoError(t, freshH.Restore(snap))

tables := freshDB.ListAllTables()
require.Len(t, tables, 1)
	assert.Equal(t, "handler-test-table", tables[0].Name)
}

func TestDynamoDBHandler_Routing(t *testing.T) {
t.Parallel()

db := dynamodb.NewInMemoryDB()
h := dynamodb.NewHandler(db, slog.Default())

assert.Equal(t, "DynamoDB", h.Name())
assert.Greater(t, h.MatchPriority(), 0)

e := echo.New()

tests := []struct {
name      string
target    string
wantMatch bool
}{
{"dynamodb target", "DynamoDB_20120810.ListTables", true},
{"streams target", "DynamoDBStreams_20120810.ListStreams", true},
{"no match", "SQS.SendMessage", false},
}

for _, tt := range tests {
t.Run(tt.name, func(t *testing.T) {
t.Parallel()

req := httptest.NewRequest(http.MethodPost, "/", nil)
req.Header.Set("X-Amz-Target", tt.target)
c := e.NewContext(req, httptest.NewRecorder())
assert.Equal(t, tt.wantMatch, h.RouteMatcher()(c))
})
}

// Test ExtractOperation
req := httptest.NewRequest(http.MethodPost, "/", nil)
req.Header.Set("X-Amz-Target", "DynamoDB_20120810.ListTables")
c := e.NewContext(req, httptest.NewRecorder())
assert.Equal(t, "ListTables", h.ExtractOperation(c))
}
