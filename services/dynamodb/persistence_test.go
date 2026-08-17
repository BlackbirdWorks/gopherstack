package dynamodb_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

func TestInMemoryDB_SnapshotRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, db *dynamodb.InMemoryDB) string
		verify func(t *testing.T, db *dynamodb.InMemoryDB, id string)
		name   string
	}{
		{
			name: "round_trip_preserves_state",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) string {
				t.Helper()
				input := models.ToSDKCreateTableInput(&models.CreateTableInput{
					TableName: "test-table",
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "id", KeyType: models.KeyTypeHash},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "id", AttributeType: "S"},
					},
				})
				_, err := db.CreateTable(t.Context(), input)
				require.NoError(t, err)

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
			setup: func(_ *testing.T, _ *dynamodb.InMemoryDB) string { return "" },
			verify: func(t *testing.T, db *dynamodb.InMemoryDB, _ string) {
				t.Helper()

				tables := db.ListAllTables()
				assert.Empty(t, tables)
			},
		},
		{
			name: "stream_shards_round_trip",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) string {
				t.Helper()
				input := models.ToSDKCreateTableInput(&models.CreateTableInput{
					TableName: "stream-table",
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "id", KeyType: models.KeyTypeHash},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "id", AttributeType: "S"},
					},
					StreamSpecification: map[string]any{
						"StreamEnabled":  true,
						"StreamViewType": "NEW_AND_OLD_IMAGES",
					},
				})
				_, err := db.CreateTable(t.Context(), input)
				require.NoError(t, err)

				return "stream-table"
			},
			verify: func(t *testing.T, db *dynamodb.InMemoryDB, tableName string) {
				t.Helper()

				table, ok := db.GetTable(tableName)
				require.True(t, ok)
				require.NotEmpty(t, table.StreamARN)
				shards := db.StreamShards(tableName)
				assert.NotEmpty(t, shards, "stream shards must not be empty after restore")
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			original := dynamodb.NewInMemoryDB()
			id := tt.setup(t, original)

			snap := original.Snapshot(t.Context())
			require.NotNil(t, snap)

			fresh := dynamodb.NewInMemoryDB()
			require.NoError(t, fresh.Restore(t.Context(), snap))

			tt.verify(t, fresh, id)
		})
	}
}

func TestInMemoryDB_RestoreInvalidData(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	err := db.Restore(t.Context(), []byte("not-valid-json"))
	require.Error(t, err)
}

func TestDynamoDBHandler_Persistence(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)

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

	snap := h.Snapshot(t.Context())
	require.NotNil(t, snap)

	freshDB := dynamodb.NewInMemoryDB()
	freshH := dynamodb.NewHandler(freshDB)
	require.NoError(t, freshH.Restore(t.Context(), snap))

	tables := freshDB.ListAllTables()
	require.Len(t, tables, 1)
	assert.Equal(t, "handler-test-table", tables[0].Name)
}

func TestDynamoDBHandler_Routing(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	h := dynamodb.NewHandler(db)

	assert.Equal(t, "DynamoDB", h.Name())
	assert.Positive(t, h.MatchPriority())

	e := echo.New()

	tests := []struct {
		name      string
		target    string
		wantMatch bool
	}{
		{"dynamodb target", "DynamoDB_20120810.ListTables", true},
		{"streams target", "DynamoDBStreams_20120810.ListStreams", false},
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
