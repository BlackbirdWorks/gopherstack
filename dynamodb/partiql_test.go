package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dynamodb"
	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

func TestPartiQL_ExecuteStatement(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db, slog.Default())

	// Create a table and put some items
	createTableBody := mustMarshal(t, map[string]any{
		"TableName": "PartiQLTable",
		"KeySchema": []map[string]string{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]string{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	doRequest(t, handler, "DynamoDB_20120810.CreateTable", createTableBody)

	putBody := mustMarshal(t, map[string]any{
		"TableName": "PartiQLTable",
		"Item": map[string]any{
			"pk": map[string]string{"S": "row1"},
		},
	})
	doRequest(t, handler, "DynamoDB_20120810.PutItem", putBody)

	// Execute a SELECT PartiQL statement
	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `SELECT * FROM "PartiQLTable"`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 1)
}

func TestPartiQL_ExecuteStatement_InvalidStatement(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db, slog.Default())

	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `SELECT * FROM badformat`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestPartiQL_BatchExecuteStatement(t *testing.T) {
	t.Parallel()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db, slog.Default())

	// Create a table
	createTableBody := mustMarshal(t, map[string]any{
		"TableName": "BatchPartiQLTable",
		"KeySchema": []map[string]string{
			{"AttributeName": "id", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]string{
			{"AttributeName": "id", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	doRequest(t, handler, "DynamoDB_20120810.CreateTable", createTableBody)

	// Put two items
	for _, key := range []string{"a", "b"} {
		putBody := mustMarshal(t, map[string]any{
			"TableName": "BatchPartiQLTable",
			"Item":      map[string]any{"id": map[string]string{"S": key}},
		})
		doRequest(t, handler, "DynamoDB_20120810.PutItem", putBody)
	}

	batchBody := mustMarshal(t, map[string]any{
		"Statements": []map[string]any{
			{"Statement": `SELECT * FROM "BatchPartiQLTable"`},
		},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.BatchExecuteStatement", batchBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	responses, ok := resp["Responses"].([]any)
	require.True(t, ok)
	assert.Len(t, responses, 1)
}

// doRequest fires a POST to the DynamoDB handler with the given X-Amz-Target.
func doRequest(t *testing.T, handler *dynamodb.DynamoDBHandler, target, body string) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	ctx := logger.Save(req.Context(), slog.Default())
	req = req.WithContext(ctx)

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)

	err := handler.Handler()(c)
	require.NoError(t, err)

	return rec
}
