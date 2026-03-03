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

// setupPartiQLTable creates a table and inserts rows for PartiQL tests.
// It returns the handler to use.
func setupPartiQLTable(t *testing.T, tableName string, rows []map[string]any) *dynamodb.DynamoDBHandler {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db, slog.Default())

	createBody := mustMarshal(t, map[string]any{
		"TableName": tableName,
		"KeySchema": []map[string]string{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]string{
			{"AttributeName": "pk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	doRequest(t, handler, "DynamoDB_20120810.CreateTable", createBody)

	for _, row := range rows {
		putBody := mustMarshal(t, map[string]any{"TableName": tableName, "Item": row})
		doRequest(t, handler, "DynamoDB_20120810.PutItem", putBody)
	}

	return handler
}

func TestPartiQL_SelectWithWhere_Param(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "a"}, "status": map[string]string{"S": "active"}},
		{"pk": map[string]string{"S": "b"}, "status": map[string]string{"S": "inactive"}},
	}
	handler := setupPartiQLTable(t, "WhereTable", rows)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement":  `SELECT * FROM "WhereTable" WHERE pk = ?`,
		"Parameters": []map[string]any{{"S": "a"}},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item := items[0].(map[string]any)
	pkVal := item["pk"].(map[string]any)
	assert.Equal(t, "a", pkVal["S"])
}

func TestPartiQL_SelectWithWhere_StringLiteral(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "x"}, "color": map[string]string{"S": "red"}},
		{"pk": map[string]string{"S": "y"}, "color": map[string]string{"S": "blue"}},
	}
	handler := setupPartiQLTable(t, "LiteralTable", rows)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `SELECT * FROM "LiteralTable" WHERE pk = 'x'`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item := items[0].(map[string]any)
	assert.Equal(t, "x", item["pk"].(map[string]any)["S"])
}

func TestPartiQL_SelectWithLimit(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "1"}},
		{"pk": map[string]string{"S": "2"}},
		{"pk": map[string]string{"S": "3"}},
	}
	handler := setupPartiQLTable(t, "LimitTable", rows)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `SELECT * FROM "LimitTable" LIMIT 2`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	assert.Len(t, items, 2)
}

func TestPartiQL_SelectWithProjection(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{
			"pk":     map[string]string{"S": "p1"},
			"name":   map[string]string{"S": "Alice"},
			"secret": map[string]string{"S": "hidden"},
		},
	}
	handler := setupPartiQLTable(t, "ProjTable", rows)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `SELECT pk, name FROM "ProjTable"`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	items, ok := resp["Items"].([]any)
	require.True(t, ok)
	require.Len(t, items, 1)

	item := items[0].(map[string]any)
	assert.Contains(t, item, "pk")
	assert.Contains(t, item, "name")
	assert.NotContains(t, item, "secret")
}

func TestPartiQL_InsertInto(t *testing.T) {
	t.Parallel()

	handler := setupPartiQLTable(t, "InsertTable", nil)

	// INSERT using ? parameters
	stmtBody := mustMarshal(t, map[string]any{
		"Statement":  `INSERT INTO "InsertTable" VALUE {'pk': ?, 'val': ?}`,
		"Parameters": []map[string]any{{"S": "k1"}, {"S": "hello"}},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the item was inserted
	getBody := mustMarshal(t, map[string]any{
		"TableName": "InsertTable",
		"Key":       map[string]any{"pk": map[string]string{"S": "k1"}},
	})
	getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	item, ok := getResp["Item"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "hello", item["val"].(map[string]any)["S"])
}

func TestPartiQL_InsertInto_Literals(t *testing.T) {
	t.Parallel()

	handler := setupPartiQLTable(t, "InsertLitTable", nil)

	// INSERT using literal values
	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `INSERT INTO "InsertLitTable" VALUE {'pk': 'lit1', 'count': 42}`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	require.Equal(t, http.StatusOK, rec.Code)

	getBody := mustMarshal(t, map[string]any{
		"TableName": "InsertLitTable",
		"Key":       map[string]any{"pk": map[string]string{"S": "lit1"}},
	})
	getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	item, ok := getResp["Item"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, "42", item["count"].(map[string]any)["N"])
}

func TestPartiQL_Update(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "u1"}, "status": map[string]string{"S": "old"}},
	}
	handler := setupPartiQLTable(t, "UpdateTable", rows)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement":  `UPDATE "UpdateTable" SET status = ? WHERE pk = ?`,
		"Parameters": []map[string]any{{"S": "new"}, {"S": "u1"}},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify the item was updated
	getBody := mustMarshal(t, map[string]any{
		"TableName": "UpdateTable",
		"Key":       map[string]any{"pk": map[string]string{"S": "u1"}},
	})
	getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)
	require.Equal(t, http.StatusOK, getRec.Code)

	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	item := getResp["Item"].(map[string]any)
	assert.Equal(t, "new", item["status"].(map[string]any)["S"])
}

func TestPartiQL_Update_NoWhere(t *testing.T) {
	t.Parallel()

	handler := setupPartiQLTable(t, "UpdateNoWhereTable", nil)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement":  `UPDATE "UpdateNoWhereTable" SET status = ?`,
		"Parameters": []map[string]any{{"S": "new"}},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestPartiQL_Delete(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "del1"}, "attr": map[string]string{"S": "val"}},
		{"pk": map[string]string{"S": "del2"}, "attr": map[string]string{"S": "val"}},
	}
	handler := setupPartiQLTable(t, "DeleteTable", rows)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement":  `DELETE FROM "DeleteTable" WHERE pk = ?`,
		"Parameters": []map[string]any{{"S": "del1"}},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify del1 is gone
	getBody := mustMarshal(t, map[string]any{
		"TableName": "DeleteTable",
		"Key":       map[string]any{"pk": map[string]string{"S": "del1"}},
	})
	getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)
	require.Equal(t, http.StatusOK, getRec.Code)
	var getResp map[string]any
	require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
	assert.Nil(t, getResp["Item"])

	// Verify del2 is still there
	getBody2 := mustMarshal(t, map[string]any{
		"TableName": "DeleteTable",
		"Key":       map[string]any{"pk": map[string]string{"S": "del2"}},
	})
	getRec2 := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody2)
	require.Equal(t, http.StatusOK, getRec2.Code)
	var getResp2 map[string]any
	require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))
	assert.NotNil(t, getResp2["Item"])
}

func TestPartiQL_Delete_NoWhere(t *testing.T) {
	t.Parallel()

	handler := setupPartiQLTable(t, "DeleteNoWhereTable", nil)

	stmtBody := mustMarshal(t, map[string]any{
		"Statement": `DELETE FROM "DeleteNoWhereTable"`,
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", stmtBody)
	assert.NotEqual(t, http.StatusOK, rec.Code)
}

func TestPartiQL_BatchExecuteStatement_WithWhere(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "ba"}, "v": map[string]string{"S": "1"}},
		{"pk": map[string]string{"S": "bb"}, "v": map[string]string{"S": "2"}},
	}
	handler := setupPartiQLTable(t, "BatchWhereTable", rows)

	batchBody := mustMarshal(t, map[string]any{
		"Statements": []map[string]any{
			{
				"Statement":  `SELECT * FROM "BatchWhereTable" WHERE pk = ?`,
				"Parameters": []map[string]any{{"S": "ba"}},
			},
			{
				"Statement":  `SELECT * FROM "BatchWhereTable" WHERE pk = ?`,
				"Parameters": []map[string]any{{"S": "bb"}},
			},
		},
	})
	rec := doRequest(t, handler, "DynamoDB_20120810.BatchExecuteStatement", batchBody)

	require.Equal(t, http.StatusOK, rec.Code)
	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	responses, ok := resp["Responses"].([]any)
	require.True(t, ok)
	require.Len(t, responses, 2)

	r0 := responses[0].(map[string]any)
	r1 := responses[1].(map[string]any)
	assert.Equal(t, "ba", r0["Item"].(map[string]any)["pk"].(map[string]any)["S"])
	assert.Equal(t, "bb", r1["Item"].(map[string]any)["pk"].(map[string]any)["S"])
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
