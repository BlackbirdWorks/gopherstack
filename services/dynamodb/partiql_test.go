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

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

// partiqlRows returns a fresh copy of the shared dataset used across PartiQL tests.
func partiqlRows() []map[string]any {
	return []map[string]any{
		{"pk": map[string]string{"S": "a"}, "status": map[string]string{"S": "active"}},
		{"pk": map[string]string{"S": "b"}, "status": map[string]string{"S": "inactive"}},
		{"pk": map[string]string{"S": "c"}, "status": map[string]string{"S": "active"}},
	}
}

// setupPartiQLTable creates a fresh InMemoryDB with a single-key table named "T" and inserts rows.
func setupPartiQLTable(t *testing.T, rows []map[string]any) *dynamodb.DynamoDBHandler {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)

	createBody := mustMarshal(t, map[string]any{
		"TableName": "T",
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
		putBody := mustMarshal(t, map[string]any{"TableName": "T", "Item": row})
		doRequest(t, handler, "DynamoDB_20120810.PutItem", putBody)
	}

	return handler
}

// setupPartiQLCompositeTable creates a table with a hash+range key for multi-key tests.
func setupPartiQLCompositeTable(t *testing.T, rows []map[string]any) *dynamodb.DynamoDBHandler {
	t.Helper()

	db := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(db)

	createBody := mustMarshal(t, map[string]any{
		"TableName": "T",
		"KeySchema": []map[string]string{
			{"AttributeName": "pk", "KeyType": "HASH"},
			{"AttributeName": "sk", "KeyType": "RANGE"},
		},
		"AttributeDefinitions": []map[string]string{
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "sk", "AttributeType": "S"},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	doRequest(t, handler, "DynamoDB_20120810.CreateTable", createBody)

	for _, row := range rows {
		putBody := mustMarshal(t, map[string]any{"TableName": "T", "Item": row})
		doRequest(t, handler, "DynamoDB_20120810.PutItem", putBody)
	}

	return handler
}

// partiqlItemsFrom decodes the Items list from an ExecuteStatement response.
func partiqlItemsFrom(t *testing.T, rec *httptest.ResponseRecorder) []any {
	t.Helper()
	require.Equal(t, http.StatusOK, rec.Code, "response body: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	items, ok := resp["Items"].([]any)
	require.True(t, ok, "Items field missing or wrong type")

	return items
}

// ── SELECT ─────────────────────────────────────────────────────────────────────

func TestPartiQL_Select(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		statement string
		rows      []map[string]any
		params    []map[string]any
		wantPKs   []string
		skipAttrs []string
		wantCount int
		wantErr   bool
	}{
		{
			name:      "select all",
			statement: `SELECT * FROM "T"`,
			wantCount: 3,
			rows:      partiqlRows(),
		},
		{
			name:      "select all empty table",
			statement: `SELECT * FROM "T"`,
			wantCount: 0,
		},
		{
			name:      "select WHERE param equals",
			statement: `SELECT * FROM "T" WHERE pk = ?`,
			params:    []map[string]any{{"S": "a"}},
			wantCount: 1,
			wantPKs:   []string{"a"},
			rows:      partiqlRows(),
		},
		{
			name:      "select WHERE string literal",
			statement: `SELECT * FROM "T" WHERE pk = 'a'`,
			wantCount: 1,
			wantPKs:   []string{"a"},
			rows:      partiqlRows(),
		},
		{
			name:      "select WHERE escaped quote in literal",
			statement: `SELECT * FROM "T" WHERE pk = 'O''Reilly'`,
			wantCount: 1,
			wantPKs:   []string{"O'Reilly"},
			rows: []map[string]any{
				{"pk": map[string]string{"S": "O'Reilly"}},
				{"pk": map[string]string{"S": "normal"}},
			},
		},
		{
			name:      "select WHERE literal question mark is not a param",
			statement: `SELECT * FROM "T" WHERE pk = '?'`,
			wantCount: 0, // no item has pk='?'
			rows:      partiqlRows(),
		},
		{
			name:      "select LIMIT",
			statement: `SELECT * FROM "T" LIMIT 2`,
			wantCount: 2,
			rows:      partiqlRows(),
		},
		{
			name:      "select projection specific columns",
			statement: `SELECT pk, status FROM "T"`,
			wantCount: 1,
			skipAttrs: []string{"secret"},
			rows: []map[string]any{
				{
					"pk":     map[string]string{"S": "p1"},
					"status": map[string]string{"S": "active"},
					"secret": map[string]string{"S": "hidden"},
				},
			},
		},
		{
			name:      "select invalid statement no quoted table",
			statement: `SELECT * FROM badformat`,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := setupPartiQLTable(t, tc.rows)

			body := mustMarshal(t, map[string]any{
				"Statement":  tc.statement,
				"Parameters": tc.params,
			})
			rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", body)

			if tc.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}

			items := partiqlItemsFrom(t, rec)
			assert.Len(t, items, tc.wantCount)

			// Verify expected PKs are present.
			if len(tc.wantPKs) > 0 {
				pks := make([]string, 0, len(items))
				for _, raw := range items {
					item := raw.(map[string]any)
					pks = append(pks, item["pk"].(map[string]any)["S"].(string))
				}

				for _, want := range tc.wantPKs {
					assert.Contains(t, pks, want)
				}
			}

			// Verify excluded attributes are absent (projection test).
			if len(tc.skipAttrs) > 0 && len(items) > 0 {
				item := items[0].(map[string]any)
				for _, attr := range tc.skipAttrs {
					assert.NotContains(t, item, attr)
				}
			}
		})
	}
}

// ── INSERT ─────────────────────────────────────────────────────────────────────

func TestPartiQL_Insert(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantVal   map[string]any
		name      string
		statement string
		lookupPK  string
		wantAttr  string
		params    []map[string]any
		wantErr   bool
	}{
		{
			name:      "insert with question mark params",
			statement: `INSERT INTO "T" VALUE {'pk': ?, 'val': ?}`,
			params:    []map[string]any{{"S": "k1"}, {"S": "hello"}},
			lookupPK:  "k1",
			wantAttr:  "val",
			wantVal:   map[string]any{"S": "hello"},
		},
		{
			name:      "insert with string literal",
			statement: `INSERT INTO "T" VALUE {'pk': 'lit1', 'label': 'world'}`,
			lookupPK:  "lit1",
			wantAttr:  "label",
			wantVal:   map[string]any{"S": "world"},
		},
		{
			name:      "insert with numeric literal",
			statement: `INSERT INTO "T" VALUE {'pk': 'num1', 'count': 42}`,
			lookupPK:  "num1",
			wantAttr:  "count",
			wantVal:   map[string]any{"N": "42"},
		},
		{
			name:      "insert with boolean literal",
			statement: `INSERT INTO "T" VALUE {'pk': 'bool1', 'active': TRUE}`,
			lookupPK:  "bool1",
			wantAttr:  "active",
			wantVal:   map[string]any{"BOOL": true},
		},
		{
			name:      "insert with null literal",
			statement: `INSERT INTO "T" VALUE {'pk': 'null1', 'x': NULL}`,
			lookupPK:  "null1",
			wantAttr:  "x",
			wantVal:   map[string]any{"NULL": true},
		},
		{
			name:      "insert with escaped quote in string literal",
			statement: `INSERT INTO "T" VALUE {'pk': 'O''Reilly', 'note': 'it''s fine'}`,
			lookupPK:  "O'Reilly",
			wantAttr:  "note",
			wantVal:   map[string]any{"S": "it's fine"},
		},
		{
			name:      "insert with comma in string value",
			statement: `INSERT INTO "T" VALUE {'pk': 'comma1', 'csv': 'hello, world'}`,
			lookupPK:  "comma1",
			wantAttr:  "csv",
			wantVal:   map[string]any{"S": "hello, world"},
		},
		{
			name:      "insert question mark in string is not a param",
			statement: `INSERT INTO "T" VALUE {'pk': 'q1', 'note': '?'}`,
			lookupPK:  "q1",
			wantAttr:  "note",
			wantVal:   map[string]any{"S": "?"},
		},
		{
			name:      "insert missing VALUE clause",
			statement: `INSERT INTO "T" {'pk': 'bad'}`,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := setupPartiQLTable(t, nil)

			body := mustMarshal(t, map[string]any{
				"Statement":  tc.statement,
				"Parameters": tc.params,
			})
			rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", body)

			if tc.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			// Verify the item was actually inserted.
			getBody := mustMarshal(t, map[string]any{
				"TableName": "T",
				"Key":       map[string]any{"pk": map[string]string{"S": tc.lookupPK}},
			})
			getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			item, ok := getResp["Item"].(map[string]any)
			require.True(t, ok, "item not found for pk=%q", tc.lookupPK)
			assert.Equal(t, tc.wantVal, item[tc.wantAttr], "attribute %q mismatch", tc.wantAttr)
		})
	}
}

// ── UPDATE ─────────────────────────────────────────────────────────────────────

func TestPartiQL_Update(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantVal   map[string]any
		name      string
		statement string
		lookupPK  string
		wantAttr  string
		params    []map[string]any
		wantErr   bool
	}{
		{
			name:      "update with params",
			statement: `UPDATE "T" SET status = ? WHERE pk = ?`,
			params:    []map[string]any{{"S": "new"}, {"S": "a"}},
			lookupPK:  "a",
			wantAttr:  "status",
			wantVal:   map[string]any{"S": "new"},
		},
		{
			name:      "update with string literal in SET",
			statement: `UPDATE "T" SET status = 'updated' WHERE pk = ?`,
			params:    []map[string]any{{"S": "b"}},
			lookupPK:  "b",
			wantAttr:  "status",
			wantVal:   map[string]any{"S": "updated"},
		},
		{
			name:      "update no WHERE clause",
			statement: `UPDATE "T" SET status = ?`,
			params:    []map[string]any{{"S": "new"}},
			wantErr:   true,
		},
		{
			name:      "update no SET clause",
			statement: `UPDATE "T" WHERE pk = ?`,
			params:    []map[string]any{{"S": "a"}},
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := setupPartiQLTable(t, partiqlRows())

			body := mustMarshal(t, map[string]any{
				"Statement":  tc.statement,
				"Parameters": tc.params,
			})
			rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", body)

			if tc.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			getBody := mustMarshal(t, map[string]any{
				"TableName": "T",
				"Key":       map[string]any{"pk": map[string]string{"S": tc.lookupPK}},
			})
			getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)
			require.Equal(t, http.StatusOK, getRec.Code)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			item := getResp["Item"].(map[string]any)
			assert.Equal(t, tc.wantVal, item[tc.wantAttr])
		})
	}
}

// ── DELETE ─────────────────────────────────────────────────────────────────────

func TestPartiQL_Delete(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		statement   string
		deletedPK   string
		survivingPK string
		params      []map[string]any
		wantErr     bool
	}{
		{
			name:        "delete with param",
			statement:   `DELETE FROM "T" WHERE pk = ?`,
			params:      []map[string]any{{"S": "a"}},
			deletedPK:   "a",
			survivingPK: "b",
		},
		{
			name:        "delete with string literal",
			statement:   `DELETE FROM "T" WHERE pk = 'b'`,
			deletedPK:   "b",
			survivingPK: "a",
		},
		{
			name:        "delete with lowercase where keyword",
			statement:   `DELETE FROM "T" where pk = ?`,
			params:      []map[string]any{{"S": "c"}},
			deletedPK:   "c",
			survivingPK: "a",
		},
		{
			name:      "delete no WHERE clause",
			statement: `DELETE FROM "T"`,
			wantErr:   true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := setupPartiQLTable(t, partiqlRows())

			body := mustMarshal(t, map[string]any{
				"Statement":  tc.statement,
				"Parameters": tc.params,
			})
			rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", body)

			if tc.wantErr {
				assert.NotEqual(t, http.StatusOK, rec.Code)

				return
			}

			require.Equal(t, http.StatusOK, rec.Code)

			// Deleted item must be gone.
			getBody := mustMarshal(t, map[string]any{
				"TableName": "T",
				"Key":       map[string]any{"pk": map[string]string{"S": tc.deletedPK}},
			})
			getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))
			assert.Nil(t, getResp["Item"], "deleted item should not be present")

			// Surviving item must still be there.
			getBody2 := mustMarshal(t, map[string]any{
				"TableName": "T",
				"Key":       map[string]any{"pk": map[string]string{"S": tc.survivingPK}},
			})
			getRec2 := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody2)

			var getResp2 map[string]any
			require.NoError(t, json.Unmarshal(getRec2.Body.Bytes(), &getResp2))
			assert.NotNil(t, getResp2["Item"], "surviving item should still be present")
		})
	}
}

// ── COMPOSITE KEY (UPDATE/DELETE with case-insensitive AND) ───────────────────

func TestPartiQL_CompositeKey_CaseInsensitiveAND(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{
			"pk":    map[string]string{"S": "user1"},
			"sk":    map[string]string{"S": "order1"},
			"total": map[string]string{"N": "100"},
		},
		{
			"pk":    map[string]string{"S": "user1"},
			"sk":    map[string]string{"S": "order2"},
			"total": map[string]string{"N": "200"},
		},
	}

	tests := []struct {
		wantVal   map[string]any
		name      string
		statement string
		lookupSK  string
		wantAttr  string
		params    []map[string]any
		wantGone  bool
	}{
		{
			name:      "update with lowercase and in WHERE",
			statement: `UPDATE "T" SET total = ? WHERE pk = ? and sk = ?`,
			params:    []map[string]any{{"N": "999"}, {"S": "user1"}, {"S": "order1"}},
			lookupSK:  "order1",
			wantAttr:  "total",
			wantVal:   map[string]any{"N": "999"},
		},
		{
			name:      "delete with uppercase AND in WHERE",
			statement: `DELETE FROM "T" WHERE pk = ? AND sk = ?`,
			params:    []map[string]any{{"S": "user1"}, {"S": "order2"}},
			lookupSK:  "order2",
			wantGone:  true,
		},
		{
			name:      "delete with mixed-case And in WHERE",
			statement: `DELETE FROM "T" WHERE pk = ? And sk = ?`,
			params:    []map[string]any{{"S": "user1"}, {"S": "order1"}},
			lookupSK:  "order1",
			wantGone:  true,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := setupPartiQLCompositeTable(t, rows)

			body := mustMarshal(t, map[string]any{
				"Statement":  tc.statement,
				"Parameters": tc.params,
			})
			rec := doRequest(t, handler, "DynamoDB_20120810.ExecuteStatement", body)
			require.Equal(t, http.StatusOK, rec.Code)

			// Look up the item by composite key.
			getBody := mustMarshal(t, map[string]any{
				"TableName": "T",
				"Key": map[string]any{
					"pk": map[string]string{"S": "user1"},
					"sk": map[string]string{"S": tc.lookupSK},
				},
			})
			getRec := doRequest(t, handler, "DynamoDB_20120810.GetItem", getBody)

			var getResp map[string]any
			require.NoError(t, json.Unmarshal(getRec.Body.Bytes(), &getResp))

			if tc.wantGone {
				assert.Nil(t, getResp["Item"])
			} else {
				item := getResp["Item"].(map[string]any)
				assert.Equal(t, tc.wantVal, item[tc.wantAttr])
			}
		})
	}
}

// ── BATCH ─────────────────────────────────────────────────────────────────────

func TestPartiQL_Batch(t *testing.T) {
	t.Parallel()

	rows := []map[string]any{
		{"pk": map[string]string{"S": "ba"}, "v": map[string]string{"S": "1"}},
		{"pk": map[string]string{"S": "bb"}, "v": map[string]string{"S": "2"}},
	}

	tests := []struct {
		name         string
		wantFirstPK  string
		wantSecondPK string
		statements   []map[string]any
		wantLen      int
		wantErrAt    int
	}{
		{
			name: "batch select all returns first item each",
			statements: []map[string]any{
				{"Statement": `SELECT * FROM "T"`},
			},
			wantLen:     1,
			wantFirstPK: "ba",
		},
		{
			name: "batch select with per-statement WHERE",
			statements: []map[string]any{
				{
					"Statement":  `SELECT * FROM "T" WHERE pk = ?`,
					"Parameters": []map[string]any{{"S": "ba"}},
				},
				{
					"Statement":  `SELECT * FROM "T" WHERE pk = ?`,
					"Parameters": []map[string]any{{"S": "bb"}},
				},
			},
			wantLen:      2,
			wantFirstPK:  "ba",
			wantSecondPK: "bb",
		},
		{
			name: "batch with one invalid statement returns error entry",
			statements: []map[string]any{
				{
					"Statement":  `SELECT * FROM "T" WHERE pk = ?`,
					"Parameters": []map[string]any{{"S": "ba"}},
				},
				{"Statement": `SELECT * FROM badformat`},
			},
			wantLen:     2,
			wantFirstPK: "ba",
			wantErrAt:   1, // second entry should have Error
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := setupPartiQLTable(t, rows)

			batchBody := mustMarshal(t, map[string]any{"Statements": tc.statements})
			rec := doRequest(t, handler, "DynamoDB_20120810.BatchExecuteStatement", batchBody)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			responses, ok := resp["Responses"].([]any)
			require.True(t, ok)
			require.Len(t, responses, tc.wantLen)

			if tc.wantFirstPK != "" {
				r0 := responses[0].(map[string]any)
				item, hasItem := r0["Item"].(map[string]any)
				require.True(t, hasItem, "first response should have an Item")
				assert.Equal(t, tc.wantFirstPK, item["pk"].(map[string]any)["S"])
			}

			if tc.wantSecondPK != "" {
				r1 := responses[1].(map[string]any)
				item := r1["Item"].(map[string]any)
				assert.Equal(t, tc.wantSecondPK, item["pk"].(map[string]any)["S"])
			}

			if tc.wantErrAt > 0 {
				rErr := responses[tc.wantErrAt].(map[string]any)
				assert.NotNil(t, rErr["Error"], "expected Error in response[%d]", tc.wantErrAt)
			}
		})
	}
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
