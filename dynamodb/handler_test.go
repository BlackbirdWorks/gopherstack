package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"bytes"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_ServeHTTP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup            func(t *testing.T, db *dynamodb.InMemoryDB)
		name             string
		method           string
		target           string
		body             string
		wantBodyContains string
		wantStatusCode   int
	}{
		{
			name:   "Valid CreateTable",
			method: http.MethodPost,
			target: "DynamoDB_20120810.CreateTable",
			body: `{"TableName": "HandlerTable", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], ` +
				`"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], ` +
				`"ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}}`,
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "TableDescription",
		},
		{
			name:           "Invalid Method",
			method:         http.MethodGet,
			wantStatusCode: http.StatusMethodNotAllowed,
		},
		{
			name:             "Missing Target",
			method:           http.MethodPost,
			target:           "",
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "Missing X-Amz-Target",
		},
		{
			name:             "Invalid Target Format",
			method:           http.MethodPost,
			target:           "InvalidTarget",
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "Invalid X-Amz-Target",
		},
		{
			name:             "Unknown Operation",
			method:           http.MethodPost,
			target:           "DynamoDB_20120810.UnknownOp",
			body:             "{}",
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "UnknownOperationException",
		},
		{
			name:             "ListTables",
			method:           http.MethodPost,
			target:           "DynamoDB_20120810.ListTables",
			body:             "{}",
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "HandlerTable",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				setupTable(t, db, "HandlerTable")
			},
		},
		{
			name:           "PutItem",
			method:         http.MethodPost,
			target:         "DynamoDB_20120810.PutItem",
			body:           `{"TableName": "HandlerTable", "Item": {"pk": {"S": "item1"}}}`,
			wantStatusCode: http.StatusOK,
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				setupTable(t, db, "HandlerTable")
			},
		},
		{
			name:             "GetItem",
			method:           http.MethodPost,
			target:           "DynamoDB_20120810.GetItem",
			body:             `{"TableName": "HandlerTable", "Key": {"pk": {"S": "item1"}}}`,
			wantStatusCode:   http.StatusOK,
			wantBodyContains: `{"Item":{"pk":{"S":"item1"}}}`,
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				setupTable(t, db, "HandlerTable")
				_, err := db.PutItem([]byte(`{"TableName": "HandlerTable", "Item": {"pk": {"S": "item1"}}}`))
				require.NoError(t, err)
			},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := dynamodb.NewHandler()
			if tc.setup != nil {
				tc.setup(t, handler.DB)
			}

			req := httptest.NewRequest(tc.method, "/", bytes.NewBufferString(tc.body))
			if tc.target != "" {
				req.Header.Set("X-Amz-Target", tc.target)
			}
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			resp := w.Result()
			defer resp.Body.Close()

			assert.Equal(t, tc.wantStatusCode, resp.StatusCode)
			if tc.wantBodyContains != "" {
				assert.Contains(t, w.Body.String(), tc.wantBodyContains)
			}
		})
	}
}

func TestHandler_Dispatch_Coverage(t *testing.T) {
	t.Parallel()
	// Test dispatching to all supported operations to ensure dispatch switch is covered
	handler := dynamodb.NewHandler()
	handler.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	setupTable(t, handler.DB, "DispatchTable")

	ops := []string{
		"CreateTable", "DescribeTable", "ListTables",
		"PutItem", "GetItem", "Scan", "UpdateItem", "Query",
		"BatchGetItem", "BatchWriteItem", "DeleteItem", "DeleteTable",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			var body string
			if op == "CreateTable" {
				// CreateTable needs valid body to not fail immediately in generic unmarshal if checked there
				// But dispatch just calls the method. The method might fail, but dispatch is covered.
				// We just want to check we don't return "UnknownOperationException"
				body = `{"TableName": "NewTable_` + op + `", "KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}], ` +
					`"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}], ` +
					`"ProvisionedThroughput": {"ReadCapacityUnits": 1, "WriteCapacityUnits": 1}}`
			} else {
				body = `{"TableName": "DispatchTable"}`
			}

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+op)
			w := httptest.NewRecorder()

			handler.ServeHTTP(w, req)

			// We don't necessarily care if the op succeeded (some need more complex body),
			// just that it didn't return 400 UnknownOperation
			// Exception: "UnknownOperation" tests will return 400

			if w.Code == http.StatusBadRequest {
				// check if it is UnknownOperation
				assert.NotContains(t, w.Body.String(), "UnknownOperationException",
					"Op %s returned UnknownOperation", op)
			}
		})
	}
}

func setupTable(t *testing.T, db *dynamodb.InMemoryDB, name string) {
	t.Helper()
	_, err := db.CreateTable([]byte(`{
		"TableName": "` + name + `",
		"KeySchema": [{"AttributeName": "pk", "KeyType": "HASH"}],
		"AttributeDefinitions": [{"AttributeName": "pk", "AttributeType": "S"}],
		"ProvisionedThroughput": {"ReadCapacityUnits": 5, "WriteCapacityUnits": 5}
	}`))
	require.NoError(t, err)
}
