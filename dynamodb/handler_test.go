package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"bytes"
	"encoding/json"
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
			body: mustMarshal(t, dynamodb.CreateTableInput{
				TableName: "HandlerTable",
				KeySchema: []dynamodb.KeySchemaElement{
					{AttributeName: "pk", KeyType: "HASH"},
					{AttributeName: "sk", KeyType: "RANGE"},
				},
				AttributeDefinitions: []dynamodb.AttributeDefinition{
					{AttributeName: "pk", AttributeType: "S"},
					{AttributeName: "sk", AttributeType: "S"},
				},
			}),
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "TableDescription",
		},
		{
			name:           "Invalid Method",
			method:         http.MethodPut,
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
			name:           "ListTables",
			method:         http.MethodPost,
			target:         "DynamoDB_20120810.ListTables",
			body:           "{}",
			wantStatusCode: http.StatusOK,
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "HandlerTable", "pk")
			},
			wantBodyContains: "HandlerTable",
		},
		{
			name:   "PutItem",
			method: http.MethodPost,
			target: "DynamoDB_20120810.PutItem",
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "HandlerTable", "pk")
			},
			body: mustMarshal(
				t,
				dynamodb.PutItemInput{
					TableName: "HandlerTable",
					Item:      map[string]any{"pk": map[string]any{"S": "item1"}},
				},
			),
			wantStatusCode: http.StatusOK,
		},
		{
			name:             "TransactGetItems (Implemented)",
			method:           http.MethodPost,
			target:           "DynamoDB_20120810.TransactGetItems",
			body:             "{}",
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
		{
			name:             "TransactWriteItems (Implemented)",
			method:           http.MethodPost,
			target:           "DynamoDB_20120810.TransactWriteItems",
			body:             "{}",
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "ValidationException",
		},
		{
			name:             "GetSupportedOperations",
			method:           http.MethodGet,
			target:           "Unsupported", // Target ignored for GET /
			wantStatusCode:   http.StatusOK,
			wantBodyContains: "CreateTable",
		},
		{
			name:             "Invalid JSON",
			method:           http.MethodPost,
			target:           "DynamoDB_20120810.PutItem",
			body:             "{ invalid json }",
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "JSON Error", // was "cannot be converted to a JSON"
		},
		{
			name:   "Table Not Found (ResourceNotFoundException)",
			method: http.MethodPost,
			target: "DynamoDB_20120810.DescribeTable",
			body: mustMarshal(t, dynamodb.DescribeTableInput{
				TableName: "MissingTable",
			}),
			wantStatusCode:   http.StatusNotFound,
			wantBodyContains: "ResourceNotFoundException",
		},
		{
			name:   "GetItem",
			method: http.MethodPost,
			target: "DynamoDB_20120810.GetItem",
			body: mustMarshal(t, dynamodb.GetItemInput{
				TableName: "HandlerTable",
				Key:       map[string]any{"pk": map[string]any{"S": "item1"}},
			}),
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "HandlerTable", "pk")
				putInput := dynamodb.PutItemInput{
					TableName: "HandlerTable",
					Item:      map[string]any{"pk": map[string]any{"S": "item1"}},
				}
				sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(sdkPut)
				require.NoError(t, err)
			},
			wantStatusCode:   http.StatusOK,
			wantBodyContains: `{"Item":{"pk":{"S":"item1"}}}`,
		},
		{
			name:   "Query",
			method: http.MethodPost,
			target: "DynamoDB_20120810.Query",
			body: mustMarshal(t, dynamodb.QueryInput{
				TableName:              "HandlerTable",
				KeyConditionExpression: "pk = :pk AND sk BETWEEN :sk1 AND :sk2",
				ExpressionAttributeValues: map[string]any{
					":pk":  map[string]any{"S": "pk1"},
					":sk1": map[string]any{"S": "sk1"},
					":sk2": map[string]any{"S": "sk3"},
					"info": map[string]any{
						"M": map[string]any{
							"author": map[string]any{"S": "me"},
							"year":   map[string]any{"N": "2020"},
						},
					},
				},
			}),
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "HandlerTable", "pk", "sk")
				items := []struct{ pk, sk string }{
					{"pk1", "sk1"}, {"pk1", "sk2"}, {"pk1", "sk3"}, {"pk2", "sk1"},
				}
				for _, item := range items {
					putInput := dynamodb.PutItemInput{
						TableName: "HandlerTable",
						Item: map[string]any{
							"pk": map[string]any{"S": item.pk},
							"sk": map[string]any{"S": item.sk},
						},
					}
					sdkPut, _ := dynamodb.ToSDKPutItemInput(&putInput)
					_, putErr := db.PutItem(sdkPut)
					require.NoError(t, putErr)
				}
			},
			wantStatusCode:   http.StatusOK,
			wantBodyContains: `"Count":3`,
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
	createTableHelper(t, handler.DB, "DispatchTable", "pk")

	ops := []string{
		"CreateTable", "DescribeTable", "ListTables",
		"PutItem", "GetItem", "Scan", "UpdateItem", "Query",
		"BatchGetItem", "BatchWriteItem", "DeleteItem", "DeleteTable",
	}

	for _, op := range ops {
		t.Run(op, func(t *testing.T) {
			t.Parallel()
			var body string
			switch op {
			case "CreateTable":
				body = mustMarshal(t, dynamodb.CreateTableInput{
					TableName:            "NewTable_" + op,
					KeySchema:            []dynamodb.KeySchemaElement{{AttributeName: "pk", KeyType: "HASH"}},
					AttributeDefinitions: []dynamodb.AttributeDefinition{{AttributeName: "pk", AttributeType: "S"}},
				})
			case "PutItem":
				body = mustMarshal(t, dynamodb.PutItemInput{
					TableName: "DispatchTable",
					Item:      map[string]any{"pk": map[string]any{"S": "item1"}},
				})
			case "Query":
				body = mustMarshal(t, dynamodb.QueryInput{
					TableName:              "DispatchTable",
					KeyConditionExpression: "pk = :pk",
					ExpressionAttributeValues: map[string]any{
						":pk": map[string]any{"S": "item1"},
					},
				})
			default:
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

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)

	return string(data)
}
