package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"hash/crc32"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"Gopherstack/dynamodb"
	"Gopherstack/dynamodb/models"
	"Gopherstack/pkgs/logger"
)

// serveEchoHandler is a test helper that invokes an Echo handler with a raw HTTP request.
func serveEchoHandler(handler echo.HandlerFunc, w http.ResponseWriter, r *http.Request) error {
	e := echo.New()
	c := e.NewContext(r, w)
	// Inject logger into context for handlers that expect it
	ctx := logger.Save(r.Context(), slog.Default())
	*c.Request() = *r.WithContext(ctx)
	return handler(c)
}

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
			body: mustMarshal(t, models.CreateTableInput{
				TableName: "HandlerTable",
				KeySchema: []models.KeySchemaElement{
					{AttributeName: "pk", KeyType: "HASH"},
					{AttributeName: "sk", KeyType: "RANGE"},
				},
				AttributeDefinitions: []models.AttributeDefinition{
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
				models.PutItemInput{
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
			body: mustMarshal(t, models.DescribeTableInput{
				TableName: "MissingTable",
			}),
			wantStatusCode:   http.StatusBadRequest,
			wantBodyContains: "ResourceNotFoundException",
		},
		{
			name:   "GetItem",
			method: http.MethodPost,
			target: "DynamoDB_20120810.GetItem",
			body: mustMarshal(t, models.GetItemInput{
				TableName: "HandlerTable",
				Key:       map[string]any{"pk": map[string]any{"S": "item1"}},
			}),
			setup: func(t *testing.T, db *dynamodb.InMemoryDB) {
				t.Helper()
				createTableHelper(t, db, "HandlerTable", "pk")
				putInput := models.PutItemInput{
					TableName: "HandlerTable",
					Item:      map[string]any{"pk": map[string]any{"S": "item1"}},
				}
				sdkPut, _ := models.ToSDKPutItemInput(&putInput)
				_, err := db.PutItem(t.Context(), sdkPut)
				require.NoError(t, err)
			},
			wantStatusCode:   http.StatusOK,
			wantBodyContains: `{"Item":{"pk":{"S":"item1"}}}`,
		},
		{
			name:   "Query",
			method: http.MethodPost,
			target: "DynamoDB_20120810.Query",
			body: mustMarshal(t, models.QueryInput{
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
					putInput := models.PutItemInput{
						TableName: "HandlerTable",
						Item: map[string]any{
							"pk": map[string]any{"S": item.pk},
							"sk": map[string]any{"S": item.sk},
						},
					}
					sdkPut, _ := models.ToSDKPutItemInput(&putInput)
					_, putErr := db.PutItem(t.Context(), sdkPut)
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

			handler := dynamodb.NewHandler(slog.Default())
			if tc.setup != nil {
				tc.setup(t, handler.DB)
			}

			req := httptest.NewRequest(tc.method, "/", bytes.NewBufferString(tc.body))
			if tc.target != "" {
				req.Header.Set("X-Amz-Target", tc.target)
			}
			w := httptest.NewRecorder()

			echoHandler := handler.Handler()
			_ = serveEchoHandler(echoHandler, w, req)

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
	handler := dynamodb.NewHandler(slog.Default())
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
				body = mustMarshal(t, models.CreateTableInput{
					TableName: "NewTable_" + op,
					KeySchema: []models.KeySchemaElement{
						{AttributeName: "pk", KeyType: "HASH"},
					},
					AttributeDefinitions: []models.AttributeDefinition{
						{AttributeName: "pk", AttributeType: "S"},
					},
				})
			case "PutItem":
				body = mustMarshal(t, models.PutItemInput{
					TableName: "DispatchTable",
					Item:      map[string]any{"pk": map[string]any{"S": "item1"}},
				})
			case "Query":
				body = mustMarshal(t, models.QueryInput{
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

			echoHandler := handler.Handler()
			_ = serveEchoHandler(echoHandler, w, req)

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

func TestHandler_CRC32Header(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup  func(t *testing.T, db *dynamodb.InMemoryDB)
		name   string
		target string
		body   string
	}{
		{
			name:   "success response has crc32 header",
			target: "DynamoDB_20120810.ListTables",
			body:   "{}",
		},
		{
			name:   "error response has crc32 header",
			target: "DynamoDB_20120810.DescribeTable",
			body:   mustMarshal(t, models.DescribeTableInput{TableName: "Missing"}),
		},
		{
			name:   "unknown operation response has crc32 header",
			target: "DynamoDB_20120810.NotAnOp",
			body:   "{}",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			handler := dynamodb.NewHandler(slog.Default())
			if tc.setup != nil {
				tc.setup(t, handler.DB)
			}

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(tc.body))
			req.Header.Set("X-Amz-Target", tc.target)
			w := httptest.NewRecorder()

			echoHandler := handler.Handler()
			_ = serveEchoHandler(echoHandler, w, req)

			resp := w.Result()
			defer resp.Body.Close()

			crc32H := resp.Header.Get("X-Amz-Crc32")
			if crc32H == "" {
				crc32H = resp.Header.Get("X-Amz-Crc32")
			}
			require.NotEmpty(t, crc32H, "X-Amz-Crc32 header must be present")

			gotChecksum, err := strconv.ParseUint(crc32H, 10, 32)
			require.NoError(t, err, "x-amz-crc32 header must be a valid uint32")

			wantChecksum := uint64(crc32.ChecksumIEEE(w.Body.Bytes()))
			assert.Equal(
				t,
				wantChecksum,
				gotChecksum,
				"x-amz-crc32 must match CRC32/IEEE of response body",
			)
		})
	}
}

func mustMarshal(t *testing.T, v any) string {
	t.Helper()
	data, err := json.Marshal(v)
	require.NoError(t, err)

	return string(data)
}

func TestHandler_TransactOps_Coverage(t *testing.T) {
	t.Parallel()
	handler := dynamodb.NewHandler(slog.Default())
	createTableHelper(t, handler.DB, "TransactTable", "pk")

	tests := []struct {
		body           any
		name           string
		action         string
		wantStatusCode int
	}{
		{
			name:   "TransactWriteItems_Empty",
			action: "TransactWriteItems",
			body: models.TransactWriteItemsInput{
				TransactItems: []models.TransactWriteItem{},
			},
			wantStatusCode: http.StatusBadRequest,
		},
		{
			name:           "TransactGetItems_Empty",
			action:         "TransactGetItems",
			body:           models.TransactGetItemsInput{TransactItems: []models.TransactGetItem{}},
			wantStatusCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			bodyBytes, _ := json.Marshal(tt.body)
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(bodyBytes))
			req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+tt.action)
			w := httptest.NewRecorder()
			echoHandler := handler.Handler()
			_ = serveEchoHandler(echoHandler, w, req)
			assert.Equal(t, tt.wantStatusCode, w.Code)
		})
	}
}
