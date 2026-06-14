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

func makeParityRequest(
	t *testing.T,
	h *dynamodb.DynamoDBHandler,
	target string,
	body []byte,
) *httptest.ResponseRecorder {
	t.Helper()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
	req.Header.Set("X-Amz-Target", target)
	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	ctx := logger.Save(req.Context(), slog.Default())
	*req = *req.WithContext(ctx)

	e := echo.New()
	w := httptest.NewRecorder()
	c := e.NewContext(req, w)
	_ = h.Handler()(c)

	return w
}

func TestParity_Query_ConsistentRead_GSI_Rejected(t *testing.T) {
	t.Parallel()

	type queryBody struct {
		ExpressionAttributeValues map[string]any `json:"ExpressionAttributeValues"`
		TableName                 string         `json:"TableName"`
		IndexName                 string         `json:"IndexName,omitempty"`
		KeyConditionExpression    string         `json:"KeyConditionExpression"`
		ConsistentRead            bool           `json:"ConsistentRead"`
	}

	createTableBody, err := json.Marshal(map[string]any{
		"TableName": "Users",
		"KeySchema": []map[string]any{
			{"AttributeName": "pk", "KeyType": "HASH"},
		},
		"AttributeDefinitions": []map[string]any{
			{"AttributeName": "pk", "AttributeType": "S"},
			{"AttributeName": "email", "AttributeType": "S"},
		},
		"GlobalSecondaryIndexes": []map[string]any{
			{
				"IndexName": "EmailIndex",
				"KeySchema": []map[string]any{
					{"AttributeName": "email", "KeyType": "HASH"},
				},
				"Projection": map[string]any{"ProjectionType": "ALL"},
				"ProvisionedThroughput": map[string]any{
					"ReadCapacityUnits":  1,
					"WriteCapacityUnits": 1,
				},
			},
		},
		"BillingMode": "PAY_PER_REQUEST",
	})
	require.NoError(t, err)

	putItemBody, err := json.Marshal(map[string]any{
		"TableName": "Users",
		"Item": map[string]any{
			"pk":    map[string]any{"S": "user1"},
			"email": map[string]any{"S": "test@example.com"},
		},
	})
	require.NoError(t, err)

	tests := []struct {
		query            queryBody
		wantBodyContains []string
		name             string
		wantStatus       int
	}{
		{
			name: "gsi_consistentread_true_rejected",
			query: queryBody{
				TableName:              "Users",
				IndexName:              "EmailIndex",
				ConsistentRead:         true,
				KeyConditionExpression: "email = :e",
				ExpressionAttributeValues: map[string]any{
					":e": map[string]any{"S": "test@example.com"},
				},
			},
			wantStatus:       http.StatusBadRequest,
			wantBodyContains: []string{"ValidationException", "Consistent reads"},
		},
		{
			name: "gsi_consistentread_false_accepted",
			query: queryBody{
				TableName:              "Users",
				IndexName:              "EmailIndex",
				ConsistentRead:         false,
				KeyConditionExpression: "email = :e",
				ExpressionAttributeValues: map[string]any{
					":e": map[string]any{"S": "test@example.com"},
				},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: nil,
		},
		{
			name: "no_index_consistentread_true_accepted",
			query: queryBody{
				TableName:              "Users",
				ConsistentRead:         true,
				KeyConditionExpression: "pk = :p",
				ExpressionAttributeValues: map[string]any{
					":p": map[string]any{"S": "user1"},
				},
			},
			wantStatus:       http.StatusOK,
			wantBodyContains: nil,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			db := dynamodb.NewInMemoryDB()
			h := dynamodb.NewHandler(db)

			// Create table with GSI.
			w := makeParityRequest(t, h, "DynamoDB_20120810.CreateTable", createTableBody)
			require.Equal(t, http.StatusOK, w.Code, "CreateTable failed: %s", w.Body.String())

			// Put an item.
			w = makeParityRequest(t, h, "DynamoDB_20120810.PutItem", putItemBody)
			require.Equal(t, http.StatusOK, w.Code, "PutItem failed: %s", w.Body.String())

			// Execute query.
			queryRaw, marshalErr := json.Marshal(tc.query)
			require.NoError(t, marshalErr)
			w = makeParityRequest(t, h, "DynamoDB_20120810.Query", queryRaw)

			assert.Equal(t, tc.wantStatus, w.Code)
			for _, want := range tc.wantBodyContains {
				assert.Contains(t, w.Body.String(), want)
			}
		})
	}
}
