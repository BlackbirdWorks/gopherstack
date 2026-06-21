package dynamodb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

// doDDBRequest issues a single DynamoDB JSON request against the handler and
// returns the HTTP status code and decoded JSON body.
func doDDBRequest(
	t *testing.T,
	handler *dynamodb.DynamoDBHandler,
	target, body string,
) (int, map[string]any) {
	t.Helper()

	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(body))
	req.Header.Set("X-Amz-Target", "DynamoDB_20120810."+target)
	w := httptest.NewRecorder()

	require.NoError(t, serveEchoHandler(handler.Handler(), w, req))

	resp := w.Result()
	defer resp.Body.Close()

	var decoded map[string]any
	if w.Body.Len() > 0 {
		require.NoError(t, json.Unmarshal(w.Body.Bytes(), &decoded))
	}

	return resp.StatusCode, decoded
}

// TestConditionCheckFailure_ReturnsItem verifies that a failed conditional
// PutItem/UpdateItem/DeleteItem returns the existing item in the
// ConditionalCheckFailedException body when
// ReturnValuesOnConditionCheckFailure=ALL_OLD (AWS optimistic-locking parity).
func TestConditionCheckFailure_ReturnsItem(t *testing.T) {
	t.Parallel()

	const table = "CondTable"

	// Each op fails its condition against an existing item {pk:"a", v:"1"}.
	cases := []struct {
		name   string
		target string
		body   string
	}{
		{
			name:   "PutItem",
			target: "PutItem",
			body: mustMarshal(t, models.PutItemInput{
				TableName: table,
				Item: map[string]any{
					"pk": map[string]any{"S": "a"},
					"v":  map[string]any{"S": "2"},
				},
				ConditionExpression:                 "attribute_not_exists(pk)",
				ReturnValuesOnConditionCheckFailure: "ALL_OLD",
			}),
		},
		{
			name:   "UpdateItem",
			target: "UpdateItem",
			body: mustMarshal(t, models.UpdateItemInput{
				TableName:           table,
				Key:                 map[string]any{"pk": map[string]any{"S": "a"}},
				UpdateExpression:    "SET v = :new",
				ConditionExpression: "v = :expected",
				ExpressionAttributeValues: map[string]any{
					":new":      map[string]any{"S": "9"},
					":expected": map[string]any{"S": "wrong"},
				},
				ReturnValuesOnConditionCheckFailure: "ALL_OLD",
			}),
		},
		{
			name:   "DeleteItem",
			target: "DeleteItem",
			body: mustMarshal(t, models.DeleteItemInput{
				TableName:           table,
				Key:                 map[string]any{"pk": map[string]any{"S": "a"}},
				ConditionExpression: "v = :expected",
				ExpressionAttributeValues: map[string]any{
					":expected": map[string]any{"S": "wrong"},
				},
				ReturnValuesOnConditionCheckFailure: "ALL_OLD",
			}),
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			backend := dynamodb.NewInMemoryDB()
			handler := dynamodb.NewHandler(backend)
			createTableHelper(t, backend, table, "pk")

			put := models.PutItemInput{
				TableName: table,
				Item: map[string]any{
					"pk": map[string]any{"S": "a"},
					"v":  map[string]any{"S": "1"},
				},
			}
			sdkPut, _ := models.ToSDKPutItemInput(&put)
			_, err := backend.PutItem(t.Context(), sdkPut)
			require.NoError(t, err)

			status, decoded := doDDBRequest(t, handler, tc.target, tc.body)

			assert.Equal(t, http.StatusBadRequest, status)
			assert.Contains(t, decoded["__type"], "ConditionalCheckFailedException")

			item, ok := decoded["Item"].(map[string]any)
			require.True(t, ok, "error body should contain the existing Item, got: %v", decoded)
			// The returned item must be the full existing item in wire form.
			assert.Equal(t, map[string]any{"S": "a"}, item["pk"])
			assert.Equal(t, map[string]any{"S": "1"}, item["v"])
		})
	}
}

// TestConditionCheckFailure_OmitsItemByDefault verifies that without
// ReturnValuesOnConditionCheckFailure=ALL_OLD the error carries no Item.
func TestConditionCheckFailure_OmitsItemByDefault(t *testing.T) {
	t.Parallel()

	const table = "CondTableNone"

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, table, "pk")

	put := models.PutItemInput{
		TableName: table,
		Item:      map[string]any{"pk": map[string]any{"S": "a"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&put)
	_, err := backend.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	body := mustMarshal(t, models.PutItemInput{
		TableName:           table,
		Item:                map[string]any{"pk": map[string]any{"S": "a"}},
		ConditionExpression: "attribute_not_exists(pk)",
	})

	status, decoded := doDDBRequest(t, handler, "PutItem", body)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, decoded["__type"], "ConditionalCheckFailedException")
	_, hasItem := decoded["Item"]
	assert.False(t, hasItem, "error body must not include Item when ALL_OLD not requested")
}

// TestTransactWrite_CancellationReasonItemWireFormat verifies that a cancelled
// TransactWriteItems returns the existing item in CancellationReasons[].Item in
// DynamoDB wire form ({"S":...}), not the smithy SDK union form ({"Value":...}).
func TestTransactWrite_CancellationReasonItemWireFormat(t *testing.T) {
	t.Parallel()

	const table = "TxnCondTable"

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, table, "pk")

	put := models.PutItemInput{
		TableName: table,
		Item:      map[string]any{"pk": map[string]any{"S": "a"}, "v": map[string]any{"S": "1"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&put)
	_, err := backend.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	body := mustMarshal(t, models.TransactWriteItemsInput{
		TransactItems: []models.TransactWriteItem{
			{
				Put: &models.PutItemInput{
					TableName: table,
					Item: map[string]any{
						"pk": map[string]any{"S": "a"},
						"v":  map[string]any{"S": "2"},
					},
					ConditionExpression:                 "attribute_not_exists(pk)",
					ReturnValuesOnConditionCheckFailure: "ALL_OLD",
				},
			},
		},
	})

	status, decoded := doDDBRequest(t, handler, "TransactWriteItems", body)

	assert.Equal(t, http.StatusBadRequest, status)
	assert.Contains(t, decoded["__type"], "TransactionCanceledException")

	reasons, ok := decoded["CancellationReasons"].([]any)
	require.True(t, ok, "expected CancellationReasons, got: %v", decoded)
	require.Len(t, reasons, 1)
	reason, _ := reasons[0].(map[string]any)
	item, ok := reason["Item"].(map[string]any)
	require.True(t, ok, "cancellation reason should carry the existing Item, got: %v", reason)
	assert.Equal(t, map[string]any{"S": "1"}, item["v"])
}

// TestDeleteItem_ReturnValuesAllOld verifies that DeleteItem with
// ReturnValues=ALL_OLD returns the deleted item's attributes over the wire.
func TestDeleteItem_ReturnValuesAllOld(t *testing.T) {
	t.Parallel()

	const table = "DelRetTable"

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)
	createTableHelper(t, backend, table, "pk")

	put := models.PutItemInput{
		TableName: table,
		Item:      map[string]any{"pk": map[string]any{"S": "a"}, "v": map[string]any{"S": "1"}},
	}
	sdkPut, _ := models.ToSDKPutItemInput(&put)
	_, err := backend.PutItem(t.Context(), sdkPut)
	require.NoError(t, err)

	body := mustMarshal(t, models.DeleteItemInput{
		TableName:    table,
		Key:          map[string]any{"pk": map[string]any{"S": "a"}},
		ReturnValues: "ALL_OLD",
	})

	status, decoded := doDDBRequest(t, handler, "DeleteItem", body)

	require.Equal(t, http.StatusOK, status)
	attrs, ok := decoded["Attributes"].(map[string]any)
	require.True(t, ok, "DeleteItem ALL_OLD should return Attributes, got: %v", decoded)
	assert.Equal(t, map[string]any{"S": "1"}, attrs["v"])
}
