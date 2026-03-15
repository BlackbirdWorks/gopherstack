package dynamodb_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
	"github.com/blackbirdworks/gopherstack/services/dynamodb/models"
)

func TestHandler_Realism(t *testing.T) {
	t.Parallel()

	memoryDB := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(memoryDB)
	echoHandler := handler.Handler()

	tableName := "RealismTable"
	createTableHelper(t, memoryDB, tableName, "pk", "sk")

	// Create large items (approx 100KB each)
	// DynamoDB item size overhead is not perfectly simulated but 100KB strings will definitely trigger 1MB.
	largeString := strings.Repeat("a", 100*1024)

	for i := 0; i < 20; i++ {
		putInput := models.PutItemInput{
			TableName: tableName,
			Item: map[string]any{
				"pk":   map[string]any{"S": "pk1"},
				"sk":   map[string]any{"S": fmt.Sprintf("sk%03d", i)},
				"data": map[string]any{"S": largeString},
			},
		}
		sdkPut, _ := models.ToSDKPutItemInput(&putInput)
		_, err := memoryDB.PutItem(context.Background(), sdkPut)
		require.NoError(t, err)
	}

	t.Run("Scan hits 1MB limit", func(t *testing.T) {
		reqBody := `{"TableName": "RealismTable"}`
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(reqBody))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Scan")
		w := httptest.NewRecorder()

		_ = serveEchoHandler(echoHandler, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var output struct {
			Items            []map[string]any `json:"Items"`
			LastEvaluatedKey map[string]any   `json:"LastEvaluatedKey"`
			ScannedCount     int              `json:"ScannedCount"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &output)
		require.NoError(t, err)

		// 20 items * 100KB = 2MB. Should truncate at approx 10 items.
		assert.Less(t, len(output.Items), 20, "Scan should have truncated results")
		assert.NotEmpty(t, output.LastEvaluatedKey, "Scan should return LastEvaluatedKey when truncated by size")
		assert.Equal(t, len(output.Items), output.ScannedCount, "ScannedCount should match Items len when no filter")
	})

	t.Run("Query hits 1MB limit", func(t *testing.T) {
		reqBody := `{
			"TableName": "RealismTable",
			"KeyConditionExpression": "pk = :pk",
			"ExpressionAttributeValues": {
				":pk": {"S": "pk1"}
			}
		}`
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewBufferString(reqBody))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.Query")
		w := httptest.NewRecorder()

		_ = serveEchoHandler(echoHandler, w, req)
		require.Equal(t, http.StatusOK, w.Code)

		var output struct {
			Items            []map[string]any `json:"Items"`
			LastEvaluatedKey map[string]any   `json:"LastEvaluatedKey"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &output)
		require.NoError(t, err)

		assert.Less(t, len(output.Items), 20, "Query should have truncated results")
		assert.NotEmpty(t, output.LastEvaluatedKey, "Query should return LastEvaluatedKey when truncated by size")
	})

	t.Run("BatchGetItem hits 16MB limit", func(t *testing.T) {
		// 100 items of 200KB = 20MB.
		largeString200 := strings.Repeat("b", 200*1024)
		for i := 0; i < 90; i++ {
			putInput := models.PutItemInput{
				TableName: tableName,
				Item: map[string]any{
					"pk":   map[string]any{"S": "batchpk"},
					"sk":   map[string]any{"S": fmt.Sprintf("sk%03d", i)},
					"data": map[string]any{"S": largeString200},
				},
			}
			sdkPut, _ := models.ToSDKPutItemInput(&putInput)
			_, err := memoryDB.PutItem(context.Background(), sdkPut)
			require.NoError(t, err)
		}

		// Request all 90 items
		var keys []map[string]any
		for i := 0; i < 90; i++ {
			keys = append(keys, map[string]any{
				"pk": map[string]any{"S": "batchpk"},
				"sk": map[string]any{"S": fmt.Sprintf("sk%03d", i)},
			})
		}

		batchInput := struct {
			RequestItems map[string]any `json:"RequestItems"`
		}{
			RequestItems: map[string]any{
				tableName: map[string]any{
					"Keys": keys,
				},
			},
		}
		body, _ := json.Marshal(batchInput)
		req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(body))
		req.Header.Set("X-Amz-Target", "DynamoDB_20120810.BatchGetItem")
		w := httptest.NewRecorder()

		_ = serveEchoHandler(echoHandler, w, req)
		if w.Code != http.StatusOK {
			t.Logf("Error body: %s", w.Body.String())
		}
		require.Equal(t, http.StatusOK, w.Code)

		var output struct {
			Responses       map[string][]map[string]any `json:"Responses"`
			UnprocessedKeys map[string]any               `json:"UnprocessedKeys"`
		}
		err := json.Unmarshal(w.Body.Bytes(), &output)
		require.NoError(t, err)

		// 90 * 200KB = 18MB. 16MB limit should trigger.
		totalReturned := len(output.Responses[tableName])
		assert.Less(t, totalReturned, 90, "BatchGetItem should have truncated results")
		assert.NotEmpty(t, output.UnprocessedKeys, "BatchGetItem should return UnprocessedKeys when size limit hit")

		unprocessed := output.UnprocessedKeys[tableName].(map[string]any)["Keys"].([]any)
		assert.Equal(t, 90, totalReturned+len(unprocessed), "Total items requested should match returned + unprocessed")
	})
}
