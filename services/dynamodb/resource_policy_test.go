package dynamodb_test

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"
)

func TestDeleteResourcePolicy(t *testing.T) {
	t.Parallel()

	backend := dynamodb.NewInMemoryDB()
	handler := dynamodb.NewHandler(backend)

	createTableHelper(t, backend, "MyTable", "pk")

	// Get the table ARN from DescribeTable.
	code, resp := invokeOp(t, handler, "DescribeTable", map[string]any{"TableName": "MyTable"})
	require.Equal(t, http.StatusOK, code)
	tableDesc, _ := resp["Table"].(map[string]any)
	tableARN, _ := tableDesc["TableArn"].(string)
	require.NotEmpty(t, tableARN)

	code, _ = invokeOp(t, handler, "DeleteResourcePolicy", map[string]any{
		"ResourceArn": tableARN,
	})
	assert.Equal(t, http.StatusOK, code)
}

func getTestTableARN(
	t *testing.T,
	backend *dynamodb.InMemoryDB,
	handler *dynamodb.DynamoDBHandler,
	tableName string,
) string {
	t.Helper()

	createTableHelper(t, backend, tableName, "pk")

	code, resp := invokeOp(t, handler, "DescribeTable", map[string]any{"TableName": tableName})
	require.Equal(t, http.StatusOK, code)

	tableDesc, _ := resp["Table"].(map[string]any)
	tableARN, _ := tableDesc["TableArn"].(string)
	require.NotEmpty(t, tableARN)

	return tableARN
}

func TestResourcePolicy(t *testing.T) {
	t.Parallel()

	t.Run("GetResourcePolicy_success", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPGetTable")
		code, _ := invokeOp(
			t,
			handler,
			"GetResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("GetResourcePolicy_missing_arn", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		code, resp := invokeOp(t, handler, "GetResourcePolicy", map[string]any{"ResourceArn": ""})
		assert.Equal(t, http.StatusBadRequest, code)
		bodyBytes, _ := json.Marshal(resp)
		assert.Contains(t, string(bodyBytes), "ValidationException")
	})

	t.Run("PutResourcePolicy_success", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPPutTable")
		code, _ := invokeOp(t, handler, "PutResourcePolicy", map[string]any{
			"ResourceArn": tableARN,
			"Policy":      `{"Version":"2012-10-17","Statement":[]}`,
		})
		assert.Equal(t, http.StatusOK, code)

		// Verify round-trip: GetResourcePolicy returns the stored policy.
		code2, resp2 := invokeOp(
			t,
			handler,
			"GetResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusOK, code2)
		bodyBytes, _ := json.Marshal(resp2)
		assert.Contains(t, string(bodyBytes), "2012-10-17")
	})

	t.Run("PutResourcePolicy_missing_policy", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPMissingPolicyTable")
		code, resp := invokeOp(
			t,
			handler,
			"PutResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusBadRequest, code)
		bodyBytes, _ := json.Marshal(resp)
		assert.Contains(t, string(bodyBytes), "ValidationException")
	})

	t.Run("DeleteResourcePolicy_success", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		tableARN := getTestTableARN(t, backend, handler, "RPDeleteTable")
		code, _ := invokeOp(
			t,
			handler,
			"DeleteResourcePolicy",
			map[string]any{"ResourceArn": tableARN},
		)
		assert.Equal(t, http.StatusOK, code)
	})

	t.Run("DeleteResourcePolicy_missing_arn", func(t *testing.T) {
		t.Parallel()
		backend := dynamodb.NewInMemoryDB()
		handler := dynamodb.NewHandler(backend)
		code, resp := invokeOp(
			t,
			handler,
			"DeleteResourcePolicy",
			map[string]any{"ResourceArn": ""},
		)
		assert.Equal(t, http.StatusBadRequest, code)
		bodyBytes, _ := json.Marshal(resp)
		assert.Contains(t, string(bodyBytes), "ValidationException")
	})
}
