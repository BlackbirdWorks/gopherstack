package integration_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func rdsdataRequest(t *testing.T, path string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+path, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/rds-data/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=fakesignature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func rdsdataReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_RDSData_ExecuteStatement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := rdsdataRequest(t, "/Execute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:integ-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:integ-secret",
		"sql":         "SELECT 1",
	})
	body := rdsdataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "records")
}

func TestIntegration_RDSData_BatchExecuteStatement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := rdsdataRequest(t, "/BatchExecute", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:integ-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:integ-secret",
		"sql":         "INSERT INTO test VALUES (:val)",
		"parameterSets": []any{
			[]any{map[string]any{"name": "val", "value": map[string]any{"stringValue": "a"}}},
			[]any{map[string]any{"name": "val", "value": map[string]any{"stringValue": "b"}}},
		},
	})
	body := rdsdataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "updateResults")
}

func TestIntegration_RDSData_TransactionLifecycle(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Begin a transaction.
	beginResp := rdsdataRequest(t, "/BeginTransaction", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:txn-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:txn-secret",
	})
	beginBody := rdsdataReadBody(t, beginResp)
	assert.Equal(t, http.StatusOK, beginResp.StatusCode, "body: %s", beginBody)
	assert.Contains(t, beginBody, "transactionId")

	// Parse the transaction ID.
	var beginResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(beginBody), &beginResult))
	txID, ok := beginResult["transactionId"].(string)
	require.True(t, ok, "transactionId should be a string")
	require.NotEmpty(t, txID)

	// Execute a statement within the transaction.
	execResp := rdsdataRequest(t, "/Execute", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:txn-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:txn-secret",
		"sql":           "SELECT 1",
		"transactionId": txID,
	})
	execBody := rdsdataReadBody(t, execResp)
	assert.Equal(t, http.StatusOK, execResp.StatusCode, "body: %s", execBody)

	// Commit the transaction.
	commitResp := rdsdataRequest(t, "/CommitTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:txn-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:txn-secret",
		"transactionId": txID,
	})
	commitBody := rdsdataReadBody(t, commitResp)
	assert.Equal(t, http.StatusOK, commitResp.StatusCode, "body: %s", commitBody)
	assert.Contains(t, commitBody, "transactionStatus")
}

func TestIntegration_RDSData_RollbackTransaction(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Begin a transaction.
	beginResp := rdsdataRequest(t, "/BeginTransaction", map[string]any{
		"resourceArn": "arn:aws:rds:us-east-1:000000000000:cluster:rollback-cluster",
		"secretArn":   "arn:aws:secretsmanager:us-east-1:000000000000:secret:rollback-secret",
	})
	beginBody := rdsdataReadBody(t, beginResp)
	require.Equal(t, http.StatusOK, beginResp.StatusCode, "body: %s", beginBody)

	var beginResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(beginBody), &beginResult))
	txID, ok := beginResult["transactionId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, txID)

	// Rollback the transaction.
	rollbackResp := rdsdataRequest(t, "/RollbackTransaction", map[string]any{
		"resourceArn":   "arn:aws:rds:us-east-1:000000000000:cluster:rollback-cluster",
		"secretArn":     "arn:aws:secretsmanager:us-east-1:000000000000:secret:rollback-secret",
		"transactionId": txID,
	})
	rollbackBody := rdsdataReadBody(t, rollbackResp)
	assert.Equal(t, http.StatusOK, rollbackResp.StatusCode, "body: %s", rollbackBody)
	assert.Contains(t, rollbackBody, "transactionStatus")
}
