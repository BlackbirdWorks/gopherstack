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

func redshiftDataRequest(t *testing.T, target string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+"/", bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "RedshiftData."+target)
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/redshift-data/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=fakesignature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func redshiftDataReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_RedshiftData_ExecuteStatement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 1",
		"ClusterIdentifier": "integ-cluster",
		"Database":          "dev",
	})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var respObj map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &respObj))
	assert.NotEmpty(t, respObj["Id"], "expected statement ID in response")
}

func TestIntegration_RedshiftData_DescribeStatement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	execResp := redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 2",
		"ClusterIdentifier": "integ-cluster",
		"Database":          "dev",
	})
	execBody := redshiftDataReadBody(t, execResp)
	require.Equal(t, http.StatusOK, execResp.StatusCode, "body: %s", execBody)

	var execObj map[string]any
	require.NoError(t, json.Unmarshal([]byte(execBody), &execObj))

	id := execObj["Id"].(string)

	resp := redshiftDataRequest(t, "DescribeStatement", map[string]any{"Id": id})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var descObj map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &descObj))
	assert.Equal(t, id, descObj["Id"])
	assert.Equal(t, "FINISHED", descObj["Status"])
}

func TestIntegration_RedshiftData_GetStatementResult(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	execResp := redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 3",
		"ClusterIdentifier": "integ-cluster",
		"Database":          "dev",
	})
	execBody := redshiftDataReadBody(t, execResp)
	require.Equal(t, http.StatusOK, execResp.StatusCode, "body: %s", execBody)

	var execObj map[string]any
	require.NoError(t, json.Unmarshal([]byte(execBody), &execObj))

	id := execObj["Id"].(string)

	resp := redshiftDataRequest(t, "GetStatementResult", map[string]any{"Id": id})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Records")
}

func TestIntegration_RedshiftData_ListStatements(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	redshiftDataRequest(t, "ExecuteStatement", map[string]any{
		"Sql":               "SELECT 4",
		"ClusterIdentifier": "integ-cluster",
		"Database":          "dev",
	})

	resp := redshiftDataRequest(t, "ListStatements", map[string]any{})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Statements")
}

func TestIntegration_RedshiftData_BatchExecuteStatement(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftDataRequest(t, "BatchExecuteStatement", map[string]any{
		"Sqls":              []string{"SELECT 1", "SELECT 2"},
		"ClusterIdentifier": "integ-cluster",
		"Database":          "dev",
	})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var respObj map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &respObj))
	assert.NotEmpty(t, respObj["Id"])
}

func TestIntegration_RedshiftData_CancelStatement_NotFound(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftDataRequest(t, "CancelStatement", map[string]any{
		"Id": "nonexistent-statement-id",
	})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
}

func TestIntegration_RedshiftData_ListDatabases(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftDataRequest(t, "ListDatabases", map[string]any{})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Databases")
}

func TestIntegration_RedshiftData_ListSchemas(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftDataRequest(t, "ListSchemas", map[string]any{})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Schemas")
}

func TestIntegration_RedshiftData_ListTables(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := redshiftDataRequest(t, "ListTables", map[string]any{})
	body := redshiftDataReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Tables")
}
