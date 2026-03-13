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

// qldbSessionRequest sends a QLDB Session SendCommand request to the integration stack.
func qldbSessionRequest(t *testing.T, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, endpoint+"/", bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/x-amz-json-1.0")
	req.Header.Set("X-Amz-Target", "QLDBSession.SendCommand")
	// SigV4 signing name for QLDB Session is "qldb".
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/qldb/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=fakesignature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func qldbSessionReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_QLDBSession_StartSession(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := qldbSessionRequest(t, map[string]any{
		"StartSession": map[string]any{"LedgerName": "integ-ledger"},
	})
	body := qldbSessionReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)

	var result map[string]any
	require.NoError(t, json.Unmarshal([]byte(body), &result))
	startSession, ok := result["StartSession"].(map[string]any)
	require.True(t, ok, "StartSession should be in response body, got: %s", body)
	assert.NotEmpty(t, startSession["SessionToken"])
}

func TestIntegration_QLDBSession_StartAndCommitTransaction(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Start a session.
	startResp := qldbSessionRequest(t, map[string]any{
		"StartSession": map[string]any{"LedgerName": "integ-ledger-tx"},
	})
	startBody := qldbSessionReadBody(t, startResp)
	require.Equal(t, http.StatusOK, startResp.StatusCode, "body: %s", startBody)

	var startResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(startBody), &startResult))
	startSessionData, ok := startResult["StartSession"].(map[string]any)
	require.True(t, ok, "StartSession key missing in response: %s", startBody)
	sessionToken, ok := startSessionData["SessionToken"].(string)
	require.True(t, ok, "SessionToken missing or wrong type in response: %s", startBody)
	require.NotEmpty(t, sessionToken)

	// Start a transaction.
	txResp := qldbSessionRequest(t, map[string]any{
		"SessionToken":     sessionToken,
		"StartTransaction": map[string]any{},
	})
	txBody := qldbSessionReadBody(t, txResp)
	require.Equal(t, http.StatusOK, txResp.StatusCode, "body: %s", txBody)

	var txResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(txBody), &txResult))
	startTxData, ok := txResult["StartTransaction"].(map[string]any)
	require.True(t, ok, "StartTransaction key missing in response: %s", txBody)
	txID, ok := startTxData["TransactionId"].(string)
	require.True(t, ok, "TransactionId missing or wrong type in response: %s", txBody)
	require.NotEmpty(t, txID)

	// Commit the transaction.
	commitResp := qldbSessionRequest(t, map[string]any{
		"SessionToken": sessionToken,
		"CommitTransaction": map[string]any{
			"TransactionId": txID,
			"CommitDigest":  []byte("testdigest"),
		},
	})
	commitBody := qldbSessionReadBody(t, commitResp)
	assert.Equal(t, http.StatusOK, commitResp.StatusCode, "body: %s", commitBody)
	assert.Contains(t, commitBody, "CommitTransaction")
}

func TestIntegration_QLDBSession_EndSession(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	// Start a session.
	startResp := qldbSessionRequest(t, map[string]any{
		"StartSession": map[string]any{"LedgerName": "integ-ledger-end"},
	})
	startBody := qldbSessionReadBody(t, startResp)
	require.Equal(t, http.StatusOK, startResp.StatusCode, "body: %s", startBody)

	var startResult map[string]any
	require.NoError(t, json.Unmarshal([]byte(startBody), &startResult))
	startSessionData, ok := startResult["StartSession"].(map[string]any)
	require.True(t, ok, "StartSession key missing in response: %s", startBody)
	sessionToken, ok := startSessionData["SessionToken"].(string)
	require.True(t, ok, "SessionToken missing or wrong type in response: %s", startBody)
	require.NotEmpty(t, sessionToken)

	// End the session.
	endResp := qldbSessionRequest(t, map[string]any{
		"SessionToken": sessionToken,
		"EndSession":   map[string]any{},
	})
	endBody := qldbSessionReadBody(t, endResp)
	assert.Equal(t, http.StatusOK, endResp.StatusCode, "body: %s", endBody)
	assert.Contains(t, endBody, "EndSession")
}

func TestIntegration_QLDBSession_InvalidSessionToken(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := qldbSessionRequest(t, map[string]any{
		"SessionToken":     "invalid-token",
		"StartTransaction": map[string]any{},
	})
	body := qldbSessionReadBody(t, resp)

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "InvalidSessionException")
}
