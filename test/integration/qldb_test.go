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

func qldbRequest(t *testing.T, method, path string, body any) *http.Response {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req, err := http.NewRequestWithContext(t.Context(), method, endpoint+path, bytes.NewReader(bodyBytes))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "AWS4-HMAC-SHA256 Credential=test/20230101/us-east-1/qldb/aws4_request, "+
		"SignedHeaders=host;x-amz-date, Signature=fakesignature")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	return resp
}

func qldbReadBody(t *testing.T, resp *http.Response) string {
	t.Helper()
	defer resp.Body.Close()

	data, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return string(data)
}

func TestIntegration_QLDB_CreateLedger(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	resp := qldbRequest(t, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "integ-ledger",
		"PermissionsMode": "ALLOW_ALL",
	})
	body := qldbReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "integ-ledger")
}

func TestIntegration_QLDB_ListLedgers(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	qldbRequest(t, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "list-ledger-integ",
		"PermissionsMode": "ALLOW_ALL",
	})

	resp := qldbRequest(t, http.MethodGet, "/ledgers", nil)
	body := qldbReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "Ledgers")
}

func TestIntegration_QLDB_DescribeLedger(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	qldbRequest(t, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "describe-ledger-integ",
		"PermissionsMode": "ALLOW_ALL",
	})

	resp := qldbRequest(t, http.MethodGet, "/ledgers/describe-ledger-integ", nil)
	body := qldbReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "describe-ledger-integ")
}

func TestIntegration_QLDB_UpdateLedger(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	qldbRequest(t, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "update-ledger-integ",
		"PermissionsMode": "ALLOW_ALL",
	})

	resp := qldbRequest(t, http.MethodPatch, "/ledgers/update-ledger-integ", map[string]any{
		"DeletionProtection": false,
	})
	body := qldbReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Contains(t, body, "update-ledger-integ")
}

func TestIntegration_QLDB_DeleteLedger(t *testing.T) {
	t.Parallel()
	dumpContainerLogsOnFailure(t)

	qldbRequest(t, http.MethodPost, "/ledgers", map[string]any{
		"Name":            "delete-ledger-integ",
		"PermissionsMode": "ALLOW_ALL",
	})

	resp := qldbRequest(t, http.MethodDelete, "/ledgers/delete-ledger-integ", nil)
	body := qldbReadBody(t, resp)

	assert.Equal(t, http.StatusOK, resp.StatusCode, "body: %s", body)
	assert.Empty(t, body)
}
