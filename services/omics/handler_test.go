package omics_test

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/omics"
)

func newTestHandler(t *testing.T) *omics.Handler {
	t.Helper()

	backend := omics.NewInMemoryBackend("000000000000", "us-east-1")

	return omics.NewHandler(backend)
}

// testRunBatchRoleArn is the fixed role ARN used by every startRunBatchBody call in
// this package's tests.
const testRunBatchRoleArn = "arn:aws:iam::000000000000:role/role"

// startRunBatchBody builds a real-shaped StartRunBatch request body (batchRunSettings/
// defaultRunSetting/requestId, not the flat workflowId/roleArn/name shape a real client
// never sends) with a single inline run setting.
func startRunBatchBody(batchName, workflowID string) map[string]any {
	return map[string]any{
		"requestId": "req-" + batchName,
		"batchName": batchName,
		"defaultRunSetting": map[string]any{
			"roleArn":    testRunBatchRoleArn,
			"workflowId": workflowID,
		},
		"batchRunSettings": map[string]any{
			"inlineSettings": []map[string]any{{"runSettingId": "s1"}},
		},
	}
}

func doRequest(t *testing.T, h *omics.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()

	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

func doRequestRaw(
	t *testing.T, h *omics.Handler, method, path, contentType string, body []byte,
) *httptest.ResponseRecorder {
	t.Helper()

	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}
