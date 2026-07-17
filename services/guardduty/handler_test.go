package guardduty_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/guardduty"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

func newTestBackend() *guardduty.InMemoryBackend {
	return guardduty.NewInMemoryBackend("123456789012", "us-east-1")
}

func newTestHandler(t *testing.T) *guardduty.Handler {
	t.Helper()

	return guardduty.NewHandler(newTestBackend())
}

func doRequest(t *testing.T, h *guardduty.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte

	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func createTestDetector(t *testing.T, h *guardduty.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/detector", map[string]any{
		"enable": true,
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	id, ok := resp["detectorId"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	return id
}
