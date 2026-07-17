package medialive_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/medialive"
)

// newTestHandler constructs a Handler backed by a fresh InMemoryBackend, for
// use by every *_test.go file in this package.
func newTestHandler(t *testing.T) *medialive.Handler {
	t.Helper()

	backend := medialive.NewInMemoryBackend("000000000000", "us-east-1")

	return medialive.NewHandler(backend)
}

// doRequest drives a single HTTP request through h.Handler() and returns the
// recorded response.
func doRequest(
	t *testing.T,
	h *medialive.Handler,
	method, path string,
	body any,
) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error

		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")

	if body != nil {
		req.ContentLength = int64(len(bodyBytes))
	}

	rec := httptest.NewRecorder()
	e := echo.New()
	c := e.NewContext(req, rec)
	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

// decodeBody JSON-decodes a response body into a generic map.
func decodeBody(t *testing.T, body []byte) map[string]any {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	return resp
}

func createTestChannel(t *testing.T, h *medialive.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/channels", map[string]any{
		"name":         "test-channel",
		"channelClass": "STANDARD",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	ch := resp["channel"].(map[string]any)

	return ch["id"].(string)
}

func createTestInput(t *testing.T, h *medialive.Handler) string {
	t.Helper()

	rec := doRequest(t, h, http.MethodPost, "/prod/inputs", map[string]any{
		"name": "test-input",
		"type": "UDP_PUSH",
	})
	require.Equal(t, http.StatusCreated, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	inp := resp["input"].(map[string]any)

	return inp["id"].(string)
}

// TestAccountConfiguration covers DescribeAccountConfiguration/
// UpdateAccountConfiguration, which live in handler.go core (no dedicated
// resource family owns account-wide configuration).
func TestAccountConfiguration(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, http.MethodGet, "/prod/accountConfiguration", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	cfg := decodeBody(t, rec.Body.Bytes())["accountConfiguration"].(map[string]any)
	assert.Empty(t, cfg["kmsKeyId"])

	rec = doRequest(t, h, http.MethodPut, "/prod/accountConfiguration", map[string]any{
		"accountConfiguration": map[string]any{"kmsKeyId": "key-123"},
	})
	require.Equal(t, http.StatusOK, rec.Code)
	cfg = decodeBody(t, rec.Body.Bytes())["accountConfiguration"].(map[string]any)
	assert.Equal(t, "key-123", cfg["kmsKeyId"])

	rec = doRequest(t, h, http.MethodGet, "/prod/accountConfiguration", nil)
	require.Equal(t, http.StatusOK, rec.Code)
	cfg = decodeBody(t, rec.Body.Bytes())["accountConfiguration"].(map[string]any)
	assert.Equal(t, "key-123", cfg["kmsKeyId"])
}
