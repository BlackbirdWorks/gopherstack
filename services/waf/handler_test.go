package waf_test

// handler_test.go holds cross-cutting test helpers shared by every other
// *_test.go file in this package (constructing a handler, issuing raw
// dispatch calls, fetching a change token, decoding an error response) plus
// dispatch-level tests that aren't specific to any single resource family.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/waf"
)

// --- helpers ---

func newWAFHandler(t *testing.T) *waf.Handler {
	t.Helper()

	return waf.NewHandler(waf.NewInMemoryBackend("123456789012", "us-east-1"))
}

func wafDo(t *testing.T, h *waf.Handler, action string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var b []byte
	if body != nil {
		var err error
		b, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(b))
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	req.Header.Set("X-Amz-Target", "AWSWAF_20150824."+action)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))

	return rec
}

func wafGetToken(t *testing.T, h *waf.Handler) string {
	t.Helper()

	rec := wafDo(t, h, "GetChangeToken", nil)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	token, ok := resp["ChangeToken"].(string)
	require.True(t, ok)
	require.NotEmpty(t, token)

	return token
}

func errType(t *testing.T, body []byte) string {
	t.Helper()

	var resp map[string]any
	require.NoError(t, json.Unmarshal(body, &resp))

	typ, _ := resp["__type"].(string)

	return typ
}

// --- dispatch-level tests ---

func TestWAF_UnknownOperation_Returns400(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, "/", nil)
	req.Header.Set("X-Amz-Target", "AWSWAF_20150824.DoSomethingImaginary")
	req.Header.Set("Content-Type", "application/x-amz-json-1.1")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	c.SetRequest(req)
	require.NoError(t, h.Handler()(c))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestWAF_HandlerOpsNonZero(t *testing.T) {
	t.Parallel()

	h := newWAFHandler(t)
	assert.Greater(t, waf.HandlerOpsLen(h), 40,
		"WAF Classic handler should implement at least 40 operations")
}
