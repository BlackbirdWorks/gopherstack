// Package awstest provides shared HTTP test helpers for service handler tests.
// The 300+ handler test files in this repository repeat the same echo.New() +
// httptest.NewRequest + httptest.NewRecorder + NewContext wiring; these helpers
// collapse it to a single call so tests focus on inputs and assertions rather
// than transport plumbing.
package awstest

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// NewContext builds an echo context and response recorder for an httptest
// request with the given method, path and body (body may be nil). The returned
// recorder captures the handler's response.
func NewContext(method, path string, body io.Reader) (*echo.Context, *httptest.ResponseRecorder) {
	e := echo.New()
	req := httptest.NewRequestWithContext(context.Background(), method, path, body)
	rec := httptest.NewRecorder()

	return e.NewContext(req, rec), rec
}

// NewJSONContext is like [NewContext] but marshals payload to JSON for the
// request body and sets Content-Type: application/json. A nil payload yields an
// empty body. It fails the test if marshalling fails.
func NewJSONContext(
	t *testing.T, method, path string, payload any,
) (*echo.Context, *httptest.ResponseRecorder) {
	t.Helper()

	var body io.Reader
	if payload != nil {
		raw, err := json.Marshal(payload)
		require.NoError(t, err)
		body = bytes.NewReader(raw)
	}

	c, rec := NewContext(method, path, body)
	c.Request().Header.Set("Content-Type", "application/json")

	return c, rec
}

// DecodeJSON unmarshals the recorder's body into v, failing the test on error.
func DecodeJSON(t *testing.T, rec *httptest.ResponseRecorder, v any) {
	t.Helper()

	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), v))
}
