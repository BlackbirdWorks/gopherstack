package cosmosdb_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"
)

// newEchoContextForRequest wraps req in an *echo.Context for tests that call
// Handler methods (ExtractOperation/ExtractResource) directly rather than
// through the full HTTP handler chain.
func newEchoContextForRequest(t *testing.T, req *http.Request) *echo.Context {
	t.Helper()

	e := echo.New()
	rec := httptest.NewRecorder()

	return e.NewContext(req, rec)
}

// decodeJSONBody decodes raw JSON into a map[string]any using
// json.Decoder.UseNumber, mirroring the package's own decodeJSONObject so
// tests can construct document bodies carrying exact-precision json.Number
// values (e.g. integers beyond float64's 53-bit mantissa) instead of a
// plain map literal, which can only ever hold a float64 or an explicitly
// constructed json.Number.
func decodeJSONBody(t *testing.T, raw string) map[string]any {
	t.Helper()

	dec := json.NewDecoder(bytes.NewReader([]byte(raw)))
	dec.UseNumber()

	var m map[string]any

	require.NoError(t, dec.Decode(&m))

	return m
}
