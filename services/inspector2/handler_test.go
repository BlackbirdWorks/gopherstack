package inspector2_test

// Shared test helpers used across the handler_<family>_test.go files in this
// package: a fresh handler/backend pair, an HTTP round-trip helper, and small
// convenience wrappers used by many family tests (creating a filter, seeding
// a finding). Keeping these in one place avoids every family test file
// re-declaring its own near-identical fixture plumbing.

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"unicode"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/inspector2"
)

// sanitizeFilterName rewrites s so it satisfies the real Inspector2 filter
// name charset (alphanumeric, dot, underscore, dash), replacing every other
// rune with a dash. Used by table-driven tests that build a filter name from
// a free-text subtest name (which may contain spaces/parens/etc).
func sanitizeFilterName(s string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '.' || r == '_' || r == '-' {
			return r
		}

		return '-'
	}, s)
}

// newAuditBackend returns a fresh backend for account 123456789012 in us-east-1.
func newAuditBackend() *inspector2.InMemoryBackend {
	return inspector2.NewInMemoryBackend("123456789012", "us-east-1")
}

// newAuditHandler returns a fresh handler wrapping newAuditBackend().
func newAuditHandler(t *testing.T) *inspector2.Handler {
	t.Helper()

	return inspector2.NewHandler(newAuditBackend())
}

// auditDo issues an HTTP request against h and returns the recorded response.
func auditDo(t *testing.T, h *inspector2.Handler, method, path string, body any) *httptest.ResponseRecorder {
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

// auditCreateFilter creates a filter named name and returns its ARN.
func auditCreateFilter(t *testing.T, h *inspector2.Handler, name string) string {
	t.Helper()

	rec := auditDo(t, h, http.MethodPost, "/filters/create", map[string]any{
		"name":   name,
		"action": "NONE",
	})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, ok := resp["arn"].(string)
	require.True(t, ok)
	require.NotEmpty(t, arn)

	return arn
}

// inspector2UnmarshalError is a thin json.Unmarshal wrapper used by tests that
// assert on the shape of an error response body.
func inspector2UnmarshalError(t *testing.T, data []byte, v any) error {
	t.Helper()

	return json.Unmarshal(data, v)
}

// newParityBackend returns a fresh backend for account 000000000000 in
// us-east-1, used by the ListFindings-focused tests where the account ID
// appears in assertions.
func newParityBackend() *inspector2.InMemoryBackend {
	return inspector2.NewInMemoryBackend("000000000000", "us-east-1")
}

// newParityHandlerAndBackend returns a handler and its backing backend so
// tests can seed findings directly while also exercising the HTTP surface.
func newParityHandlerAndBackend(t *testing.T) (*inspector2.Handler, *inspector2.InMemoryBackend) {
	t.Helper()

	b := newParityBackend()

	return inspector2.NewHandler(b), b
}

// parityDo issues an HTTP request against h and returns the recorded response.
func parityDo(t *testing.T, h *inspector2.Handler, method, path string, body any) *httptest.ResponseRecorder {
	t.Helper()

	var bodyBytes []byte

	if body != nil {
		var err error
		bodyBytes, err = json.Marshal(body)
		require.NoError(t, err)
	}

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(bodyBytes))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// parityDoRaw is like parityDo but sends a raw (possibly malformed) body.
func parityDoRaw(t *testing.T, h *inspector2.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	require.NoError(t, h.Handler()(c))

	return rec
}

// parityListFindings calls POST /findings/list and returns the findings array.
func parityListFindings(t *testing.T, h *inspector2.Handler, body map[string]any) []any {
	t.Helper()

	rec := parityDo(t, h, http.MethodPost, "/findings/list", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	findings, ok := resp["findings"].([]any)
	if !ok {
		return []any{}
	}

	return findings
}

// parityListFindingsWithToken is parityListFindings but also returns nextToken.
func parityListFindingsWithToken(
	t *testing.T, h *inspector2.Handler, body map[string]any,
) ([]any, string) {
	t.Helper()

	rec := parityDo(t, h, http.MethodPost, "/findings/list", body)
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	findings, _ := resp["findings"].([]any)
	nextToken, _ := resp["nextToken"].(string)

	return findings, nextToken
}

// paritySeedFinding seeds a finding directly on the backend and returns its ARN.
func paritySeedFinding(
	t *testing.T,
	b *inspector2.InMemoryBackend,
	findingType, severity, status, title string,
) string {
	t.Helper()

	return inspector2.SeedFinding(
		b,
		findingType,
		severity,
		status,
		title,
		"Description for "+title,
		[]inspector2.FindingResource{{Type: "AWS_EC2_INSTANCE", ID: "i-12345678"}},
	)
}

// TestHandlerSupportedOperationsCount pins the total number of operations the
// handler advertises (13 core ops in handler.go + 62 extended ops in
// handler_routing.go), so an accidental drop while splitting files would fail
// loudly.
func TestHandlerSupportedOperationsCount(t *testing.T) {
	t.Parallel()

	h := newAuditHandler(t)

	assert.Equal(t, 75, inspector2.HandlerOpsLen(h))
}
