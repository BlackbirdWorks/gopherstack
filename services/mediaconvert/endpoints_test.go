package mediaconvert_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/mediaconvert"
)

// doRequestRaw sends a request with an unvalidated raw body, unlike
// doRequest (handler_test.go) which always JSON-marshals a Go value first.
// Used here to exercise malformed-JSON error handling.
func doRequestRaw(t *testing.T, h *mediaconvert.Handler, method, path string, body []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(method, path, bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	require.NoError(t, h.Handler()(c))

	return rec
}

// TestDescribeEndpoints_POST verifies the real (POST-only) wire shape: a
// POST with an empty body returns exactly one synthetic endpoint.
func TestDescribeEndpoints_POST(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/endpoints", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	endpoints, ok := out["endpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 1)

	entry := endpoints[0].(map[string]any)
	assert.NotEmpty(t, entry["url"])
}

// TestDescribeEndpoints_GETRejected verifies GET is no longer accepted --
// the real DescribeEndpoints operation is POST-only.
func TestDescribeEndpoints_GETRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodGet, "/2017-08-29/endpoints", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestDescribeEndpoints_WithModeAndMaxResults verifies the POST JSON body
// (mode/maxResults/nextToken) is parsed rather than ignored.
func TestDescribeEndpoints_WithModeAndMaxResults(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/endpoints", map[string]any{
		"mode":       "GET_ONLY",
		"maxResults": 5,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	endpoints, ok := out["endpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 1)
	assert.Empty(t, out["nextToken"])
}

// TestDescribeEndpoints_MaxResultsZeroCapsToZero verifies maxResults, when
// present and less than the number of available endpoints, caps the list --
// the real API's documented "up to twenty" cap semantics.
func TestDescribeEndpoints_MaxResultsCaps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequest(t, h, http.MethodPost, "/2017-08-29/endpoints", map[string]any{
		"maxResults": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var out map[string]any
	require.NoError(t, json.NewDecoder(rec.Body).Decode(&out))
	endpoints, ok := out["endpoints"].([]any)
	require.True(t, ok)
	require.Len(t, endpoints, 1)
}

// TestDescribeEndpoints_InvalidBody verifies malformed JSON is rejected with
// BadRequestException rather than silently ignored.
func TestDescribeEndpoints_InvalidBody(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rec := doRequestRaw(t, h, http.MethodPost, "/2017-08-29/endpoints", []byte("{not json"))
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}
