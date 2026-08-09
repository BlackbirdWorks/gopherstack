package omics_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestReadJSONMalformedWritesExactlyOneDocument guards against the same
// double-write shape as gopherstack-kin0 (services/iot): readJSON must not
// write a 400 and then let the caller write a second response body.
func TestReadJSONMalformedWritesExactlyOneDocument(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	malformed := []byte(`{"unterminated": "json"`)
	req := httptest.NewRequest(http.MethodPost, "/annotationStore", bytes.NewReader(malformed))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	e := echo.New()
	e.Any("/*", h.Handler())
	e.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusBadRequest, rec.Code)

	dec := json.NewDecoder(rec.Body)

	var first map[string]any
	require.NoError(t, dec.Decode(&first))
	require.Contains(t, first, "message")

	var second map[string]any
	err := dec.Decode(&second)
	require.ErrorIs(
		t,
		err,
		io.EOF,
		"response body must contain exactly one JSON document, got trailing data: %q",
		rec.Body.String(),
	)
}
