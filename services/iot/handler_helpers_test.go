package iot_test

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

	"github.com/blackbirdworks/gopherstack/services/iot"
)

// TestReadBodyMalformedWritesExactlyOneDocument guards against
// gopherstack-kin0: readBody must not write a 400 and then let the caller
// write a second response body for the same request.
func TestReadBodyMalformedWritesExactlyOneDocument(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		method string
		path   string
	}{
		{"checked call site since introduction", http.MethodPost, "/billing-groups/bg-1"},
		{"call site previously ignoring the error", http.MethodPut, "/commands/cmd-1"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := iot.NewHandler(iot.NewInMemoryBackend(), nil)

			malformed := []byte(`{"unterminated": "json"`)
			req := httptest.NewRequest(tt.method, tt.path, bytes.NewReader(malformed))
			req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
			rec := httptest.NewRecorder()

			e := echo.New()
			e.Any("/*", h.Handler())
			e.ServeHTTP(rec, req)

			assert.Equal(t, http.StatusBadRequest, rec.Code)

			dec := json.NewDecoder(rec.Body)

			var first map[string]any
			require.NoError(t, dec.Decode(&first))
			require.Contains(t, first, "error")

			var second map[string]any
			err := dec.Decode(&second)
			require.ErrorIs(
				t,
				err,
				io.EOF,
				"response body must contain exactly one JSON document, got trailing data: %q",
				rec.Body.String(),
			)
		})
	}
}
