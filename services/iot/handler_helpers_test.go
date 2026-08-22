package iot_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws/protocol/restjson"
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
			require.Contains(t, first, "__type")
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
		})
	}
}

// TestReadBodyMalformed_RealSDKClient_DecodesInvalidRequestException proves
// a malformed-body 400 response is decodable by the real aws-sdk-go-v2
// error path, not just by a raw json.Unmarshal that would accept any key
// name. Before this fix every one of these ~48 call sites (readBody and its
// direct json.NewDecoder call sites) wrote {"error": msg}; restjson.GetErrorInfo
// (aws-sdk-go-v2/aws/protocol/restjson/decoder_util.go), the function the
// real generated deserializer calls, only reads "Code"/"__type"/"Message"
// (matched case-insensitively) -- "error" matches none of them, so every
// client-visible error code and message collapsed to "UnknownError"
// regardless of the real failure, exactly the gopherstack-aitg class fixed
// in services/iotdataplane.
func TestReadBodyMalformed_RealSDKClient_DecodesInvalidRequestException(t *testing.T) {
	t.Parallel()

	h := iot.NewHandler(iot.NewInMemoryBackend(), nil)

	malformed := []byte(`{"unterminated": "json"`)
	req := httptest.NewRequest(http.MethodPost, "/billing-groups/bg-1", bytes.NewReader(malformed))
	req.Header.Set(echo.HeaderContentType, echo.MIMEApplicationJSON)
	rec := httptest.NewRecorder()

	e := echo.New()
	e.Any("/*", h.Handler())
	e.ServeHTTP(rec, req)

	require.Equal(t, http.StatusBadRequest, rec.Code)

	errorType, message, err := restjson.GetErrorInfo(json.NewDecoder(rec.Body))
	require.NoError(t, err)
	assert.Equal(t, "InvalidRequestException", errorType,
		"a real client must decode a modeled error code, not fall through to UnknownError")
	assert.NotEmpty(t, message, "a real client must decode the real failure message, not lose it")
}
