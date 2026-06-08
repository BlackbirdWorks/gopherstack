package httputils_test

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputils"
)

// TestReadBody_CachedBodyClose exercises the Close() method on bodyReadCloser
// by reading the body twice (second read hits the cached path) and then
// explicitly closing via io.ReadCloser interface.
func TestReadBody_CachedBodyClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		content []byte
	}{
		{
			name:    "close_after_cached_read",
			content: []byte("cached body content"),
		},
		{
			name:    "close_on_empty_body",
			content: []byte{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(tt.content))

			// First read caches the body replacing r.Body with bodyReadCloser
			body1, err := httputils.ReadBody(req)
			require.NoError(t, err)
			assert.Equal(t, tt.content, body1)

			// Second read hits the cached bodyReadCloser path and also exercises
			// the Seek back to start. The underlying r.Body is now a *bodyReadCloser.
			body2, err := httputils.ReadBody(req)
			require.NoError(t, err)
			assert.Equal(t, tt.content, body2)

			// Close the body (calls bodyReadCloser.Close which returns nil)
			err = req.Body.Close()
			require.NoError(t, err)

			// After close we can still read via the interface because Close is a no-op
			_, _ = io.ReadAll(req.Body)
		})
	}
}

// TestExtractServiceFromRequest tests the ExtractServiceFromRequest function
// with various Authorization header configurations.
func TestExtractServiceFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authorization string
		wantService   string
	}{
		{
			name: "full_sigv4_credential",
			authorization: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1/s3/aws4_request, " +
				"SignedHeaders=host, Signature=abc",
			wantService: "s3",
		},
		{
			name: "kms_service",
			authorization: "AWS4-HMAC-SHA256 Credential=AKIAIOSFODNN7EXAMPLE/20240101/us-west-2/kms/aws4_request, " +
				"SignedHeaders=host;x-amz-date, Signature=def",
			wantService: "kms",
		},
		{
			name: "dynamodb_service",
			authorization: "AWS4-HMAC-SHA256 Credential=AKID/20240601/eu-west-1/dynamodb/aws4_request, " +
				"SignedHeaders=host, Signature=xyz",
			wantService: "dynamodb",
		},
		{
			name:          "no_authorization_header",
			authorization: "",
			wantService:   "",
		},
		{
			name:          "authorization_without_credential",
			authorization: "Bearer sometoken",
			wantService:   "",
		},
		{
			name:          "credential_too_short",
			authorization: "AWS4-HMAC-SHA256 Credential=AKID/20240101/us-east-1",
			wantService:   "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}

			service := httputils.ExtractServiceFromRequest(req)
			assert.Equal(t, tt.wantService, service)
		})
	}
}

// TestWriteJSON_WriteError exercises the write error path in WriteJSON by using
// a responseWriter that fails on Write.
func TestWriteJSON_WriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload any
		name    string
	}{
		{
			name:    "write_failure_does_not_panic",
			payload: map[string]string{"key": "value"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &failingWriter{ResponseRecorder: httptest.NewRecorder()}
			// Should not panic even when Write fails
			httputils.WriteJSON(t.Context(), w, http.StatusOK, tt.payload)
		})
	}
}

// TestWriteXML_WriteError exercises the write error path in WriteXML.
func TestWriteXML_WriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload any
		name    string
	}{
		{
			name: "write_failure_does_not_panic",
			payload: struct {
				Val string `xml:"val"`
			}{Val: "hello"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &failingWriter{ResponseRecorder: httptest.NewRecorder()}
			// Should not panic even when Write fails
			httputils.WriteXML(t.Context(), w, http.StatusOK, tt.payload)
		})
	}
}

// TestWriteDynamoDBResponse_WriteError exercises the write error path in WriteDynamoDBResponse.
func TestWriteDynamoDBResponse_WriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload any
		name    string
	}{
		{
			name:    "write_failure_does_not_panic",
			payload: map[string]string{"result": "ok"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := &failingWriter{ResponseRecorder: httptest.NewRecorder()}
			// Should not panic even when Write fails
			httputils.WriteDynamoDBResponse(t.Context(), w, http.StatusOK, tt.payload)
		})
	}
}

// failingWriter is a ResponseWriter whose Write always returns an error,
// used to exercise error-handling branches in the write functions.
type failingWriter struct {
	*httptest.ResponseRecorder
}

func (f *failingWriter) Write(_ []byte) (int, error) {
	return 0, io.ErrClosedPipe
}

// TestResponseWriter_WriteWithContentType exercises the Write path when
// Content-Type is already set (should not override).
func TestResponseWriter_WriteWithContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		wantCT      string
	}{
		{
			name:        "preserves_existing_content_type",
			contentType: "application/json",
			wantCT:      "application/json",
		},
		{
			name:        "sets_default_text_plain_when_empty",
			contentType: "",
			wantCT:      "text/plain; charset=utf-8",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inner := httptest.NewRecorder()
			w := httputils.NewResponseWriter(inner)

			if tt.contentType != "" {
				w.Header().Set("Content-Type", tt.contentType)
			}

			n, err := w.Write([]byte("body"))
			require.NoError(t, err)
			assert.Equal(t, 4, n)
			assert.Equal(t, tt.wantCT, w.Header().Get("Content-Type"))
		})
	}
}
