package httputil_test

import (
	"encoding/xml"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
)

var errCovTest = errors.New("coverage test error")

func TestWriteJSON_WithLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  any
		name     string
		wantCode int
	}{
		{
			name:     "marshal_error_with_logger",
			payload:  make(chan int),
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "success_with_logger",
			payload:  map[string]string{"ok": "yes"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			httputil.WriteJSON(slog.Default(), w, http.StatusOK, tt.payload)
			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

func TestWriteXML_WithLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  any
		name     string
		wantCode int
	}{
		{
			name: "marshal_error_with_logger",
			payload: struct {
				F func() `xml:"f"`
			}{F: func() {}},
			wantCode: http.StatusInternalServerError,
		},
		{
			name: "success_with_logger",
			payload: struct {
				XMLName xml.Name `xml:"root"`
				Val     string   `xml:"val"`
			}{Val: "hello"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			httputil.WriteXML(slog.Default(), w, http.StatusOK, tt.payload)
			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

func TestWriteDynamoDBResponse_WithLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  any
		name     string
		wantCode int
	}{
		{
			name:     "marshal_error_with_logger",
			payload:  make(chan int),
			wantCode: http.StatusInternalServerError,
		},
		{
			name:     "success_with_logger",
			payload:  map[string]string{"result": "ok"},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			httputil.WriteDynamoDBResponse(slog.Default(), w, http.StatusOK, tt.payload)
			assert.Equal(t, tt.wantCode, w.Code)
		})
	}
}

func TestWriteError_WithLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "with_logger_and_error",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/test", nil)
			httputil.WriteError(slog.Default(), w, req, errCovTest, tt.wantCode)
			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), "coverage test error")
		})
	}
}

func TestEchoError_WithLogger(t *testing.T) {
	t.Parallel()

	tests := []struct {
		err      error
		name     string
		message  string
		wantBody string
		code     int
		wantCode int
	}{
		{
			name:     "with_logger_and_error",
			code:     http.StatusForbidden,
			message:  "forbidden",
			err:      errCovTest,
			wantCode: http.StatusForbidden,
			wantBody: "forbidden",
		},
		{
			name:     "with_logger_nil_error",
			code:     http.StatusOK,
			message:  "ok",
			err:      nil,
			wantCode: http.StatusOK,
			wantBody: "ok",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			res := httputil.EchoError(slog.Default(), c, tt.code, tt.message, tt.err)
			require.NoError(t, res)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, tt.wantBody, rec.Body.String())
		})
	}
}

func TestResponseWriter_WriteWithZeroStatus(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantStatusCode int
	}{
		{
			name:           "write_sets_200_when_status_not_set",
			wantStatusCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			inner := httptest.NewRecorder()
			// Create ResponseWriter without NewResponseWriter so statusCode is 0.
			w := &httputil.ResponseWriter{ResponseWriter: inner}

			n, err := w.Write([]byte("hello"))
			require.NoError(t, err)
			assert.Equal(t, 5, n)
			assert.Equal(t, tt.wantStatusCode, w.StatusCode())
		})
	}
}

func TestWriteJSON_PreservesContentType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		contentType string
		wantCT      string
	}{
		{
			name:        "keeps_existing_content_type",
			contentType: "application/x-amz-json-1.1",
			wantCT:      "application/x-amz-json-1.1",
		},
		{
			name:   "sets_application_json_when_empty",
			wantCT: "application/json",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			if tt.contentType != "" {
				w.Header().Set("Content-Type", tt.contentType)
			}

			httputil.WriteJSON(nil, w, http.StatusOK, map[string]string{"k": "v"})
			assert.Equal(t, tt.wantCT, w.Header().Get("Content-Type"))
		})
	}
}

func TestWriteS3ErrorResponse_WithLogger(t *testing.T) {
	t.Parallel()

	type s3Err struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}

	tests := []struct {
		name     string
		wantCode int
	}{
		{
			name:     "writes_xml_error_with_logger",
			wantCode: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
			httputil.WriteS3ErrorResponse(
				slog.Default(), w, req,
				s3Err{Code: "NoSuchKey", Message: "not found"},
				tt.wantCode,
			)
			assert.Equal(t, tt.wantCode, w.Code)
			assert.Contains(t, w.Body.String(), "NoSuchKey")
		})
	}
}
