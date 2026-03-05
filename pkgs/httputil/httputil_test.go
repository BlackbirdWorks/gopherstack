package httputil_test

import (
	"bytes"
	"encoding/xml"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/httputil"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errSomethingWentWrong = errors.New("something went wrong")

var errOops = errors.New("oops")

func TestReadBody(t *testing.T) {
	t.Parallel()

	content := []byte("hello world")
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(content))

	// First read
	body, err := httputil.ReadBody(req)
	require.NoError(t, err)
	assert.Equal(t, content, body)

	// Second read (verify re-seeding)
	body2, err := httputil.ReadBody(req)
	require.NoError(t, err)
	assert.Equal(t, content, body2)

	// Nil body case
	reqNil := httptest.NewRequest(http.MethodGet, "/", nil)
	reqNil.Body = nil
	bodyNil, err := httputil.ReadBody(reqNil)
	require.NoError(t, err)
	assert.Nil(t, bodyNil)
}

func TestDrainBody(t *testing.T) {
	t.Parallel()

	content := []byte("some body")
	req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(content))

	httputil.DrainBody(req)

	body, _ := io.ReadAll(req.Body)
	assert.Empty(t, body)
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  any
		name     string
		wantCT   string
		wantBody string
		status   int
		wantCode int
	}{
		{
			name:     "success",
			status:   http.StatusCreated,
			payload:  map[string]string{"foo": "bar"},
			wantCode: http.StatusCreated,
			wantCT:   "application/json",
			wantBody: `{"foo":"bar"}`,
		},
		{
			name:     "marshal_error",
			status:   http.StatusOK,
			payload:  make(chan int),
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			httputil.WriteJSON(t.Context(), w, tt.status, tt.payload)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantCT != "" {
				assert.Equal(t, tt.wantCT, w.Header().Get("Content-Type"))
			}
			if tt.wantBody != "" {
				assert.JSONEq(t, tt.wantBody, w.Body.String())
			}
		})
	}
}

func TestWriteXML(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  any
		name     string
		wantCT   string
		wantBody string
		status   int
		wantCode int
	}{
		{
			name:   "success",
			status: http.StatusOK,
			payload: struct {
				XMLName xml.Name `xml:"root"`
				Foo     string   `xml:"foo"`
			}{Foo: "bar"},
			wantCode: http.StatusOK,
			wantCT:   "application/xml",
			wantBody: "<root><foo>bar</foo></root>",
		},
		{
			name:   "marshal_error",
			status: http.StatusOK,
			payload: struct {
				F func() `xml:"f"`
			}{F: func() {}},
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			httputil.WriteXML(t.Context(), w, tt.status, tt.payload)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantCT != "" {
				assert.Equal(t, tt.wantCT, w.Header().Get("Content-Type"))
				assert.Contains(t, w.Body.String(), xml.Header)
			}
			if tt.wantBody != "" {
				assert.Contains(t, w.Body.String(), tt.wantBody)
			}
		})
	}
}

func TestWriteDynamoDBResponse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		payload  any
		name     string
		wantCT   string
		wantBody string
		status   int
		wantCode int
		wantCRC  bool
	}{
		{
			name:     "success",
			status:   http.StatusOK,
			payload:  map[string]string{"result": "ok"},
			wantCode: http.StatusOK,
			wantCT:   "application/x-amz-json-1.0",
			wantBody: `{"result":"ok"}`,
			wantCRC:  true,
		},
		{
			name:     "marshal_error",
			status:   http.StatusOK,
			payload:  make(chan int),
			wantCode: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			w := httptest.NewRecorder()
			httputil.WriteDynamoDBResponse(t.Context(), w, tt.status, tt.payload)

			assert.Equal(t, tt.wantCode, w.Code)
			if tt.wantCT != "" {
				assert.Equal(t, tt.wantCT, w.Header().Get("Content-Type"))
			}
			if tt.wantCRC {
				assert.NotEmpty(t, w.Header().Get("X-Amz-Crc32"))
			}
			if tt.wantBody != "" {
				assert.JSONEq(t, tt.wantBody, w.Body.String())
			}
		})
	}
}

func TestWriteError(t *testing.T) {
	t.Parallel()

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	httputil.WriteError(t.Context(), w, req, errSomethingWentWrong, http.StatusBadRequest)

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "something went wrong")
}

func TestResponseWriter(t *testing.T) {
	t.Parallel()

	inner := httptest.NewRecorder()
	w := httputil.NewResponseWriter(inner)

	assert.Equal(t, http.StatusOK, w.StatusCode())

	w.WriteHeader(http.StatusAccepted)
	assert.Equal(t, http.StatusAccepted, w.StatusCode())

	_, _ = w.Write([]byte("foo"))
	assert.Equal(t, "foo", inner.Body.String())
}

func TestContextOperations(t *testing.T) {
	t.Parallel()

	ctx := t.Context()

	assert.Equal(t, "Unknown", httputil.GetOperation(ctx))
	assert.Empty(t, httputil.GetResource(ctx))

	ctx = httputil.SetOperation(ctx, "GetItem")
	assert.Equal(t, "GetItem", httputil.GetOperation(ctx))

	ctx = httputil.SetResource(ctx, "MyTable")
	assert.Equal(t, "GetItem", httputil.GetOperation(ctx))
	assert.Equal(t, "MyTable", httputil.GetResource(ctx))

	ctx = httputil.SetOperationAndResource(t.Context(), "PutItem", "AnotherTable")
	assert.Equal(t, "PutItem", httputil.GetOperation(ctx))
	assert.Equal(t, "AnotherTable", httputil.GetResource(ctx))
}

func TestEchoError(t *testing.T) {
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
			name:     "with_error",
			code:     http.StatusForbidden,
			message:  "denied",
			err:      errOops,
			wantCode: http.StatusForbidden,
			wantBody: "denied",
		},
		{
			name:     "nil_error",
			code:     http.StatusOK,
			message:  "ok",
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

			res := httputil.EchoError(t.Context(), c, tt.code, tt.message, tt.err)

			require.NoError(t, res)
			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Equal(t, tt.wantBody, rec.Body.String())
		})
	}
}

func TestRequestIDMiddleware(t *testing.T) {
	t.Parallel()

	e := echo.New()
	e.Use(httputil.RequestIDMiddleware())
	e.GET("/", func(c *echo.Context) error {
		return c.String(http.StatusOK, "ok")
	})

	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	e.ServeHTTP(rec, req)

	requestID := rec.Header().Get("X-Amz-Request-Id")
	assert.NotEmpty(t, requestID)
	assert.Len(t, requestID, 36)
}

func TestExtractRegionFromRequest(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		authorization string
		xAmzRegion    string
		defaultRegion string
		wantRegion    string
	}{
		{
			name: "sigv4",
			authorization: "AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/kms/aws4_request, " +
				"SignedHeaders=host, Signature=abc",
			defaultRegion: "us-east-1",
			wantRegion:    "eu-west-1",
		},
		{
			name:          "x_amz_region_header",
			xAmzRegion:    "ap-southeast-1",
			defaultRegion: "us-east-1",
			wantRegion:    "ap-southeast-1",
		},
		{
			name:          "default",
			defaultRegion: "us-west-2",
			wantRegion:    "us-west-2",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			req := httptest.NewRequest(http.MethodPost, "/", nil)
			if tt.authorization != "" {
				req.Header.Set("Authorization", tt.authorization)
			}
			if tt.xAmzRegion != "" {
				req.Header.Set("X-Amz-Region", tt.xAmzRegion)
			}

			region := httputil.ExtractRegionFromRequest(req, tt.defaultRegion)
			assert.Equal(t, tt.wantRegion, region)
		})
	}
}

func TestWriteS3ErrorResponse(t *testing.T) {
	t.Parallel()

	type s3Err struct {
		XMLName xml.Name `xml:"Error"`
		Code    string   `xml:"Code"`
		Message string   `xml:"Message"`
	}

	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
	httputil.WriteS3ErrorResponse(
		t.Context(),
		w,
		req,
		s3Err{Code: "NoSuchKey", Message: "not found"},
		http.StatusNotFound,
	)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
	assert.Contains(t, w.Body.String(), "NoSuchKey")
}
