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

func TestHTTPUtil(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		run  func(t *testing.T)
	}{
		{name: "ReadBody", run: func(t *testing.T) {
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
		}},
		{name: "DrainBody", run: func(t *testing.T) {
			content := []byte("some body")
			req := httptest.NewRequest(http.MethodPost, "/", bytes.NewReader(content))

			httputil.DrainBody(req)

			// Try to read again, it should be empty/closed
			body, _ := io.ReadAll(req.Body)
			assert.Empty(t, body)
		}},
		{name: "WriteJSON", run: func(t *testing.T) {
			w := httptest.NewRecorder()
			payload := map[string]string{"foo": "bar"}

			httputil.WriteJSON(nil, w, http.StatusCreated, payload)

			assert.Equal(t, http.StatusCreated, w.Code)
			assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
			assert.JSONEq(t, `{"foo":"bar"}`, w.Body.String())
		}},
		{name: "WriteXML", run: func(t *testing.T) {
			w := httptest.NewRecorder()
			type Payload struct {
				XMLName xml.Name `xml:"root"`
				Foo     string   `xml:"foo"`
			}
			payload := Payload{Foo: "bar"}

			httputil.WriteXML(nil, w, http.StatusOK, payload)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
			assert.Contains(t, w.Body.String(), xml.Header)
			assert.Contains(t, w.Body.String(), "<root><foo>bar</foo></root>")
		}},
		{name: "WriteDynamoDBResponse", run: func(t *testing.T) {
			w := httptest.NewRecorder()
			payload := map[string]string{"result": "ok"}

			httputil.WriteDynamoDBResponse(nil, w, http.StatusOK, payload)

			assert.Equal(t, http.StatusOK, w.Code)
			assert.Equal(t, "application/x-amz-json-1.0", w.Header().Get("Content-Type"))
			assert.NotEmpty(t, w.Header().Get("X-Amz-Crc32"))
			assert.JSONEq(t, `{"result":"ok"}`, w.Body.String())
		}},
		{name: "WriteError", run: func(t *testing.T) {
			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/", nil)

			httputil.WriteError(nil, w, req, errSomethingWentWrong, http.StatusBadRequest)

			assert.Equal(t, http.StatusBadRequest, w.Code)
			assert.Contains(t, w.Body.String(), "something went wrong")
		}},
		{name: "ResponseWriter", run: func(t *testing.T) {
			inner := httptest.NewRecorder()
			w := httputil.NewResponseWriter(inner)

			assert.Equal(t, http.StatusOK, w.StatusCode())

			w.WriteHeader(http.StatusAccepted)
			assert.Equal(t, http.StatusAccepted, w.StatusCode())

			_, _ = w.Write([]byte("foo"))
			assert.Equal(t, "foo", inner.Body.String())
		}},
		{name: "ContextOperations", run: func(t *testing.T) {
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
		}},
		{name: "EchoError", run: func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			res := httputil.EchoError(nil, c, http.StatusForbidden, "denied", errOops)

			require.NoError(t, res)
			assert.Equal(t, http.StatusForbidden, rec.Code)
			assert.Equal(t, "denied", rec.Body.String())
		}},
		{name: "RequestIDMiddleware", run: func(t *testing.T) {
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
			// Should be a valid UUID (36 characters with hyphens).
			assert.Len(t, requestID, 36)
		}},
		{name: "ExtractRegionFromRequest_SigV4", run: func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("Authorization",
				"AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/kms/aws4_request, "+
					"SignedHeaders=host, Signature=abc")

			region := httputil.ExtractRegionFromRequest(req, "us-east-1")
			assert.Equal(t, "eu-west-1", region)
		}},
		{name: "ExtractRegionFromRequest_XAmzRegionHeader", run: func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)
			req.Header.Set("X-Amz-Region", "ap-southeast-1")

			region := httputil.ExtractRegionFromRequest(req, "us-east-1")
			assert.Equal(t, "ap-southeast-1", region)
		}},
		{name: "ExtractRegionFromRequest_Default", run: func(t *testing.T) {
			req := httptest.NewRequest(http.MethodPost, "/", nil)

			region := httputil.ExtractRegionFromRequest(req, "us-west-2")
			assert.Equal(t, "us-west-2", region)
		}},
		{name: "WriteS3ErrorResponse", run: func(t *testing.T) {
			type s3Err struct {
				XMLName xml.Name `xml:"Error"`
				Code    string   `xml:"Code"`
				Message string   `xml:"Message"`
			}

			w := httptest.NewRecorder()
			req := httptest.NewRequest(http.MethodGet, "/bucket/key", nil)
			httputil.WriteS3ErrorResponse(nil, w, req, s3Err{Code: "NoSuchKey", Message: "not found"}, http.StatusNotFound)

			assert.Equal(t, http.StatusNotFound, w.Code)
			assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
			assert.Contains(t, w.Body.String(), "NoSuchKey")
		}},
		{name: "WriteJSON_MarshalError", run: func(t *testing.T) {
			// An un-marshallable value (channel) should cause a 500 response.
			w := httptest.NewRecorder()
			ch := make(chan int)
			httputil.WriteJSON(nil, w, http.StatusOK, ch)
			assert.Equal(t, http.StatusInternalServerError, w.Code)
		}},
		{name: "WriteXML_MarshalError", run: func(t *testing.T) {
			// A function value cannot be marshalled to XML.
			w := httptest.NewRecorder()
			type bad struct {
				F func() `xml:"f"`
			}
			httputil.WriteXML(nil, w, http.StatusOK, bad{F: func() {}})
			assert.Equal(t, http.StatusInternalServerError, w.Code)
		}},
		{name: "WriteDynamoDBResponse_MarshalError", run: func(t *testing.T) {
			w := httptest.NewRecorder()
			ch := make(chan int)
			httputil.WriteDynamoDBResponse(nil, w, http.StatusOK, ch)
			assert.Equal(t, http.StatusInternalServerError, w.Code)
		}},
		{name: "EchoError_NilError", run: func(t *testing.T) {
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			res := httputil.EchoError(nil, c, http.StatusOK, "ok", nil)
			require.NoError(t, res)
			assert.Equal(t, http.StatusOK, rec.Code)
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			tt.run(t)
		})
	}
}
