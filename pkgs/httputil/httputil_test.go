package httputil_test

import (
	"bytes"
	"context"
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

	// Try to read again, it should be empty/closed
	body, _ := io.ReadAll(req.Body)
	assert.Empty(t, body)
}

func TestWriteJSON(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	payload := map[string]string{"foo": "bar"}

	httputil.WriteJSON(nil, w, http.StatusCreated, payload)

	assert.Equal(t, http.StatusCreated, w.Code)
	assert.Equal(t, "application/json", w.Header().Get("Content-Type"))
	assert.JSONEq(t, `{"foo":"bar"}`, w.Body.String())
}

func TestWriteXML(t *testing.T) {
	t.Parallel()
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
}

func TestWriteDynamoDBResponse(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	payload := map[string]string{"result": "ok"}

	httputil.WriteDynamoDBResponse(nil, w, http.StatusOK, payload)

	assert.Equal(t, http.StatusOK, w.Code)
	assert.Equal(t, "application/x-amz-json-1.0", w.Header().Get("Content-Type"))
	assert.NotEmpty(t, w.Header().Get("X-Amz-Crc32"))
	assert.JSONEq(t, `{"result":"ok"}`, w.Body.String())
}

var errSomethingWentWrong = errors.New("something went wrong")

func TestWriteError(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)

	httputil.WriteError(nil, w, req, errSomethingWentWrong, http.StatusBadRequest)

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
	ctx := context.Background()

	assert.Equal(t, "Unknown", httputil.GetOperation(ctx))
	assert.Empty(t, httputil.GetResource(ctx))

	ctx = httputil.SetOperation(ctx, "GetItem")
	assert.Equal(t, "GetItem", httputil.GetOperation(ctx))

	ctx = httputil.SetResource(ctx, "MyTable")
	assert.Equal(t, "GetItem", httputil.GetOperation(ctx))
	assert.Equal(t, "MyTable", httputil.GetResource(ctx))

	ctx = httputil.SetOperationAndResource(context.Background(), "PutItem", "AnotherTable")
	assert.Equal(t, "PutItem", httputil.GetOperation(ctx))
	assert.Equal(t, "AnotherTable", httputil.GetResource(ctx))
}

var errOops = errors.New("oops")

func TestEchoError(t *testing.T) {
	t.Parallel()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	res := httputil.EchoError(nil, c, http.StatusForbidden, "denied", errOops)

	require.NoError(t, res)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, "denied", rec.Body.String())
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
	// Should be a valid UUID (36 characters with hyphens).
	assert.Len(t, requestID, 36)
}

func TestExtractRegionFromRequest(t *testing.T) {
	t.Parallel()

	t.Run("extracts region from SigV4 Authorization header", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("Authorization",
			"AWS4-HMAC-SHA256 Credential=AKID/20240101/eu-west-1/kms/aws4_request, "+
				"SignedHeaders=host, Signature=abc")

		region := httputil.ExtractRegionFromRequest(req, "us-east-1")
		assert.Equal(t, "eu-west-1", region)
	})

	t.Run("falls back to X-Amz-Region header", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodPost, "/", nil)
		req.Header.Set("X-Amz-Region", "ap-southeast-1")

		region := httputil.ExtractRegionFromRequest(req, "us-east-1")
		assert.Equal(t, "ap-southeast-1", region)
	})

	t.Run("falls back to defaultRegion when no headers", func(t *testing.T) {
		t.Parallel()
		req := httptest.NewRequest(http.MethodPost, "/", nil)

		region := httputil.ExtractRegionFromRequest(req, "us-west-2")
		assert.Equal(t, "us-west-2", region)
	})
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
	httputil.WriteS3ErrorResponse(nil, w, req, s3Err{Code: "NoSuchKey", Message: "not found"}, http.StatusNotFound)

	assert.Equal(t, http.StatusNotFound, w.Code)
	assert.Equal(t, "application/xml", w.Header().Get("Content-Type"))
	assert.Contains(t, w.Body.String(), "NoSuchKey")
}

func TestWriteJSON_MarshalError(t *testing.T) {
	t.Parallel()
	// An un-marshallable value (channel) should cause a 500 response.
	w := httptest.NewRecorder()
	ch := make(chan int)
	httputil.WriteJSON(nil, w, http.StatusOK, ch)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestWriteXML_MarshalError(t *testing.T) {
	t.Parallel()
	// A function value cannot be marshalled to XML.
	w := httptest.NewRecorder()
	type bad struct {
		F func() `xml:"f"`
	}
	httputil.WriteXML(nil, w, http.StatusOK, bad{F: func() {}})
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestWriteDynamoDBResponse_MarshalError(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	ch := make(chan int)
	httputil.WriteDynamoDBResponse(nil, w, http.StatusOK, ch)
	assert.Equal(t, http.StatusInternalServerError, w.Code)
}

func TestEchoError_NilError(t *testing.T) {
	t.Parallel()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	res := httputil.EchoError(nil, c, http.StatusOK, "ok", nil)
	require.NoError(t, res)
	assert.Equal(t, http.StatusOK, rec.Code)
}
