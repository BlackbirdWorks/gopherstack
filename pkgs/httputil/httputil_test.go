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

func TestWriteError(t *testing.T) {
	t.Parallel()
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	err := errors.New("something went wrong")

	httputil.WriteError(nil, w, req, err, http.StatusBadRequest)

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

func TestEchoError(t *testing.T) {
	t.Parallel()
	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := errors.New("oops")
	res := httputil.EchoError(nil, c, http.StatusForbidden, "denied", err)

	require.NoError(t, res)
	assert.Equal(t, http.StatusForbidden, rec.Code)
	assert.Equal(t, "denied", rec.Body.String())
}
