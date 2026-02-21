package handler_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/handler"
)

// --- OperationContext ---

func TestNewOperationContext_GetOperation_GetResource(t *testing.T) {
	t.Parallel()

	ctx := handler.NewOperationContext(context.Background(), "PutItem", "my-table")

	assert.Equal(t, "PutItem", handler.GetOperation(ctx))
	assert.Equal(t, "my-table", handler.GetResource(ctx))
}

func TestGetOperation_DefaultsToUnknown(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "Unknown", handler.GetOperation(context.Background()))
}

func TestGetResource_DefaultsToEmpty(t *testing.T) {
	t.Parallel()

	assert.Equal(t, "", handler.GetResource(context.Background()))
}

func TestSetOperation_UpdatesOperation(t *testing.T) {
	t.Parallel()

	ctx := handler.NewOperationContext(context.Background(), "GetItem", "tbl")
	ctx2 := handler.SetOperation(ctx, "DeleteItem")

	assert.Equal(t, "DeleteItem", handler.GetOperation(ctx2))
	// resource is preserved
	assert.Equal(t, "tbl", handler.GetResource(ctx2))
}

func TestSetOperation_NoopOnEmptyContext(t *testing.T) {
	t.Parallel()

	// SetOperation on a context with no operation data returns the context unchanged.
	ctx := handler.SetOperation(context.Background(), "ShouldNotPanic")

	assert.Equal(t, "Unknown", handler.GetOperation(ctx))
}

// --- ResponseWriter ---

func TestResponseWriter_DefaultStatusOK(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	rw := handler.NewResponseWriter(rec)

	assert.Equal(t, http.StatusOK, rw.StatusCode())
}

func TestResponseWriter_WriteHeaderCapturesCode(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	rw := handler.NewResponseWriter(rec)
	rw.WriteHeader(http.StatusCreated)

	assert.Equal(t, http.StatusCreated, rw.StatusCode())
	assert.Equal(t, http.StatusCreated, rec.Code)
}

func TestResponseWriter_WriteSetsOKIfNotSet(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	rw := handler.NewResponseWriter(rec)
	// Override the default 200 by zeroing it out through the embedded interface:
	// We can't zero out statusCode directly (unexported), so instead just exercise Write.
	n, err := rw.Write([]byte("hello"))

	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, http.StatusOK, rw.StatusCode())
}

// --- WriteJSON ---

func TestWriteJSON_SetsHeadersAndBody(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	payload := map[string]string{"key": "value"}

	err := handler.WriteJSON(rec, http.StatusAccepted, payload)

	require.NoError(t, err)
	assert.Equal(t, http.StatusAccepted, rec.Code)
	assert.Equal(t, "application/json", rec.Header().Get("Content-Type"))

	var got map[string]string
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &got))
	assert.Equal(t, "value", got["key"])
}

// --- WriteJSONWithChecksum ---

func TestWriteJSONWithChecksum_SetsChecksumHeader(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()

	err := handler.WriteJSONWithChecksum(rec, http.StatusOK, map[string]int{"n": 42})

	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "application/x-amz-json-1.0", rec.Header().Get("Content-Type"))
	assert.NotEmpty(t, rec.Header().Get("X-Amz-Crc32"))
}

// --- WriteError ---

func TestWriteError_WritesPlainText(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	log := slog.Default()

	handler.WriteError(log, rec, http.StatusBadRequest, "bad input", nil)

	assert.Equal(t, http.StatusBadRequest, rec.Code)
	assert.Contains(t, rec.Body.String(), "bad input")
}

func TestWriteError_WithErr_LogsAndWrites(t *testing.T) {
	t.Parallel()

	rec := httptest.NewRecorder()
	log := slog.Default()

	handler.WriteError(log, rec, http.StatusInternalServerError, "server error", assert.AnError)

	assert.Equal(t, http.StatusInternalServerError, rec.Code)
}

// --- EchoError ---

func TestEchoError_ReturnsEchoStringResponse(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	log := slog.Default()

	err := handler.EchoError(log, c, http.StatusNotFound, "not found", nil)

	require.NoError(t, err)
	assert.Equal(t, http.StatusNotFound, rec.Code)
	assert.Contains(t, rec.Body.String(), "not found")
}

func TestEchoError_WithErr_LogsAndResponds(t *testing.T) {
	t.Parallel()

	e := echo.New()
	req := httptest.NewRequest(http.MethodGet, "/", nil)
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)
	log := slog.Default()

	err := handler.EchoError(log, c, http.StatusBadGateway, "upstream error", assert.AnError)

	require.NoError(t, err)
	assert.Equal(t, http.StatusBadGateway, rec.Code)
}
