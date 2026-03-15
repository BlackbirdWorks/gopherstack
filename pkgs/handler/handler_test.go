package handler_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/handler"
)

// --- OperationContext ---

func TestOperationContext(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup       func(context.Context) context.Context
		name        string
		expectedOp  string
		expectedRes string
	}{
		{
			name: "NewOperationContext sets operation and resource",
			setup: func(ctx context.Context) context.Context {
				return handler.NewOperationContext(ctx, "PutItem", "my-table")
			},
			expectedOp:  "PutItem",
			expectedRes: "my-table",
		},
		{
			name: "GetOperation defaults to Unknown",
			setup: func(ctx context.Context) context.Context {
				return ctx
			},
			expectedOp:  "Unknown",
			expectedRes: "",
		},
		{
			name: "GetResource defaults to empty",
			setup: func(ctx context.Context) context.Context {
				return ctx
			},
			expectedOp:  "Unknown",
			expectedRes: "",
		},
		{
			name: "SetOperation updates operation and preserves resource",
			setup: func(ctx context.Context) context.Context {
				ctx = handler.NewOperationContext(ctx, "GetItem", "tbl")

				return handler.SetOperation(ctx, "DeleteItem")
			},
			expectedOp:  "DeleteItem",
			expectedRes: "tbl",
		},
		{
			name: "SetOperation noop on empty context",
			setup: func(ctx context.Context) context.Context {
				return handler.SetOperation(ctx, "ShouldNotPanic")
			},
			expectedOp:  "Unknown",
			expectedRes: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			ctx := tt.setup(t.Context())
			assert.Equal(t, tt.expectedOp, handler.GetOperation(ctx))
			assert.Equal(t, tt.expectedRes, handler.GetResource(ctx))
		})
	}
}

// --- ResponseWriter ---

func TestResponseWriter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		action         func(*handler.ResponseWriter)
		name           string
		expectedBody   string
		expectedStatus int
	}{
		{
			name:           "No action yields zero status",
			action:         func(_ *handler.ResponseWriter) {},
			expectedStatus: 0,
		},
		{
			name: "WriteHeader captures code",
			action: func(rw *handler.ResponseWriter) {
				rw.WriteHeader(http.StatusCreated)
			},
			expectedStatus: http.StatusCreated,
		},
		{
			name: "Write sets OK if not set",
			action: func(rw *handler.ResponseWriter) {
				_, _ = rw.Write([]byte("hello"))
			},
			expectedStatus: http.StatusOK,
			expectedBody:   "hello",
		},
		{
			name: "WriteHeader before Write preserves status",
			action: func(rw *handler.ResponseWriter) {
				rw.WriteHeader(http.StatusCreated)
				_, _ = rw.Write([]byte("created"))
			},
			expectedStatus: http.StatusCreated,
			expectedBody:   "created",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()
			rw := handler.NewResponseWriter(rec)
			tt.action(rw)
			assert.Equal(t, tt.expectedStatus, rw.StatusCode())
			if tt.expectedBody != "" {
				assert.Equal(t, tt.expectedBody, rec.Body.String())
			}
		})
	}
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

func TestWriteError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		message        string
		err            error
		expectedBody   string
		status         int
		expectedStatus int
	}{
		{
			name:           "Writes plain text without error",
			status:         http.StatusBadRequest,
			message:        "bad input",
			err:            nil,
			expectedStatus: http.StatusBadRequest,
			expectedBody:   "bad input",
		},
		{
			name:           "With error logs and writes",
			status:         http.StatusInternalServerError,
			message:        "server error",
			err:            assert.AnError,
			expectedStatus: http.StatusInternalServerError,
			expectedBody:   "server error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			rec := httptest.NewRecorder()

			handler.WriteError(t.Context(), rec, tt.status, tt.message, tt.err)

			assert.Equal(t, tt.expectedStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.expectedBody)
		})
	}
}

// --- EchoError ---

func TestEchoError(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		message        string
		err            error
		expectedBody   string
		status         int
		expectedStatus int
	}{
		{
			name:           "Returns echo string response",
			status:         http.StatusNotFound,
			message:        "not found",
			err:            nil,
			expectedStatus: http.StatusNotFound,
			expectedBody:   "not found",
		},
		{
			name:           "With error logs and responds",
			status:         http.StatusBadGateway,
			message:        "upstream error",
			err:            assert.AnError,
			expectedStatus: http.StatusBadGateway,
			expectedBody:   "upstream error",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			e := echo.New()
			req := httptest.NewRequest(http.MethodGet, "/", nil)
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)

			err := handler.EchoError(t.Context(), c, tt.status, tt.message, tt.err)

			require.NoError(t, err)
			assert.Equal(t, tt.expectedStatus, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.expectedBody)
		})
	}
}
