package dashboard_test

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/services/azurequeue"
)

func TestAzureQueueProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/queue/devstoreaccount1"

	const msgBody = "<QueueMessage><MessageText>hi</MessageText></QueueMessage>"

	tests := []struct {
		name     string
		method   string
		path     string
		body     string
		wantBody string
		wantCode int
		seed     bool
	}{
		{name: "create queue", method: http.MethodPut, path: base + "/q1", wantCode: http.StatusCreated},
		{
			name: "list queues", method: http.MethodGet, path: base + "?comp=list",
			seed: true, wantCode: http.StatusOK, wantBody: "q1",
		},
		{
			name: "put message", method: http.MethodPost, path: base + "/q1/messages?messagettl=60",
			body: msgBody, seed: true, wantCode: http.StatusCreated, wantBody: "MessageId",
		},
		{
			name: "peek message", method: http.MethodGet, path: base + "/q1/messages?peekonly=true",
			seed: true, wantCode: http.StatusOK, wantBody: "hi",
		},
		{
			name: "clear messages", method: http.MethodDelete, path: base + "/q1/messages",
			seed: true, wantCode: http.StatusNoContent,
		},
		{name: "missing queue", method: http.MethodDelete, path: base + "/nope", wantCode: http.StatusNotFound},
		{name: "traversal blocked", method: http.MethodGet, path: base + "/q1/../x", wantCode: http.StatusBadRequest},
		{
			name:     "unknown subresource",
			method:   http.MethodGet,
			path:     base + "/q1/other",
			wantCode: http.StatusBadRequest,
		},
		{name: "too deep", method: http.MethodGet, path: base + "/q1/messages/a/b", wantCode: http.StatusBadRequest},
		{
			name:     "no account",
			method:   http.MethodGet,
			path:     "/dashboard/api/v1/azure/queue/",
			wantCode: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q := azurequeue.NewHandler(azurequeue.NewInMemoryBackend())
			h := dashboard.NewHandler(dashboard.Config{AzureQueueOps: q})

			if tt.seed {
				seed := httptest.NewRequest(http.MethodPut, base+"/q1", nil)
				require.NoError(t, h.Handler()(echo.NewContext(seed, httptest.NewRecorder())))

				put := httptest.NewRequest(http.MethodPost, base+"/q1/messages", strings.NewReader(msgBody))
				require.NoError(t, h.Handler()(echo.NewContext(put, httptest.NewRecorder())))
			}

			rec := httptest.NewRecorder()
			req := httptest.NewRequest(tt.method, tt.path, strings.NewReader(tt.body))
			require.NoError(t, h.Handler()(echo.NewContext(req, rec)))

			assert.Equal(t, tt.wantCode, rec.Code)
			assert.Contains(t, rec.Body.String(), tt.wantBody)
		})
	}
}

func TestAzureQueueProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/queue/a?comp=list", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
