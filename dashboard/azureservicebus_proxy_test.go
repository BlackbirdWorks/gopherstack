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
	"github.com/blackbirdworks/gopherstack/services/azureservicebus"
)

func TestAzureServiceBusProxy(t *testing.T) {
	t.Parallel()

	const base = "/dashboard/api/v1/azure/servicebus"

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
			name: "list queues", method: http.MethodGet, path: base + "/$Resources/Queues",
			seed: true, wantCode: http.StatusOK, wantBody: "q1",
		},
		{
			name: "get queue", method: http.MethodGet, path: base + "/q1",
			seed: true, wantCode: http.StatusOK, wantBody: "MaxDeliveryCount",
		},
		{
			name: "send message", method: http.MethodPost, path: base + "/q1/messages",
			body: "hello", seed: true, wantCode: http.StatusCreated,
		},
		{
			name: "peek lock", method: http.MethodPost, path: base + "/q1/messages/head",
			seed: true, wantCode: http.StatusCreated, wantBody: "hi",
		},
		{
			name: "dead letter empty", method: http.MethodPost, path: base + "/q1/$DeadLetterQueue/messages/head",
			seed: true, wantCode: http.StatusNoContent,
		},
		{name: "create topic", method: http.MethodPut, path: base + "/t1?type=topic", wantCode: http.StatusCreated},
		{name: "missing queue", method: http.MethodDelete, path: base + "/nope", wantCode: http.StatusNotFound},
		{name: "traversal blocked", method: http.MethodGet, path: base + "/q1/../x", wantCode: http.StatusBadRequest},
		{
			name: "unknown subresource", method: http.MethodGet, path: base + "/q1/other",
			wantCode: http.StatusBadRequest,
		},
		{
			name: "bad resources", method: http.MethodGet, path: base + "/$Resources/Other",
			wantCode: http.StatusBadRequest,
		},
		{name: "too deep", method: http.MethodGet, path: base + "/a/b/c/d/e/f/g", wantCode: http.StatusBadRequest},
		{name: "no entity", method: http.MethodGet, path: base + "/", wantCode: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sb := azureservicebus.NewHandler(azureservicebus.NewInMemoryBackend())
			h := dashboard.NewHandler(dashboard.Config{AzureServiceBusOps: sb})

			if tt.seed {
				seed := httptest.NewRequest(http.MethodPut, base+"/q1", nil)
				require.NoError(t, h.Handler()(echo.NewContext(seed, httptest.NewRecorder())))

				put := httptest.NewRequest(http.MethodPost, base+"/q1/messages", strings.NewReader("hi"))
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

func TestAzureServiceBusProxyDisabled(t *testing.T) {
	t.Parallel()

	h := dashboard.NewHandler(dashboard.Config{})
	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/dashboard/api/v1/azure/servicebus/$Resources/Queues", nil)
	require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
	assert.Equal(t, http.StatusServiceUnavailable, rec.Code)
}
