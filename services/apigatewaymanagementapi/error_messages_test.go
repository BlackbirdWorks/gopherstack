package apigatewaymanagementapi_test

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigatewaymanagementapi"
)

func TestModeledErrorBodies(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantType   string
		wantMsg    string
		wantStatus int
	}{
		{
			name: "delete_gone", method: http.MethodDelete, wantStatus: http.StatusGone,
			wantType: "GoneException", wantMsg: "The connection with the provided id no longer exists.",
		},
		{
			name: "get_gone", method: http.MethodGet, wantStatus: http.StatusGone,
			wantType: "GoneException", wantMsg: "The connection with the provided id no longer exists.",
		},
		{
			name: "post_gone", method: http.MethodPost, wantStatus: http.StatusGone,
			wantType: "GoneException", wantMsg: "The connection with the provided id no longer exists.",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := apigatewaymanagementapi.NewHandler(apigatewaymanagementapi.NewInMemoryBackend())
			e := echo.New()
			req := httptest.NewRequest(tt.method, "/@connections/missing=", strings.NewReader("x"))
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))

			assert.Equal(t, tt.wantStatus, rec.Code)
			assert.Equal(t, tt.wantType, rec.Header().Get("X-Amzn-Errortype"))

			var body map[string]string

			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &body))
			assert.Equal(t, tt.wantMsg, body["message"])
			assert.NotContains(t, body, "connectionId")
		})
	}
}
