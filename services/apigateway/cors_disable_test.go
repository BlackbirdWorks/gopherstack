package apigateway_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/apigateway"
)

func TestProxyCustomCORSToggle(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		method     string
		wantOrigin string
		disable    bool
	}{
		{name: "preflight enabled", method: http.MethodOptions, wantOrigin: "https://example.com"},
		{name: "preflight disabled", method: http.MethodOptions, disable: true},
		{name: "actual enabled", method: http.MethodGet, wantOrigin: "https://example.com"},
		{name: "actual disabled", method: http.MethodGet, disable: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			backend := apigateway.NewInMemoryBackend()
			h := apigateway.NewHandler(backend)
			h.DisableCustomCORS = tt.disable
			e := echo.New()

			api, _ := backend.CreateRestAPI(apigateway.CreateRestAPIInput{Name: "cors-toggle"})
			resources, _, _ := backend.GetResources(api.ID, "", 0)
			rootID := resources[0].ID

			_, err := backend.UpdateResource(api.ID, rootID, apigateway.UpdateResourceInput{
				CorsConfiguration: &apigateway.CorsConfiguration{
					AllowOrigins: []string{"https://example.com"},
					AllowMethods: []string{"GET", "OPTIONS"},
				},
			})
			require.NoError(t, err)

			_, _ = backend.PutMethod(apigateway.PutMethodInput{
				RestAPIID: api.ID, ResourceID: rootID, HTTPMethod: "GET", AuthorizationType: "NONE",
			})
			_, _ = backend.PutIntegration(api.ID, rootID, "GET", apigateway.PutIntegrationInput{Type: "MOCK"})
			_, _ = backend.PutIntegrationResponse(
				api.ID,
				rootID,
				"GET",
				"200",
				apigateway.PutIntegrationResponseInput{},
			)
			depl, _ := backend.CreateDeployment(api.ID, "prod", "v1")
			_, _ = backend.CreateStage(apigateway.CreateStageInput{
				RestAPIID: api.ID, StageName: "prod", DeploymentID: depl.ID,
			})

			req := httptest.NewRequest(tt.method, "/restapis/"+api.ID+"/prod/_user_request_/", nil)
			req.Header.Set("Origin", "https://example.com")
			req.Header.Set("Access-Control-Request-Method", "GET")
			rec := httptest.NewRecorder()
			require.NoError(t, h.Handler()(e.NewContext(req, rec)))

			assert.Equal(t, tt.wantOrigin, rec.Header().Get("Access-Control-Allow-Origin"))
		})
	}
}
