package appconfig_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/appconfig"
)

// TestHandler_UnmatchedRouteCarriesTypedErrorCode drives a raw request whose
// path is claimed by RouteMatcher but whose method parseAppConfigPath does
// not map to any operation (DELETE /settings; real GetAccountSettings/
// UpdateAccountSettings only bind GET/PATCH). Before the fix, Handler()'s
// dispatch-miss fallback wrote {"message": "not found"} with neither the
// X-Amzn-Errortype header nor a __type body field, so restjson.GetErrorInfo
// (aws-sdk-go-v2 aws/protocol/restjson/decoder_util.go:15) found no code
// anywhere and every unmatched route decoded client-side as
// smithy.GenericAPIError{Code:"UnknownError"}.
func TestHandler_UnmatchedRouteCarriesTypedErrorCode(t *testing.T) {
	t.Parallel()

	backend := appconfig.NewInMemoryBackend("000000000000", "us-east-1")
	h := appconfig.NewHandler(backend)

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodDelete, srv.URL+"/settings", nil)
	require.NoError(t, err)
	req.Header.Set("Authorization",
		"AWS4-HMAC-SHA256 Credential=test/20260101/us-east-1/appconfig/aws4_request, "+
			"SignedHeaders=host, Signature=deadbeef")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	require.Equal(t, http.StatusNotFound, resp.StatusCode)
	require.Equal(t, "ResourceNotFoundException", resp.Header.Get("X-Amzn-Errortype"))

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(raw, &decoded))
	require.NotEmpty(t, decoded["message"])
}
