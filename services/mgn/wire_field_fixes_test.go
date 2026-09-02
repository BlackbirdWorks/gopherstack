package mgn_test

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/mgn"
)

// newRawServer stands up the same router newRoundTripClient uses but returns
// its URL directly, for tests that need to send a body shape the real SDK
// client cannot express (a member the real Input does not have).
func newRawServer(t *testing.T, h *mgn.Handler) string {
	t.Helper()

	e := echo.New()
	registry := service.NewRegistry()
	require.NoError(t, registry.Register(h))
	e.Use(service.NewServiceRouter(registry).RouteHandler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)

	return srv.URL
}

func rawPost(t *testing.T, url string, body map[string]any) map[string]any {
	t.Helper()

	raw, err := json.Marshal(body)
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, url, bytes.NewReader(raw))
	require.NoError(t, err)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)

	t.Cleanup(func() { _ = resp.Body.Close() })

	var out map[string]any
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&out))
	require.Equal(t, http.StatusOK, resp.StatusCode, "response: %v", out)

	return out
}

// TestCreateLaunchConfigurationTemplate_Ec2LaunchTemplateIDNotAccepted proves
// gopherstack-101r's fix for wire.go's createLaunchConfigurationTemplateRequest:
// ec2LaunchTemplateID is Output-only on the real
// CreateLaunchConfigurationTemplateInput/UpdateLaunchConfigurationTemplateInput
// (aws-sdk-go-v2/service/mgn@v1.48.4 api_op_CreateLaunchConfigurationTemplate.go:104,
// api_op_UpdateLaunchConfigurationTemplate.go:106) -- no real client can ever send
// it, so a raw body that does is the only way to exercise this. Before the fix,
// this value flowed straight through to the response; after, it never does.
func TestCreateLaunchConfigurationTemplate_Ec2LaunchTemplateIDNotAccepted(t *testing.T) {
	t.Parallel()

	backend := mgn.NewInMemoryBackend(t.Context(), rtTestAccountID, rtTestRegion)
	t.Cleanup(backend.Close)
	backend.InitializeService()

	h := mgn.NewHandler(backend)
	url := newRawServer(t, h)

	created := rawPost(t, url+"/CreateLaunchConfigurationTemplate", map[string]any{
		"ec2LaunchTemplateID": "lt-attacker-supplied",
	})
	require.Empty(t, created["ec2LaunchTemplateID"], "Create must not accept ec2LaunchTemplateID from the request")

	id, ok := created["launchConfigurationTemplateID"].(string)
	require.True(t, ok)
	require.NotEmpty(t, id)

	updated := rawPost(t, url+"/UpdateLaunchConfigurationTemplate", map[string]any{
		"launchConfigurationTemplateID": id,
		"ec2LaunchTemplateID":           "lt-attacker-supplied-2",
	})
	require.Empty(t, updated["ec2LaunchTemplateID"], "Update must not accept ec2LaunchTemplateID from the request")
}
