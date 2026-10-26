package azurefunctions_test

import (
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

const webDockerOptIn = "GOPHERSTACK_AZAPP_DOCKER_TEST"

// TestDocker_WebAppContainer runs a real custom-container web app (busybox httpd) behind <app>.<domain> Host
// routing. Opt-in via GOPHERSTACK_AZAPP_DOCKER_TEST=1; it needs a container runtime and pulls busybox.
func TestDocker_WebAppContainer(t *testing.T) {
	t.Parallel()

	if os.Getenv(webDockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", webDockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "azurefunctions.test.webruntime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	e := newEnv(t, false)
	e.backend.SetRuntime(func() (azurefunctions.Runner, error) { return lr.Get() })
	e.backend.Configure(azurefunctions.Settings{StartupTimeout: 5 * time.Minute})

	e.backend.PutApp("shop", azurefunctions.AppConfig{
		Kind:            webKind,
		LinuxFx:         "DOCKER|busybox:1.36",
		HealthCheckPath: "/healthz.txt",
		AppCommandLine: `sh -c "mkdir -p /www && echo ok > /www/healthz.txt && ` +
			`echo hello-from-$GREETING > /www/index.html && exec httpd -f -p 8081 -h /www"`,
	})
	require.NoError(
		t,
		e.backend.SetSettings("shop", map[string]string{"WEBSITES_PORT": "8081", "GREETING": "appservice"}),
	)

	host := "shop." + testDomain

	resp, body := e.do(t, http.MethodGet, host, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "hello-from-appservice")

	info, ok := e.backend.App("shop")
	require.True(t, ok)
	assert.Empty(t, info.Failure)

	require.NoError(t, e.backend.Restart("shop"))

	resp, body = e.do(t, http.MethodGet, host, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "hello-from-appservice")

	e.backend.DeleteApp("shop")
}

// TestDocker_SlotSwap runs two real busybox slots, swaps their content and checks that each hostname now serves
// the other's page. Opt-in like TestDocker_WebAppContainer.
func TestDocker_SlotSwap(t *testing.T) {
	t.Parallel()

	if os.Getenv(webDockerOptIn) == "" {
		t.Skipf("set %s=1 to run against a real container runtime", webDockerOptIn)
	}

	lr := gcpserve.NewLazyRuntime(nil, "azurefunctions.test.slotruntime")
	t.Cleanup(func() { _ = lr.Close() })

	if _, err := lr.Get(); err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	e := newEnv(t, false)
	e.backend.SetRuntime(func() (azurefunctions.Runner, error) { return lr.Get() })
	e.backend.Configure(azurefunctions.Settings{StartupTimeout: 5 * time.Minute})

	cfg := azurefunctions.AppConfig{
		Kind: webKind, LinuxFx: "DOCKER|busybox:1.36",
		AppCommandLine: "httpd -f -p 8081 -h /home/site/wwwroot",
	}
	settings := map[string]string{"WEBSITES_PORT": "8081", "WEBSITES_ENABLE_APP_SERVICE_STORAGE": "true"}
	slot := azurefunctions.SlotName("shop", "staging")

	for _, name := range []string{"shop", slot} {
		e.backend.PutApp(name, cfg)
		require.NoError(t, e.backend.SetSettings(name, settings))
	}

	_, err := e.backend.Deploy("shop", zipOf(t, map[string]string{"index.html": "production-page"}), "test", "m")
	require.NoError(t, err)
	_, err = e.backend.Deploy(slot, zipOf(t, map[string]string{"index.html": "staging-page"}), "test", "m")
	require.NoError(t, err)

	prodHost, slotHost := "shop."+testDomain, "shop-staging."+testDomain

	resp, body := e.do(t, http.MethodGet, prodHost, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "production-page")

	resp, body = e.do(t, http.MethodGet, slotHost, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "staging-page")

	require.NoError(t, e.backend.WarmUp(t.Context(), slot))
	require.NoError(t, e.backend.SwapContent("shop", slot))

	resp, body = e.do(t, http.MethodGet, prodHost, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "staging-page", "production serves the swapped-in content")

	resp, body = e.do(t, http.MethodGet, slotHost, "/", nil, nil)
	require.Equal(t, http.StatusOK, resp.StatusCode, body)
	assert.Contains(t, body, "production-page")

	e.backend.DeleteApp("shop")
}
