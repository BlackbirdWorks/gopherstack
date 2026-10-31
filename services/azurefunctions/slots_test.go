package azurefunctions_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func slotEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t, true)
	e.backend.PutApp("shop", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts"})
	e.backend.PutApp(
		azurefunctions.SlotName("shop", "stg"),
		azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts"},
	)

	return e
}

func deployMarker(t *testing.T, e *env, name, marker string) {
	t.Helper()

	_, err := e.backend.Deploy(name, zipOf(t, map[string]string{"server.js": "1", "marker.txt": marker}), "test", "m")
	require.NoError(t, err)
}

func markerOf(t *testing.T, spec string, e *env) string {
	t.Helper()

	for _, s := range e.rt.specs() {
		if env := envOf(s); env["WEBSITE_HOSTNAME"] == spec {
			if dir := mountSource(s, "/home/site/wwwroot"); dir != "" {
				if got, err := readFile(dir + "/marker.txt"); err == nil {
					return got
				}
			}
		}
	}

	return ""
}

func TestSlots_HostnameRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		host     string
		wantSlot string
		wantCode int
	}{
		{"production", "shop." + testDomain, "Production", http.StatusOK},
		{"slot", "shop-stg." + testDomain, "stg", http.StatusOK},
		{"slot with port", "shop-stg." + testDomain + ":10012", "stg", http.StatusOK},
		{"unknown slot", "shop-nope." + testDomain, "", http.StatusNotFound},
		{"slot of unknown app", "ghost-stg." + testDomain, "", http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := slotEnv(t)
			deployMarker(t, e, "shop", "prod")
			deployMarker(t, e, "shop/stg", "stg")

			resp, _ := e.do(t, http.MethodGet, tt.host, "/x", nil, nil)
			require.Equal(t, tt.wantCode, resp.StatusCode)

			if tt.wantSlot == "" {
				return
			}

			found := false

			for _, spec := range e.rt.specs() {
				if envOf(spec)["WEBSITE_SLOT_NAME"] == tt.wantSlot {
					found = true

					assert.Equal(t, "shop", envOf(spec)["WEBSITE_SITE_NAME"])
				}
			}

			assert.True(t, found, "a host with WEBSITE_SLOT_NAME=%s started", tt.wantSlot)
		})
	}
}

func TestSlots_KuduIsPerSlot(t *testing.T) {
	t.Parallel()

	e := slotEnv(t)

	user, pass, scm, err := e.backend.PublishingCredentials("shop/stg")
	require.NoError(t, err)
	assert.Equal(t, "$shop__stg", user)
	assert.Equal(t, "http://shop-stg.scm."+testDomain+":10012", scm)

	body := zipOf(t, map[string]string{"server.js": "1", "marker.txt": "kudu-stg"})
	resp, out := e.do(t, http.MethodPost, "shop-stg.scm."+testDomain, "/api/zipdeploy", body,
		map[string]string{"basic": user + ":" + pass})
	require.Equal(t, http.StatusOK, resp.StatusCode, out)

	assert.NotEmpty(t, e.backend.Package("shop/stg"))
	assert.Empty(t, e.backend.Package("shop"), "the production site is untouched")

	resp, _ = e.do(t, http.MethodPost, "shop.scm."+testDomain, "/api/zipdeploy", body,
		map[string]string{"basic": user + ":" + pass})
	assert.Equal(t, http.StatusUnauthorized, resp.StatusCode, "slot credentials do not open the production Kudu")
}

func TestSlots_SwapContent(t *testing.T) {
	t.Parallel()

	e := slotEnv(t)
	deployMarker(t, e, "shop", "prod")
	deployMarker(t, e, "shop/stg", "stg")

	for _, host := range []string{"shop." + testDomain, "shop-stg." + testDomain} {
		resp, _ := e.do(t, http.MethodGet, host, "/", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	require.Equal(t, 2, e.rt.running())
	before := len(e.rt.specs())

	require.NoError(t, e.backend.SwapContent("shop", "shop/stg"))
	assert.Zero(t, e.rt.running(), "both apps restart: the old hosts are gone")

	for _, host := range []string{"shop." + testDomain, "shop-stg." + testDomain} {
		resp, _ := e.do(t, http.MethodGet, host, "/", nil, nil)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	assert.Len(t, e.rt.specs(), before+2)
	assert.Equal(t, "stg", markerOf(t, "shop."+testDomain+":10012", e), "production now runs the staged content")
	assert.Equal(t, "prod", markerOf(t, "shop-stg."+testDomain+":10012", e))

	require.ErrorIs(t, e.backend.SwapContent("shop", "shop/nope"), azurefunctions.ErrAppNotFound)
}

func TestSlots_WarmUp(t *testing.T) {
	t.Parallel()

	e := slotEnv(t)
	deployMarker(t, e, "shop/stg", "stg")

	require.NoError(t, e.backend.WarmUp(t.Context(), "shop/stg"))
	assert.Equal(t, 1, e.rt.running(), "warm-up starts the host")

	require.NoError(t, e.backend.WarmUp(t.Context(), "shop"), "an app without content has nothing to warm")
	assert.Equal(t, 1, e.rt.running())

	require.ErrorIs(t, e.backend.WarmUp(t.Context(), "shop/nope"), azurefunctions.ErrAppNotFound)
}

func TestSlots_DeleteAppRemovesSlots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		delete    string
		wantNames []string
	}{
		{"slot only", "shop/stg", []string{"shop"}},
		{"site and its slots", "shop", nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := slotEnv(t)
			e.backend.PutApp("shop2", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts"})
			e.backend.PutApp("shop2/keep", azurefunctions.AppConfig{Kind: webKind, LinuxFx: "NODE|20-lts"})

			e.backend.DeleteApp(tt.delete)

			want := append(append([]string(nil), tt.wantNames...), "shop2", "shop2/keep")
			assert.ElementsMatch(t, want, e.backend.Names())
		})
	}
}

func TestSlots_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	src := slotEnv(t)
	require.NoError(t, src.backend.SetSettings("shop/stg", map[string]string{"A": "slot"}))
	deployMarker(t, src, "shop/stg", "stg")

	dst := newEnv(t, true)
	require.NoError(t, dst.handler.Restore(t.Context(), src.handler.Snapshot(t.Context())))

	assert.ElementsMatch(t, []string{"shop", "shop/stg"}, dst.backend.Names())

	settings, err := dst.backend.Settings("shop/stg")
	require.NoError(t, err)
	assert.Equal(t, map[string]string{"A": "slot"}, settings)

	info, ok := dst.backend.App("shop/stg")
	require.True(t, ok)
	assert.Equal(t, "shop-stg."+testDomain+":10012", info.HostName)

	resp, _ := dst.do(t, http.MethodGet, "shop-stg."+testDomain, "/", nil, nil)
	assert.Equal(t, http.StatusOK, resp.StatusCode, "a restored slot serves from its own package")
	assert.Equal(t, "stg", markerOf(t, "shop-stg."+testDomain+":10012", dst))
}
