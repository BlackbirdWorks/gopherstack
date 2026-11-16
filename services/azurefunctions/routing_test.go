package azurefunctions_test

import (
	"context"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/azurefunctions"
)

func routingEnv(t *testing.T) *env {
	t.Helper()

	e := newEnv(t, true)
	e.backend.PutApp("app", azurefunctions.AppConfig{LinuxFx: "NODE|20"})
	e.backend.PutApp(azurefunctions.SlotName("app", "staging"), azurefunctions.AppConfig{LinuxFx: "NODE|20"})

	for _, n := range []string{"app", azurefunctions.SlotName("app", "staging")} {
		_, err := e.backend.Deploy(n, helloZip(t), "t", "m")
		require.NoError(t, err)
	}

	return e
}

func (e *env) servedSlots() []string {
	specs := e.rt.specs()
	out := make([]string, 0, len(specs))

	for _, s := range specs {
		out = append(out, envOf(s)["WEBSITE_SLOT_NAME"])
	}

	return out
}

func TestSlotTrafficRouting(t *testing.T) {
	t.Parallel()

	tests := []struct {
		hdr        map[string]string
		name       string
		path       string
		wantSlot   string
		wantCookie string
		rules      []azurefunctions.RoutingRule
	}{

		{nil, "no rules, no hint", "/api/x", "Production", "", nil},
		{nil, "query selects slot", "/api/x?x-ms-routing-name=staging", "staging", "x-ms-routing-name=staging", nil},
		{nil, "query self", "/api/x?x-ms-routing-name=self", "Production", "x-ms-routing-name=self", nil},
		{nil, "query unknown slot", "/api/x?x-ms-routing-name=nope", "Production", "", nil},
		{
			map[string]string{"Cookie": "x-ms-routing-name=staging"}, "cookie pins slot", "/api/x", "staging", "",
			nil,
		},
		{
			nil, "rule sends all traffic", "/api/x", "staging", "x-ms-routing-name=staging",
			[]azurefunctions.RoutingRule{{Slot: "staging", Percent: 100}},
		},
		{
			map[string]string{"Cookie": "x-ms-routing-name=self"}, "cookie overrides rule", "/api/x", "Production",
			"", []azurefunctions.RoutingRule{{Slot: "staging", Percent: 100}},
		},
		{
			nil, "zero percent keeps production", "/api/x", "Production", "x-ms-routing-name=self",
			[]azurefunctions.RoutingRule{{Slot: "staging", Percent: 0}},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e := routingEnv(t)
			e.backend.SetSlotHooks(func(string) []azurefunctions.RoutingRule { return tt.rules }, nil)

			resp, _ := e.do(t, http.MethodGet, "app."+testDomain, tt.path, nil, tt.hdr)
			require.Equal(t, http.StatusOK, resp.StatusCode)
			assert.Equal(t, []string{tt.wantSlot}, e.servedSlots())

			cookie := ""
			if c := resp.Header.Get("Set-Cookie"); c != "" {
				cookie, _, _ = splitCookie(c)
			}

			assert.Equal(t, tt.wantCookie, cookie)
		})
	}
}

func splitCookie(c string) (string, string, bool) {
	for i := range c {
		if c[i] == ';' {
			return c[:i], c[i+1:], true
		}
	}

	return c, "", false
}

func TestAutoSwapHook(t *testing.T) {
	t.Parallel()

	var (
		mu     sync.Mutex
		called []string
	)

	e := routingEnv(t)
	e.backend.SetSlotHooks(nil, func(_ context.Context, name string) {
		mu.Lock()
		called = append(called, name)
		mu.Unlock()
	})

	for _, label := range []string{"app", "app-staging"} {
		resp, _ := e.do(
			t,
			http.MethodPost,
			label+".scm."+testDomain,
			"/api/zipdeploy",
			helloZip(t),
			e.kuduAuth(t, map[string]string{
				"app": "app", "app-staging": "app/staging",
			}[label]),
		)
		require.Equal(t, http.StatusOK, resp.StatusCode)
	}

	require.Eventually(t, func() bool {
		mu.Lock()
		defer mu.Unlock()

		return len(called) == 1
	}, 10*time.Second, 10*time.Millisecond)

	mu.Lock()
	assert.Equal(t, []string{"app/staging"}, called, "only a slot deployment reports to the auto swap hook")
	mu.Unlock()
}
