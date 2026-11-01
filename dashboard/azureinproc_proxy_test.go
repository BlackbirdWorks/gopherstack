package dashboard_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	armSub  = "00000000-0000-0000-0000-000000000000"
	armBase = "/subscriptions/" + armSub + "/resourceGroups/demo/providers/"
)

// idleRuntime runs "containers" until they are removed.
type idleRuntime struct {
	boxes map[string]chan struct{}
	mu    sync.Mutex
	seq   int
}

func (r *idleRuntime) Pull(context.Context, string, *azurearm.ACICredential) (bool, error) {
	return false, nil
}

func (r *idleRuntime) Start(context.Context, container.Spec) (string, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.seq++
	id := "idle-" + string(rune('a'+r.seq))
	r.boxes[id] = make(chan struct{})

	return id, nil
}

func (r *idleRuntime) Wait(ctx context.Context, id string) (int, error) {
	r.mu.Lock()
	gone := r.boxes[id]
	r.mu.Unlock()

	select {
	case <-gone:
	case <-ctx.Done():
	}

	return 0, nil
}

func (r *idleRuntime) Logs(context.Context, string, string) (string, error) { return "", nil }

func (r *idleRuntime) Remove(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if c, ok := r.boxes[id]; ok {
		close(c)
		delete(r.boxes, id)
	}

	return nil
}

// newARMDashboard serves the real Resource Manager (compute, network, container apps) on an idle container runtime
// and seeds it with the given PUTs, issued through the dashboard's own ARM proxy.
func newARMDashboard(t *testing.T, seeds []struct{ path, body string }) *dashboard.DashboardHandler {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	armH, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	ip, ok := armH.Registry.ProviderNamed("Microsoft.ContainerInstance")
	require.True(t, ok)

	aci, ok := ip.(*azurearm.ContainerInstanceProvider)
	require.True(t, ok)

	rt := &idleRuntime{boxes: map[string]chan struct{}{}}
	aci.Engine().SetRuntime(func() (azurearm.ACIRuntime, error) { return rt, nil })
	t.Cleanup(func() { armH.Shutdown(context.Background()) })

	h := dashboard.NewHandler(dashboard.Config{AzureARMOps: armH})

	for _, s := range seeds {
		rec := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPut, "/dashboard/api/v1/azure/arm"+s.path, strings.NewReader(s.body))
		require.NoError(t, h.Handler()(echo.NewContext(req, rec)))
		require.Less(t, rec.Code, http.StatusBadRequest, "%s: %s", s.path, rec.Body.String())
	}

	return h
}

// jsonField walks a decoded JSON document by object keys and array indexes.
func jsonField(t *testing.T, raw []byte, path ...any) string {
	t.Helper()

	var doc any
	require.NoError(t, json.Unmarshal(raw, &doc))

	for _, p := range path {
		switch k := p.(type) {
		case string:
			m, ok := doc.(map[string]any)
			require.True(t, ok, "not an object at %q", k)
			doc = m[k]
		case int:
			a, ok := doc.([]any)
			require.True(t, ok && k < len(a), "no element %d", k)
			doc = a[k]
		}
	}

	s, ok := doc.(string)
	require.True(t, ok, "not a string: %v", doc)

	return s
}
