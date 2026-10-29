package gcpcompute_test

import (
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/gcpcompute"
)

func freePort(t *testing.T) int {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

type fakeConfig struct{ port int }

func (f fakeConfig) GetGCPComputeSettings() gcpcompute.Settings {
	return gcpcompute.Settings{Port: f.port}
}

func TestHandlerMetadata(t *testing.T) {
	t.Parallel()

	h := gcpcompute.NewHandler(gcpcompute.NewInMemoryBackend())
	assert.Equal(t, "GCPCompute", h.Name())
	assert.Equal(t, gcpcompute.DefaultPort, h.Port)
	assert.Equal(t, gcpcompute.Settings{Port: 8142}, gcpcompute.DefaultSettings())
	assert.Equal(t, 0, h.MatchPriority())
	assert.GreaterOrEqual(t, len(h.GetSupportedOperations()), 100)

	for _, op := range []string{
		"instances@zones.insert", "instances@zones.setMetadata", "networks@global.switchToCustomMode",
		"subnetworks@regions.expandIpCidrRange", "instances@agg.aggList", "operations@zones.wait",
		"addresses@global.insert", "images@global.getFromFamily", "routers@regions.getRouterStatus",
	} {
		assert.Contains(t, h.GetSupportedOperations(), op)
	}

	matcher := h.RouteMatcher()
	e := echo.New()
	c := e.NewContext(nil, nil)
	assert.False(t, matcher(c))

	_, err := (&gcpcompute.Provider{}).Init(nil)
	require.ErrorIs(t, err, gcpcompute.ErrNilAppContext)
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	tests := []struct {
		cfg  any
		name string
		port int
	}{
		{nil, "defaults", gcpcompute.DefaultPort},
		{fakeConfig{port: 9999}, "config provider", 9999},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			reg, err := (&gcpcompute.Provider{}).Init(&service.AppContext{Config: tt.cfg})
			require.NoError(t, err)

			h, ok := reg.(*gcpcompute.Handler)
			require.True(t, ok)
			assert.Equal(t, tt.port, h.Port)
			assert.Equal(t, "GCPCompute", (&gcpcompute.Provider{}).Name())
		})
	}
}

func TestStartWorkerServesAndConflicts(t *testing.T) {
	t.Parallel()

	first := gcpcompute.NewHandler(gcpcompute.NewInMemoryBackend())
	first.Port = freePort(t)
	require.NoError(t, first.StartWorker(t.Context()))
	t.Cleanup(func() { first.Shutdown(t.Context()) })
	require.NotEmpty(t, first.Addr())

	resp, err := testclient.New(t).
		Get("http://127.0.0.1:" + strconv.Itoa(first.Port) + "/compute/v1/projects/p/global/networks")
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	second := gcpcompute.NewHandler(gcpcompute.NewInMemoryBackend())
	second.Port = first.Port
	require.Error(t, second.StartWorker(t.Context()))
	second.Shutdown(t.Context())

	first.Reset()
}
