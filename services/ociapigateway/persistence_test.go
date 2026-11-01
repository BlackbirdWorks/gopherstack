package ociapigateway_test

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	ociapigateway "github.com/blackbirdworks/gopherstack/services/ociapigateway"
)

func TestSnapshotRestore(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "persist")
	e.mkDeployment(t, *gw.Id, "/v1", stockSpec(200, "restored"))

	snap := e.handler.Snapshot(t.Context())
	require.NotEmpty(t, snap)

	other := ociapigateway.NewInMemoryBackend()
	other.SetClock(e.clock.Now)
	require.NoError(t, ociapigateway.NewHandler(other).Restore(t.Context(), snap))

	other.Sweep()

	rec := httptest.NewRecorder()
	r := httptest.NewRequestWithContext(t.Context(), http.MethodGet, "/_apigw/"+*gw.Id+"/v1/hi", nil)
	other.ServeData(rec, r)

	assert.Equal(t, http.StatusOK, rec.Code)
	assert.Equal(t, "restored", rec.Body.String())

	got, _, err := other.GetGateway(*gw.Id)
	require.NoError(t, err)
	assert.Equal(t, "persist", got.DisplayName)
}

func TestRestoreRejectsBadSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"null gateway", `{"version":1,"gateways":{"a":null}}`, true},
		{
			"unparseable spec",
			`{"version":1,"deployments":{"d":{"id":"d","state":"ACTIVE","specification":{"routes":[{"path":"x"}]}}}}`,
			true,
		},
		{"old version starts empty", `{"version":0,"gateways":{"a":null}}`, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := ociapigateway.NewHandler(ociapigateway.NewInMemoryBackend())
			err := h.Restore(t.Context(), []byte(tt.body))

			if tt.wantErr {
				require.Error(t, err)

				return
			}

			require.NoError(t, err)
		})
	}
}

func TestResetClearsState(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "gone")
	e.mkDeployment(t, *gw.Id, "/v1", stockSpec(200, "x"))

	e.handler.Reset()

	assert.Equal(t, http.StatusNotFound, e.call(t, *gw.Id, http.MethodGet, "/v1/hi", nil, "").status)

	_, _, err := e.backend.GetGateway(*gw.Id)
	require.Error(t, err)
}

func TestProviderInit(t *testing.T) {
	t.Parallel()

	_, err := (&ociapigateway.Provider{}).Init(nil)
	require.ErrorIs(t, err, ociapigateway.ErrNilAppContext)

	reg, err := (&ociapigateway.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := reg.(*ociapigateway.Handler)
	require.True(t, ok)
	assert.Equal(t, ociapigateway.DefaultPort, h.Port)
	assert.Equal(t, "OCIAPIGateway", h.Name())
	assert.Len(t, h.GetSupportedOperations(), 22)
}
