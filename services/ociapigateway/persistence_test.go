package ociapigateway_test

import (
	"context"
	"strconv"
	"time"

	"github.com/oracle/oci-go-sdk/v65/apigateway"
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
	assert.Len(t, h.GetSupportedOperations(), 64)
}

func TestSnapshotRestoreNewResources(t *testing.T) {
	t.Parallel()

	e := newTLSEnv(t)
	dep := e.mkDeployment(t, *e.gw.Id, "/u", usageSpec())
	a := e.mkAPI(t, "persist-api", petsYAML)
	plan := e.mkPlan(t, "persist-plan", apigateway.Entitlement{
		Name: new("e"), Targets: []apigateway.EntitlementTarget{{DeploymentId: &dep}},
	})
	sub := e.mkSubscriber(t, "persist-tok", plan)

	_, err := e.api.CreateSdk(t.Context(), apigateway.CreateSdkRequest{
		CreateSdkDetails: apigateway.CreateSdkDetails{ApiId: a.Id, TargetLanguage: new("JAVA")},
	})
	require.NoError(t, err)

	e.post(t, "/apis/"+*a.Id+"/actions/addLock", `{"type":"FULL"}`, http.StatusOK)

	snap := e.handler.Snapshot(t.Context())

	other := ociapigateway.NewInMemoryBackend()
	other.SetClock(e.clock.Now)
	require.NoError(t, other.Restore(t.Context(), snap))

	gotAPI, _, err := other.GetAPI(*a.Id)
	require.NoError(t, err)
	assert.Len(t, gotAPI.Locks, 1)

	gotSub, _, err := other.GetSubscriber(sub)
	require.NoError(t, err)
	assert.Equal(t, []string{plan}, gotSub.UsagePlans)

	sdks, err := other.ListSDKs("", *a.Id, "", "", ociapigateway.ListOpts{})
	require.NoError(t, err)
	assert.Len(t, sdks.Data, 1)

	content, _, _, err := other.APIContent(*a.Id)
	require.NoError(t, err)
	assert.Equal(t, petsYAML, content)

	// the TLS material and CA bundles survive, so mutual TLS keeps working on a restored backend
	h := ociapigateway.NewHandler(other)
	h.Port = 0

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	restored := *e
	restored.env = &env{backend: other, port: h.Port, clock: e.clock, base: e.base}
	restored.httpsBase = "https://" + e.serverName + ":" + strconv.Itoa(h.Port)

	other.Sweep()

	open := e.mkDeployment(t, *e.gw.Id, "/m", `{"requestPolicies":{"mutualTls":{"isVerifiedCertificateRequired":true}},`+
		`"routes":[`+stock("/hi", `"GET"`, "secure")+`]}`)
	require.NotEmpty(t, open)

	snap = e.handler.Snapshot(t.Context())
	require.NoError(t, other.Restore(t.Context(), snap))
	other.Sweep()

	good := e.clientCA.issue(t, clientTemplate("c.example.com", time.Now().Add(time.Hour)))

	status, _, err := restored.get(t, "/m/hi", good.tls)
	require.NoError(t, err)
	assert.Equal(t, http.StatusOK, status)

	status, _, err = restored.get(t, "/m/hi")
	require.NoError(t, err)
	assert.Equal(t, http.StatusUnauthorized, status)
}
