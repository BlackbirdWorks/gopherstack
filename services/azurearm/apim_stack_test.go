package azurearm_test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/arm"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/cloud"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/apimanagement/armapimanagement/v2"
	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/azurearm"
)

const (
	apimRG  = "rg-apim"
	apimSvc = "gw1"
)

type apimClock struct{ ns atomic.Int64 }

func (c *apimClock) Now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *apimClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type apimStack struct {
	f       *armapimanagement.ClientFactory
	clock   *apimClock
	prov    *azurearm.APIMProvider
	handler *azurearm.Handler
	hc      *http.Client
	url     string
}

func newAPIMStack(t *testing.T) *apimStack {
	t.Helper()

	svc, err := (&azurearm.Provider{}).Init(&service.AppContext{})
	require.NoError(t, err)

	h, ok := svc.(*azurearm.Handler)
	require.True(t, ok)

	t.Cleanup(func() { h.Shutdown(context.Background()) })

	pr, ok := h.Registry.ProviderNamed("Microsoft.ApiManagement")
	require.True(t, ok)

	prov, ok := pr.(*azurearm.APIMProvider)
	require.True(t, ok)

	clock := &apimClock{}
	clock.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())
	prov.SetClock(clock.Now)

	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	f, err := armapimanagement.NewClientFactory(aciSub, aciCred{}, &arm.ClientOptions{
		Cloud: cloud.Configuration{Services: map[cloud.ServiceName]cloud.ServiceConfiguration{
			cloud.ResourceManager: {Endpoint: srv.URL, Audience: "https://management.core.windows.net/"},
		}},
		Transport: srv.Client(),
		Retry:     policy.RetryOptions{MaxRetries: -1},
	})
	require.NoError(t, err)

	hc := testclient.New(t)
	tr, ok := hc.Transport.(*http.Transport)
	require.True(t, ok)

	srvTr, ok := srv.Client().Transport.(*http.Transport)
	require.True(t, ok)

	tr.TLSClientConfig = srvTr.TLSClientConfig.Clone()

	s := &apimStack{f: f, clock: clock, prov: prov, handler: h, hc: hc, url: srv.URL}
	s.raw(t, http.MethodPut, "/subscriptions/"+aciSub+"/resourcegroups/"+apimRG, `{"location":"westus"}`, nil)

	return s
}

func (s *apimStack) raw(t *testing.T, method, path, body string, hdr map[string]string) (int, string, http.Header) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, s.url+path, strings.NewReader(body))
	require.NoError(t, err)

	req.Header.Set("Content-Type", "application/json")

	for k, v := range hdr {
		if strings.EqualFold(k, "Host") {
			req.Host = v

			continue
		}

		req.Header.Set(k, v)
	}

	resp, err := s.hc.Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	b, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(b), resp.Header
}

func (s *apimStack) createService(t *testing.T, name string, sku armapimanagement.SKUType, capacity int32) {
	t.Helper()

	ctx := t.Context()

	poller, err := s.f.NewServiceClient().BeginCreateOrUpdate(ctx, apimRG, name, armapimanagement.ServiceResource{
		Location: new("westus"),
		SKU:      &armapimanagement.ServiceSKUProperties{Name: &sku, Capacity: &capacity},
		Properties: &armapimanagement.ServiceProperties{
			PublisherEmail: new("ops@example.com"), PublisherName: new("Ops"),
		},
	}, nil)
	require.NoError(t, err)

	s.clock.Advance(time.Hour)

	res, err := poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{Frequency: 5 * time.Millisecond})
	require.NoError(t, err)
	require.Equal(t, "Succeeded", *res.Properties.ProvisioningState)
}

func (s *apimStack) devService(t *testing.T) {
	t.Helper()
	s.createService(t, apimSvc, armapimanagement.SKUTypeDeveloper, 1)
}

func (s *apimStack) gw(t *testing.T, method, path, body string, hdr map[string]string) (int, string, http.Header) {
	t.Helper()

	return s.raw(t, method, "/_apim/"+apimSvc+path, body, hdr)
}

func (s *apimStack) armPath(rest string) string {
	return "/subscriptions/" + aciSub + "/resourceGroups/" + apimRG + "/providers/Microsoft.ApiManagement/service/" +
		apimSvc + rest + "?api-version=2022-08-01"
}
