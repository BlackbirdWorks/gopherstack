package azurekeyvault_test

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore"
	"github.com/Azure/azure-sdk-for-go/sdk/azcore/policy"
	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/services/azurekeyvault"
)

type fakeCredential struct{}

func (fakeCredential) GetToken(context.Context, policy.TokenRequestOptions) (azcore.AccessToken, error) {
	return azcore.AccessToken{Token: "fake-token", ExpiresOn: time.Now().Add(time.Hour)}, nil
}

type testServer struct {
	srv     *httptest.Server
	handler *azurekeyvault.Handler
}

func newTestServer(t *testing.T, settings azurekeyvault.Settings) *testServer {
	t.Helper()

	h := azurekeyvault.NewHandler(azurekeyvault.NewInMemoryBackend(settings.AutoCreateVaults), settings)
	e := echo.New()
	e.Any("/*", h.Handler())

	srv := httptest.NewTLSServer(e)
	t.Cleanup(srv.Close)

	return &testServer{srv: srv, handler: h}
}

func defaultServer(t *testing.T) *testServer {
	t.Helper()

	return newTestServer(t, azurekeyvault.DefaultSettings())
}

// vaultURL addresses a vault by /vaults/<name> path prefix.
func (s *testServer) vaultURL(name string) string { return s.srv.URL + "/vaults/" + name }

func (s *testServer) clientOptions() azcore.ClientOptions {
	return azcore.ClientOptions{Transport: s.srv.Client()}
}

// hostTransport dials the test server for any host, trusting its certificate.
func (s *testServer) hostTransport() *http.Client {
	addr := s.srv.Listener.Addr().String()

	return &http.Client{Transport: &http.Transport{
		TLSClientConfig: &tls.Config{InsecureSkipVerify: true},
		DialContext: func(ctx context.Context, network, _ string) (net.Conn, error) {
			var d net.Dialer

			return d.DialContext(ctx, network, addr)
		},
	}}
}
