package ociapigateway_test

import (
	"context"
	"crypto/tls"
	"io"
	"net"
	"net/http"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type tlsReply struct {
	body   string
	dns    []string
	proto  int
	status int
}

func tlsGet(t *testing.T, port int, serverName, path string) tlsReply {
	t.Helper()

	tr := &http.Transport{
		ForceAttemptHTTP2: true,
		TLSClientConfig:   &tls.Config{InsecureSkipVerify: true, ServerName: serverName, MinVersion: tls.VersionTLS12},
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", "127.0.0.1:"+strconv.Itoa(port))
		},
	}
	t.Cleanup(tr.CloseIdleConnections)

	req, err := http.NewRequestWithContext(t.Context(), http.MethodGet, "https://"+serverName+path, nil)
	require.NoError(t, err)

	resp, err := (&http.Client{Transport: tr}).Do(req)
	require.NoError(t, err)

	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return tlsReply{
		status: resp.StatusCode, body: string(body), proto: resp.ProtoMajor, dns: resp.TLS.PeerCertificates[0].DNSNames,
	}
}

func freePort(t *testing.T) int {
	t.Helper()

	var lc net.ListenConfig

	ln, err := lc.Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := ln.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln.Close())

	return port
}

func TestGatewayWithoutCertificateServesTLSOverHTTP2(t *testing.T) {
	t.Parallel()

	e := newEnv(t, envOpts{})
	gw := e.mkGateway(t, "no-cert")
	e.mkDeployment(t, *gw.Id, "/p", `{"routes":[`+stock("/hi", `"GET"`, "secure")+`]}`)

	got := tlsGet(t, e.port, *gw.Hostname, "/p/hi")
	assert.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, "secure", got.body)
	assert.Equal(t, 2, got.proto, "h2 is negotiated through ALPN")
	assert.Contains(t, got.dns, *gw.Hostname, "the generated certificate names the gateway hostname")
}

func TestTLSPortServesGateways(t *testing.T) {
	t.Parallel()

	port := freePort(t)
	e := newEnv(t, envOpts{tlsPort: port})
	gw := e.mkGateway(t, "tls-port")
	e.mkDeployment(t, *gw.Id, "/p", `{"routes":[`+stock("/hi", `"GET"`, "on-tls-port")+`]}`)

	got := tlsGet(t, port, *gw.Hostname, "/p/hi")
	assert.Equal(t, http.StatusOK, got.status)
	assert.Equal(t, "on-tls-port", got.body)
}
