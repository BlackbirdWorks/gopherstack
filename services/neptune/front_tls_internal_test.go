package neptune

import (
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTLSListener(t *testing.T) {
	t.Parallel()

	raw, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	ln, pem, err := tlsListener(raw, "127.0.0.1")
	require.NoError(t, err)

	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	srv.Listener = ln
	srv.Start()
	t.Cleanup(srv.Close)

	pool := x509.NewCertPool()
	require.True(t, pool.AppendCertsFromPEM(pem))

	secure := &http.Client{
		Transport: &http.Transport{TLSClientConfig: &tls.Config{RootCAs: pool, MinVersion: tls.VersionTLS12}},
	}
	t.Cleanup(secure.CloseIdleConnections)

	req, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		"https://"+raw.Addr().String()+"/status",
		http.NoBody,
	)
	require.NoError(t, err)

	resp, err := secure.Do(req)
	require.NoError(t, err)
	_ = resp.Body.Close()
	assert.Equal(t, http.StatusNoContent, resp.StatusCode)
	assert.GreaterOrEqual(t, int(resp.TLS.Version), tls.VersionTLS12)

	plain, err := http.NewRequestWithContext(
		t.Context(),
		http.MethodGet,
		"http://"+raw.Addr().String()+"/status",
		http.NoBody,
	)
	require.NoError(t, err)

	pc := &http.Client{Transport: &http.Transport{}}
	t.Cleanup(pc.CloseIdleConnections)

	if r, perr := pc.Do(plain); perr == nil {
		_ = r.Body.Close()
		assert.NotEqual(t, http.StatusNoContent, r.StatusCode)
	}
}
