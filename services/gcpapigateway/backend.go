package gcpapigateway

import (
	"context"
	"errors"
	"net"
	"net/http"
	"strings"
	"syscall"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

const (
	dialerKeepAlive = 30 * time.Second
	localhostSuffix = ".localhost"
	loopbackIP      = "127.0.0.1"
)

// ErrDestinationNotPermitted is returned when a backend or JWKS host resolves outside loopback.
var ErrDestinationNotPermitted = errors.New(
	"gcpapigateway: destination is outside loopback and external backends are not allowed",
)

// newBackendClient builds the data-plane HTTP client. Redirects are returned to the caller, TLS is always
// verified, and the dialer refuses non-loopback peers unless external backends are allowed. Cloud Run and
// Functions *.localhost serving URLs are sent to the loopback interface.
func (b *InMemoryBackend) newBackendClient() *http.Client {
	d := &net.Dialer{
		KeepAlive: dialerKeepAlive,
		Control: func(_, address string, _ syscall.RawConn) error {
			if b.externalAllowed() {
				return nil
			}

			host, _, err := net.SplitHostPort(address)
			if err != nil {
				return err
			}

			if ip := net.ParseIP(host); ip == nil || !ip.IsLoopback() {
				return ErrDestinationNotPermitted
			}

			return nil
		},
	}

	tr := httpclient.NewTransport()
	tr.Proxy = nil
	tr.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		if host, port, err := net.SplitHostPort(addr); err == nil && strings.HasSuffix(host, localhostSuffix) {
			addr = net.JoinHostPort(loopbackIP, port)
		}

		return d.DialContext(ctx, network, addr)
	}

	return &http.Client{
		Transport:     tr,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}
}
