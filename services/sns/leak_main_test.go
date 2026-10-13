package sns_test

import (
	"context"
	"errors"
	"net"
	"net/http"
	"testing"

	"github.com/blackbirdworks/gopherstack/pkgs/testleak"
)

var errExternalHost = errors.New("sns tests: external hosts are not reachable")

func loopbackOnlyDial(ctx context.Context, network, addr string) (net.Conn, error) {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	if ip := net.ParseIP(host); host != "localhost" && (ip == nil || !ip.IsLoopback()) {
		return nil, errExternalHost
	}

	var d net.Dialer

	return d.DialContext(ctx, network, addr)
}

// TestMain asserts sns tests leave no goroutines running, guarding
// background workers and per-request goroutines against leaks. Subscribe to an
// http(s) endpoint sends a SubscriptionConfirmation, so external hosts are made unreachable.
func TestMain(m *testing.M) {
	if tr, ok := http.DefaultTransport.(*http.Transport); ok {
		tr.DialContext = loopbackOnlyDial
		tr.Proxy = nil
	}

	testleak.VerifyTestMain(m)
}
