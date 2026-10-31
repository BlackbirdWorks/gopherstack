package httpclient_test

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

var errDenied = errors.New("denied")

func TestLoopbackDialer(t *testing.T) {
	t.Parallel()

	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	tests := []struct {
		wantErr error
		name    string
		addr    string
		allow   bool
	}{
		{name: "loopback permitted", addr: ln.Addr().String()},
		{name: "loopback permitted when external allowed", addr: ln.Addr().String(), allow: true},
		{name: "external refused", addr: "192.0.2.1:80", wantErr: errDenied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := httpclient.LoopbackDialer(func() bool { return tt.allow }, errDenied)
			c, dialErr := d.DialContext(t.Context(), "tcp", tt.addr)

			if tt.wantErr != nil {
				require.ErrorIs(t, dialErr, tt.wantErr)

				return
			}

			require.NoError(t, dialErr)
			_ = c.Close()
		})
	}
}
