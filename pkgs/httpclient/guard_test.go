package httpclient_test

import (
	"errors"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

func TestLoopbackDialer(t *testing.T) {
	t.Parallel()

	errDenied := errors.New("denied")

	ln, err := new(net.ListenConfig).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	t.Cleanup(func() { _ = ln.Close() })

	tests := []struct {
		name    string
		addr    string
		allow   bool
		wantErr error
	}{
		{"loopback permitted", ln.Addr().String(), false, nil},
		{"loopback permitted when external allowed", ln.Addr().String(), true, nil},
		{"external refused", "192.0.2.1:80", false, errDenied},
		{"hostname refused", "localhost.invalid:80", false, nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d := httpclient.LoopbackDialer(func() bool { return tt.allow }, errDenied)
			c, err := d.DialContext(t.Context(), "tcp", tt.addr)

			switch {
			case tt.wantErr != nil:
				require.ErrorIs(t, err, tt.wantErr)
			case tt.name == "hostname refused":
				require.Error(t, err)
			default:
				require.NoError(t, err)
				_ = c.Close()
			}
		})
	}

	assert.NotNil(t, httpclient.NewTransport())
}
