package transfer_test

import (
	"math"
	"net"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/transfer"
)

func TestSFTPWriteAtBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		off     int64
		wantErr bool
	}{
		{name: "zero", off: 0},
		{name: "negative", off: -1, wantErr: true},
		{name: "huge", off: 1 << 40, wantErr: true},
		{name: "overflow", off: math.MaxInt64 - 1, wantErr: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			n, err := transfer.WriteAtForTest([]byte("abcd"), tc.off)
			if tc.wantErr {
				require.ErrorIs(t, err, os.ErrInvalid)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, 4, n)
		})
	}
}

func TestSFTPRemoteIP(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		addr net.Addr
		want string
	}{
		{name: "v4", addr: &net.TCPAddr{IP: net.ParseIP("127.0.0.1"), Port: 53122}, want: "127.0.0.1"},
		{name: "v6", addr: &net.TCPAddr{IP: net.ParseIP("::1"), Port: 22}, want: "::1"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, transfer.RemoteIP(tc.addr))
		})
	}
}
