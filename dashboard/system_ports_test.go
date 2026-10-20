package dashboard_test

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/dashboard"
	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

func TestCollectAllocatedPorts(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		listening bool
	}{
		{"listening", true},
		{"down", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ln, err := net.Listen("tcp", "127.0.0.1:0")
			require.NoError(t, err)

			fixedPort := ln.Addr().(*net.TCPAddr).Port
			if !tt.listening {
				require.NoError(t, ln.Close())
			} else {
				t.Cleanup(func() { _ = ln.Close() })
			}

			alloc, err := portalloc.New(1, 2)
			require.NoError(t, err)
			require.NoError(t, alloc.Reserve(fixedPort, "gcpfake"))
			require.NoError(t, alloc.Reserve(0, "disabled"))

			poolPort, err := alloc.Acquire("lambda:x")
			require.NoError(t, err)

			got := dashboard.CollectAllocatedPorts(alloc)
			require.Len(t, got, 2)

			byPort := map[int]dashboard.AllocatedPort{}
			for _, p := range got {
				byPort[p.Port] = p
			}

			want := dashboard.AllocatedPort{Port: fixedPort, Label: "gcpfake", Fixed: true, Listening: tt.listening}
			assert.Equal(t, want, byPort[fixedPort])
			assert.False(t, byPort[poolPort].Fixed)
		})
	}

	assert.Empty(t, dashboard.CollectAllocatedPorts(nil))
}
