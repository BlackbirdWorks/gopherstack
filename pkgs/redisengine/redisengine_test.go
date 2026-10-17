package redisengine_test

import (
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
	"github.com/blackbirdworks/gopherstack/pkgs/redisengine"
)

func TestEngine_StartClose(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		withAlloc bool
	}{
		{name: "ephemeral"},
		{name: "allocator", withAlloc: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var alloc *portalloc.Allocator
			if tt.withAlloc {
				var err error
				alloc, err = portalloc.New(41000, 41100)
				require.NoError(t, err)
			}

			e, err := redisengine.Start(alloc, "test")
			require.NoError(t, err)

			conn, err := net.DialTimeout("tcp", e.Addr(), time.Second)
			require.NoError(t, err)
			require.NoError(t, conn.Close())

			addr := e.Addr()
			e.Close()
			e.Close()

			_, err = net.DialTimeout("tcp", addr, time.Second)
			assert.Error(t, err)
		})
	}
}
