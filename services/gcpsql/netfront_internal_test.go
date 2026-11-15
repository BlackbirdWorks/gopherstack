package gcpsql

import (
	"net"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFrontDoorSurvivesHandlerPanic(t *testing.T) {
	t.Parallel()

	tests := []struct{ name string }{{name: "panic_then_accept"}}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var calls atomic.Int32
			allow := func(net.IP) bool {
				if calls.Add(1) == 1 {
					panic("boom")
				}

				return false
			}

			f, err := newFrontDoor(t.Context(), allow, func() (string, int) { return "", 0 })
			require.NoError(t, err)
			t.Cleanup(func() { _ = f.ln.Close(); f.wg.Wait() })

			var d net.Dialer

			for range 2 {
				c, derr := d.DialContext(t.Context(), "tcp", net.JoinHostPort("127.0.0.1", strconv.Itoa(f.port)))
				require.NoError(t, derr)

				buf := make([]byte, 1)
				_, rerr := c.Read(buf)
				require.Error(t, rerr, "server closes the connection")
				_ = c.Close()
			}

			assert.EqualValues(t, 2, calls.Load())
		})
	}
}
