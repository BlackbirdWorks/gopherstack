package portalloctest_test

import (
	"net"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc/portalloctest"
)

func TestNewDisjointAndSkipsBound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		span int
	}{
		{name: "small", span: 2},
		{name: "wide", span: 8},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a := portalloctest.New(t, tt.span)
			b := portalloctest.New(t, tt.span)

			pa, err := a.Acquire("a")
			require.NoError(t, err)

			ln, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", ":"+strconv.Itoa(pa))
			require.NoError(t, err)
			t.Cleanup(func() { _ = ln.Close() })

			pb, err := b.Acquire("b")
			require.NoError(t, err)
			assert.NotEqual(t, pa, pb)

			for range 4 {
				c := portalloctest.New(t, tt.span)
				pc, cErr := c.Acquire("c")
				require.NoError(t, cErr)
				assert.NotEqual(t, pa, pc, "must not hand out a port another caller is bound to")
			}
		})
	}
}
