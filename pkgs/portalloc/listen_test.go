package portalloc_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

func TestListenTCP(t *testing.T) {
	t.Parallel()

	t.Run("port zero is ephemeral", func(t *testing.T) {
		t.Parallel()

		ln, port, err := portalloc.ListenTCP(t.Context(), 0)
		require.NoError(t, err)
		t.Cleanup(func() { _ = ln.Close() })
		assert.Positive(t, port)

		_, _, err = portalloc.ListenTCP(t.Context(), port)
		require.Error(t, err, "the bound port is held, not just probed")
	})
}
