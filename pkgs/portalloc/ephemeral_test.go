package portalloc_test

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

func TestAcquireEphemeralUnique(t *testing.T) {
	t.Parallel()

	const n = 200

	var (
		wg    sync.WaitGroup
		mu    sync.Mutex
		ports = make(map[int]int)
	)

	for range n {
		wg.Go(func() {
			p, err := portalloc.AcquireEphemeral()
			if err != nil {
				t.Error(err)

				return
			}

			mu.Lock()
			ports[p]++
			mu.Unlock()
		})
	}

	wg.Wait()

	assert.Len(t, ports, n)

	for p := range ports {
		portalloc.ReleaseEphemeral(p)
	}
}

func TestAcquireFrom(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name  string
		alloc bool
	}{
		{name: "allocator", alloc: true},
		{name: "ephemeral"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var a *portalloc.Allocator

			if tt.alloc {
				var err error

				a, err = portalloc.New(41000, 41010)
				require.NoError(t, err)
			}

			p1, err := portalloc.AcquireFrom(a, "x")
			require.NoError(t, err)

			p2, err := portalloc.AcquireFrom(a, "y")
			require.NoError(t, err)
			assert.NotEqual(t, p1, p2)

			portalloc.ReleaseTo(a, p1)
			portalloc.ReleaseTo(a, p2)
			portalloc.ReleaseTo(a, 0)

			if tt.alloc {
				assert.False(t, a.IsAllocated(p1))
			}
		})
	}
}

func TestEphemeralReleaseReuse(t *testing.T) {
	t.Parallel()

	p, err := portalloc.AcquireEphemeral()
	require.NoError(t, err)

	portalloc.ReleaseEphemeral(p)
	portalloc.ReleaseEphemeral(p)
}
