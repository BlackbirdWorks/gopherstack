package portalloc_test

import (
	"fmt"
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/portalloc"
)

func TestNew_InvalidRange(t *testing.T) {
	tests := []struct {
		start, end int
		name       string
	}{
		{name: "zero start", start: 0, end: 10},
		{name: "end <= start", start: 10, end: 10},
		{name: "end < start", start: 10, end: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := portalloc.New(tt.start, tt.end)
			assert.Error(t, err)
		})
	}
}

func TestNew_ValidRange(t *testing.T) {
	a, err := portalloc.New(10000, 10100)
	require.NoError(t, err)
	assert.NotNil(t, a)
}

func TestAcquireRelease(t *testing.T) {
	a, err := portalloc.New(10000, 10003)
	require.NoError(t, err)

	p1, err := a.Acquire("svc-a")
	require.NoError(t, err)
	assert.Equal(t, 10000, p1)

	p2, err := a.Acquire("svc-b")
	require.NoError(t, err)
	assert.Equal(t, 10001, p2)

	assert.True(t, a.IsAllocated(p1))
	assert.True(t, a.IsAllocated(p2))
	assert.Equal(t, 1, a.Available())

	err = a.Release(p1)
	require.NoError(t, err)
	assert.False(t, a.IsAllocated(p1))
	assert.Equal(t, 2, a.Available())
}

func TestAcquire_Exhausted(t *testing.T) {
	a, err := portalloc.New(10000, 10002)
	require.NoError(t, err)

	_, err = a.Acquire("a")
	require.NoError(t, err)

	_, err = a.Acquire("b")
	require.NoError(t, err)

	_, err = a.Acquire("c")
	assert.ErrorIs(t, err, portalloc.ErrNoPortsAvailable)
}

func TestRelease_NotAllocated(t *testing.T) {
	a, err := portalloc.New(10000, 10010)
	require.NoError(t, err)

	err = a.Release(10005)
	assert.ErrorIs(t, err, portalloc.ErrPortNotAllocated)
}

func TestAllocated_Snapshot(t *testing.T) {
	a, err := portalloc.New(10000, 10010)
	require.NoError(t, err)

	_, _ = a.Acquire("alpha")
	_, _ = a.Acquire("beta")

	snap := a.Allocated()
	assert.Len(t, snap, 2)
	assert.Equal(t, "alpha", snap[10000])
	assert.Equal(t, "beta", snap[10001])
}

func TestIsListening(t *testing.T) {
	// Start a real TCP listener on a random free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	assert.True(t, portalloc.IsListening(addr))
	assert.False(t, portalloc.IsListening("127.0.0.1:1")) // port 1 should not be open
}

func TestConcurrentAcquire(t *testing.T) {
	const total = 50
	a, err := portalloc.New(20000, 20000+total)
	require.NoError(t, err)

	results := make(chan int, total)
	errs := make(chan error, total)

	for i := range total {
		go func(i int) {
			p, acqErr := a.Acquire(fmt.Sprintf("worker-%d", i))
			if acqErr != nil {
				errs <- acqErr
			} else {
				results <- p
			}
		}(i)
	}

	seen := make(map[int]bool)

	for range total {
		select {
		case p := <-results:
			assert.False(t, seen[p], "duplicate port %d", p)
			seen[p] = true
		case acqErr := <-errs:
			t.Fatalf("unexpected acquire error: %v", acqErr)
		}
	}
}
