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
	t.Parallel()

	tests := []struct {
		name  string
		start int
		end   int
	}{
		{name: "zero start", start: 0, end: 10},
		{name: "end <= start", start: 10, end: 10},
		{name: "end < start", start: 10, end: 5},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := portalloc.New(tt.start, tt.end)
			assert.ErrorIs(t, err, portalloc.ErrInvalidRange)
		})
	}
}

func TestNew_ValidRange(t *testing.T) {
	t.Parallel()

	a, err := portalloc.New(10000, 10100)
	require.NoError(t, err)
	assert.NotNil(t, a)
}

func TestAcquireRelease(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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
	t.Parallel()

	a, err := portalloc.New(10000, 10010)
	require.NoError(t, err)

	err = a.Release(10005)
	assert.ErrorIs(t, err, portalloc.ErrPortNotAllocated)
}

func TestAllocated_Snapshot(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

	// Start a real TCP listener on a random free port.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	addr := fmt.Sprintf("127.0.0.1:%d", ln.Addr().(*net.TCPAddr).Port)
	assert.True(t, portalloc.IsListening(addr))

	// Bind a second listener to get a free port, close it, then assert IsListening
	// returns false. This is more deterministic than using a well-known port like :1.
	ln2, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	freePort := ln2.Addr().(*net.TCPAddr).Port
	require.NoError(t, ln2.Close())

	freeAddr := fmt.Sprintf("127.0.0.1:%d", freePort)
	assert.False(t, portalloc.IsListening(freeAddr))
}

func TestConcurrentAcquire(t *testing.T) {
	t.Parallel()

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
			require.NoError(t, acqErr, "unexpected acquire error")
		}
	}
}

// TestReserve_InRangeBlocksAcquire is a regression test for the cross-service
// port collision this method exists to prevent: a service that binds a fixed
// port directly (outside this allocator) must be able to keep Acquire from
// ever handing that same port number to something else.
func TestReserve_InRangeBlocksAcquire(t *testing.T) {
	t.Parallel()

	a, err := portalloc.New(10000, 10003)
	require.NoError(t, err)

	require.NoError(t, a.Reserve(10000, "azureblob"))
	assert.True(t, a.IsAllocated(10000))
	assert.Equal(t, 2, a.Available())

	p1, err := a.Acquire("svc-a")
	require.NoError(t, err)
	assert.Equal(t, 10001, p1, "Acquire must skip the reserved port")

	p2, err := a.Acquire("svc-b")
	require.NoError(t, err)
	assert.Equal(t, 10002, p2)

	_, err = a.Acquire("svc-c")
	assert.ErrorIs(t, err, portalloc.ErrNoPortsAvailable, "reserved port must never be handed out")
}

func TestReserve_OutOfRangeIsNoop(t *testing.T) {
	t.Parallel()

	a, err := portalloc.New(10000, 10003)
	require.NoError(t, err)

	require.NoError(t, a.Reserve(1883, "iot-mqtt"))
	assert.False(t, a.IsAllocated(1883), "a port outside the range is never tracked")
	assert.Equal(t, 3, a.Available())
}

func TestReserve_AlreadyUsedErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		setup   func(t *testing.T, a *portalloc.Allocator)
		wantErr error
	}{
		{
			name: "already reserved",
			setup: func(t *testing.T, a *portalloc.Allocator) {
				t.Helper()
				require.NoError(t, a.Reserve(10000, "first"))
			},
			wantErr: portalloc.ErrPortAlreadyReserved,
		},
		{
			name: "already acquired",
			setup: func(t *testing.T, a *portalloc.Allocator) {
				t.Helper()
				_, err := a.Acquire("svc-a")
				require.NoError(t, err)
			},
			wantErr: portalloc.ErrPortAlreadyReserved,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			a, err := portalloc.New(10000, 10003)
			require.NoError(t, err)

			tt.setup(t, a)

			err = a.Reserve(10000, "second")
			assert.ErrorIs(t, err, tt.wantErr)
		})
	}
}
