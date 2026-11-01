package dsql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var errLaunch = errors.New("launch refused")

type fakeInstance struct {
	closed chan<- string
	id     string
}

func (f *fakeInstance) Addr() (string, int) { return "127.0.0.1", 15432 }

func (f *fakeInstance) Close(context.Context) { f.closed <- f.id }

// fakeLauncher blocks each launch until gate is closed, then succeeds or fails with err.
type fakeLauncher struct {
	gate    chan struct{}
	started chan string
	closed  chan string
	err     error
}

func newFakeLauncher(err error) *fakeLauncher {
	return &fakeLauncher{
		gate: make(chan struct{}), started: make(chan string, 16), closed: make(chan string, 16), err: err,
	}
}

func (l *fakeLauncher) Launch(ctx context.Context, ref ClusterRef) (Instance, error) {
	l.started <- ref.ID

	select {
	case <-l.gate:
	case <-ctx.Done():
		return nil, ctx.Err()
	}

	if l.err != nil {
		return nil, l.err
	}

	return &fakeInstance{id: ref.ID, closed: l.closed}, nil
}

type testClock struct{ t time.Time }

func (c *testClock) advance(b *InMemoryBackend, d time.Duration) {
	c.t = c.t.Add(d)
	now := c.t
	b.SetClock(func() time.Time { return now })
}

func newEngineBackend(t *testing.T, l Launcher) (*InMemoryBackend, *testClock) {
	t.Helper()

	b := NewInMemoryBackend()
	clk := &testClock{t: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	clk.advance(b, 0)
	b.EnableEngine(l)
	t.Cleanup(b.Close)

	return b, clk
}

func waitReady(t *testing.T, b *InMemoryBackend, id string) {
	t.Helper()

	require.Eventually(t, func() bool {
		_, _, ok := b.ConnectAddr(id)

		return ok
	}, 10*time.Second, time.Millisecond)
}

func statusOf(t *testing.T, b *InMemoryBackend, id string) string {
	t.Helper()

	c, err := b.GetCluster(id)
	require.NoError(t, err)

	return c.Status
}

func TestEngineActivation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		launchErr  error
		name       string
		wantBefore string
		wantAfter  string
		advance    time.Duration
	}{
		{name: "waits for the data plane past the deadline", advance: time.Minute,
			wantBefore: statusCreating, wantAfter: statusActive},
		{name: "waits for the deadline once the data plane is up", advance: 0,
			wantBefore: statusCreating, wantAfter: statusActive},
		{name: "launch failure", launchErr: errLaunch, advance: time.Minute,
			wantBefore: statusCreating, wantAfter: statusFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newFakeLauncher(tt.launchErr)
			b, clk := newEngineBackend(t, l)

			c, err := b.CreateCluster("123456789012", "us-east-1", CreateClusterInput{})
			require.NoError(t, err)
			require.Equal(t, c.Identifier, <-l.started)

			clk.advance(b, tt.advance)
			assert.Equal(t, tt.wantBefore, statusOf(t, b, c.Identifier), "data plane still launching")

			close(l.gate)

			if tt.launchErr == nil {
				waitReady(t, b, c.Identifier)
			} else {
				require.Eventually(t, func() bool { return statusOf(t, b, c.Identifier) == statusFailed },
					10*time.Second, time.Millisecond)
			}

			if tt.advance == 0 {
				assert.Equal(t, statusCreating, statusOf(t, b, c.Identifier), "deadline not reached")
				clk.advance(b, time.Minute)
			}

			assert.Equal(t, tt.wantAfter, statusOf(t, b, c.Identifier))
		})
	}
}

func TestEngineTeardown(t *testing.T) {
	t.Parallel()

	tests := []struct {
		do   func(t *testing.T, b *InMemoryBackend, id string)
		name string
	}{
		{name: "delete", do: func(t *testing.T, b *InMemoryBackend, id string) {
			t.Helper()
			_, err := b.DeleteCluster(id)
			require.NoError(t, err)
		}},
		{name: "reset", do: func(_ *testing.T, b *InMemoryBackend, _ string) { b.Reset() }},
		{name: "close", do: func(_ *testing.T, b *InMemoryBackend, _ string) { b.Close() }},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			l := newFakeLauncher(nil)
			close(l.gate)

			b, _ := newEngineBackend(t, l)
			c, err := b.CreateCluster("123456789012", "us-east-1", CreateClusterInput{})
			require.NoError(t, err)
			waitReady(t, b, c.Identifier)

			tt.do(t, b, c.Identifier)

			assert.Equal(t, c.Identifier, <-l.closed)

			_, _, ok := b.ConnectAddr(c.Identifier)
			assert.False(t, ok)
		})
	}
}

func TestEngineDeletionProtectionKeepsDataPlane(t *testing.T) {
	t.Parallel()

	l := newFakeLauncher(nil)
	close(l.gate)

	b, _ := newEngineBackend(t, l)
	c, err := b.CreateCluster("123456789012", "us-east-1", CreateClusterInput{DeletionProtectionEnabled: true})
	require.NoError(t, err)
	waitReady(t, b, c.Identifier)

	_, err = b.DeleteCluster(c.Identifier)
	require.ErrorIs(t, err, ErrDeletionProtected)

	_, _, ok := b.ConnectAddr(c.Identifier)
	assert.True(t, ok)
	assert.Empty(t, l.closed)
}

func TestEngineDeleteWhileLaunching(t *testing.T) {
	t.Parallel()

	l := newFakeLauncher(nil)
	b, _ := newEngineBackend(t, l)

	c, err := b.CreateCluster("123456789012", "us-east-1", CreateClusterInput{})
	require.NoError(t, err)
	require.Equal(t, c.Identifier, <-l.started)

	_, err = b.DeleteCluster(c.Identifier)
	require.NoError(t, err)

	close(l.gate)
	b.Close()
	assert.Empty(t, l.closed, "a cancelled launch leaves nothing to close")
}

func TestEngineRestoreRelaunches(t *testing.T) {
	t.Parallel()

	l1 := newFakeLauncher(nil)
	close(l1.gate)

	b1, clk := newEngineBackend(t, l1)
	c, err := b1.CreateCluster("123456789012", "us-east-1", CreateClusterInput{})
	require.NoError(t, err)
	waitReady(t, b1, c.Identifier)
	clk.advance(b1, time.Minute)
	require.Equal(t, statusActive, statusOf(t, b1, c.Identifier))

	snap := b1.Snapshot(t.Context())

	l2 := newFakeLauncher(nil)
	b2, clk2 := newEngineBackend(t, l2)
	require.NoError(t, b2.Restore(t.Context(), snap))

	assert.Equal(t, c.Identifier, <-l2.started)
	assert.Equal(t, statusCreating, statusOf(t, b2, c.Identifier), "restored clusters wait for a fresh data plane")

	close(l2.gate)
	waitReady(t, b2, c.Identifier)
	clk2.advance(b2, time.Second)
	assert.Equal(t, statusActive, statusOf(t, b2, c.Identifier))
}

func TestNoEngineKeepsLazyActivation(t *testing.T) {
	t.Parallel()

	b := NewInMemoryBackend()
	clk := &testClock{t: time.Date(2026, 10, 31, 12, 0, 0, 0, time.UTC)}
	clk.advance(b, 0)

	c, err := b.CreateCluster("123456789012", "us-east-1", CreateClusterInput{})
	require.NoError(t, err)
	assert.Equal(t, statusCreating, statusOf(t, b, c.Identifier))

	clk.advance(b, time.Minute)
	assert.Equal(t, statusActive, statusOf(t, b, c.Identifier))

	_, _, ok := b.ConnectAddr(c.Identifier)
	assert.False(t, ok)
}
