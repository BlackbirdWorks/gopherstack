package k3sengine_test

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

type stoppableRuntime struct {
	*runtime
	startErr error
	calls    []string
	mu       sync.Mutex
}

func (r *stoppableRuntime) StopContainer(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, "stop:"+id)

	return nil
}

func (r *stoppableRuntime) StartContainer(_ context.Context, id string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.calls = append(r.calls, "start:"+id)

	return r.startErr
}

func TestEngineStopStart(t *testing.T) {
	t.Parallel()

	tests := []struct {
		startErr  error
		wantErr   error
		name      string
		wantCalls []string
		stoppable bool
	}{
		{name: "kept and restarted", stoppable: true, wantCalls: []string{"stop:c1", "start:c1"}},
		{name: "start failure", stoppable: true, startErr: errBoom, wantErr: errBoom,
			wantCalls: []string{"stop:c1", "start:c1"}},
		{name: "runtime cannot keep containers", wantErr: k3sengine.ErrStopUnsupported},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := &runtime{}
			sr := &stoppableRuntime{runtime: base, startErr: tt.startErr}

			cfg := func(c *k3sengine.Config) {
				c.StartTimeout = time.Minute
				c.Probe = func(context.Context, string, string) ([]byte, error) { return []byte("served-ca"), nil }
			}

			var e *k3sengine.Engine
			if tt.stoppable {
				e, _ = newEngine(t, nil, func(c *k3sengine.Config) { cfg(c); c.Runtime = sr })
			} else {
				e, _ = newEngine(t, base, cfg)
			}

			err := e.Stop(t.Context(), "c1")
			if !tt.stoppable {
				require.ErrorIs(t, err, tt.wantErr)

				_, err = e.Start(t.Context(), "c1", "127.0.0.1:1", []byte("ca"))
				require.ErrorIs(t, err, tt.wantErr)
				assert.Empty(t, base.removed)

				return
			}

			require.NoError(t, err)

			ca, err := e.Start(t.Context(), "c1", "127.0.0.1:1", []byte("ca"))
			assert.Equal(t, tt.wantCalls, sr.calls)
			assert.Empty(t, base.removed, "stop must not remove the container")

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "served-ca", string(ca))
		})
	}
}

type exitedRuntime struct {
	*stoppableRuntime
}

func (r *exitedRuntime) WaitExit(context.Context, string) (int, error) { return 7, nil }

func (r *exitedRuntime) ReadLogs(_ context.Context, _, tail string, _ bool, stdout, _ io.Writer) error {
	_, err := io.WriteString(stdout, "boom from k3s tail="+tail)

	return err
}

func TestEngineStartFailsFastWhenNodeExits(t *testing.T) {
	t.Parallel()

	rt := &exitedRuntime{stoppableRuntime: &stoppableRuntime{runtime: &runtime{}}}

	e, _ := newEngine(t, nil, func(c *k3sengine.Config) {
		c.Runtime = rt
		c.StartTimeout = time.Hour
		c.Probe = func(ctx context.Context, _, _ string) ([]byte, error) {
			<-ctx.Done()

			return nil, ctx.Err()
		}
	})

	_, err := e.Start(t.Context(), "c1", "127.0.0.1:1", []byte("ca"))
	require.ErrorIs(t, err, k3sengine.ErrNodeExited)
	assert.Contains(t, err.Error(), "code 7")
	assert.Contains(t, err.Error(), "boom from k3s")
}
