package k3sengine_test

import (
	"context"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/k3sengine"
)

type stateRuntime struct {
	*stoppableRuntime
	execFn   func(cmd []string) (container.ExecResult, error)
	volumes  []string
	removed  []string
	unused   []string
	execCmds [][]string
	execEnvs [][]string
	vmu      sync.Mutex
}

func (r *stateRuntime) CreateVolume(_ context.Context, name string, labels map[string]string) error {
	r.vmu.Lock()
	defer r.vmu.Unlock()

	r.volumes = append(r.volumes, name+"|"+labels[k3sengine.VolumeLabel])

	return nil
}

func (r *stateRuntime) RemoveVolume(_ context.Context, name string) error {
	r.vmu.Lock()
	defer r.vmu.Unlock()

	r.removed = append(r.removed, name)

	return nil
}

func (r *stateRuntime) ListUnusedVolumes(context.Context, string, time.Duration) ([]string, error) {
	return r.unused, nil
}

func (r *stateRuntime) Exec(_ context.Context, _ string, cmd []string) (container.ExecResult, error) {
	r.vmu.Lock()
	r.execCmds = append(r.execCmds, cmd)
	fn := r.execFn
	r.vmu.Unlock()

	return fn(cmd)
}

func (r *stateRuntime) ExecEnv(_ context.Context, _ string, cmd, env []string) (container.ExecResult, error) {
	r.vmu.Lock()
	r.execCmds = append(r.execCmds, cmd)
	r.execEnvs = append(r.execEnvs, env)
	fn := r.execFn
	r.vmu.Unlock()

	return fn(cmd)
}

func TestScriptKeepsExistingCA(t *testing.T) {
	t.Parallel()

	const tls = "/var/lib/rancher/k3s/server/tls"

	for _, lb := range []bool{false, true} {
		s := k3sengine.Script(lb)
		assert.Contains(t, s, "[ -s /var/lib/rancher/k3s/server/tls/server-ca.crt ]", "a CA already on the volume wins")
		assert.Contains(t, s, "rm -f "+tls+"/.gopherstack-rotated "+tls+"/dynamic-cert.json",
			"a rotated node starts without its cached serving certificate")
		assert.Contains(t, s, `${GOPHERSTACK_NODE_NAME:+ --node-name "$GOPHERSTACK_NODE_NAME"}`)
	}
}

func TestLaunchPersistentState(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantLabels map[string]string
		name       string
		wantEnv    string
		node       k3sengine.Node
		wantMounts []string
		noVolumes  bool
	}{
		{
			name:       "volume and resource",
			node:       k3sengine.Node{Name: "c1", Image: "img", Volume: "vol-1", Resource: "rg/c1", NodeName: "n-0"},
			wantMounts: []string{"vol-1:" + k3sengine.DataDir},
			wantLabels: map[string]string{container.LabelService: "k3s-test", container.LabelResource: "rg/c1"},
			wantEnv:    "GOPHERSTACK_NODE_NAME=n-0",
		},
		{
			name:       "plain node keeps the old labels",
			node:       k3sengine.Node{Name: "c1", Image: "img"},
			wantLabels: map[string]string{container.LabelService: "k3s-test"},
		},
		{
			name:       "runtime without volumes keeps state in the container",
			node:       k3sengine.Node{Name: "c1", Image: "img", Volume: "vol-1"},
			wantLabels: map[string]string{container.LabelService: "k3s-test"},
			noVolumes:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			base := &runtime{}

			var (
				e  *k3sengine.Engine
				sr *stateRuntime
			)

			if tt.noVolumes {
				e, _ = newEngine(t, base, nil)
			} else {
				sr = &stateRuntime{stoppableRuntime: &stoppableRuntime{runtime: base}}
				e, _ = newEngine(t, nil, func(c *k3sengine.Config) { c.Runtime = sr })
			}

			_, err := e.Launch(t.Context(), tt.node, nil)
			require.NoError(t, err)
			require.Len(t, base.specs, 1)

			spec := base.specs[0]
			assert.Equal(t, tt.wantMounts, spec.Mounts)

			for k, v := range tt.wantLabels {
				assert.Equal(t, v, spec.Labels[k])
			}

			if tt.wantEnv != "" {
				assert.Contains(t, spec.Env, tt.wantEnv)
			}

			if sr != nil && tt.node.Volume != "" {
				assert.Equal(t, []string{"vol-1|test"}, sr.volumes)
			}
		})
	}
}

func TestEngineVolumes(t *testing.T) {
	t.Parallel()

	sr := &stateRuntime{stoppableRuntime: &stoppableRuntime{runtime: &runtime{}}, unused: []string{"a", "b"}}
	e, _ := newEngine(t, nil, func(c *k3sengine.Config) { c.Runtime = sr })

	require.NoError(t, e.RemoveVolume(t.Context(), "vol-1"))
	require.NoError(t, e.RemoveVolume(t.Context(), ""))
	assert.Equal(t, []string{"vol-1"}, sr.removed)

	names, err := e.UnusedVolumes(t.Context())
	require.NoError(t, err)
	assert.Equal(t, []string{"a", "b"}, names)

	plain, _ := newEngine(t, &runtime{}, nil)
	require.NoError(t, plain.RemoveVolume(t.Context(), "vol-1"))

	names, err = plain.UnusedVolumes(t.Context())
	require.NoError(t, err)
	assert.Empty(t, names)
}

func TestAdopt(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ports    map[int]int
		name     string
		wantPort int
		wantErr  bool
	}{
		{name: "running node", ports: map[int]int{k3sengine.APIPort: 42010}, wantPort: 42010},
		{name: "no api port published", ports: map[int]int{80: 42011}, wantErr: true},
		{name: "port outside the pool", ports: map[int]int{k3sengine.APIPort: 50000}, wantPort: 50000},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			e, ports := newEngine(t, &runtime{}, nil)

			inst, err := e.Adopt(container.ManagedContainer{ID: "old", Ports: tt.ports})
			if tt.wantErr {
				require.ErrorIs(t, err, k3sengine.ErrNotAdoptable)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "old", inst.ContainerID)
			assert.Equal(t, tt.wantPort, inst.Port)
			assert.Contains(t, ports.Reserved(), tt.wantPort, "the adopted port must not be handed out again")
		})
	}

	e, _ := newEngine(t, &runtime{}, nil)
	_, err := e.Adopt(container.ManagedContainer{ID: "a", Ports: map[int]int{k3sengine.APIPort: 42020}})
	require.NoError(t, err)

	_, err = e.Adopt(container.ManagedContainer{ID: "b", Ports: map[int]int{k3sengine.APIPort: 42020}})
	require.ErrorIs(t, err, k3sengine.ErrNotAdoptable, "a port already in use cannot be adopted twice")
}

func TestServingCAAndExec(t *testing.T) {
	t.Parallel()

	t.Run("reads the CA once the node wrote it", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			calls := 0
			sr := &stateRuntime{stoppableRuntime: &stoppableRuntime{runtime: &runtime{}}}
			sr.execFn = func([]string) (container.ExecResult, error) {
				calls++
				if calls < 3 {
					return container.ExecResult{ExitCode: 1}, nil
				}

				return container.ExecResult{Stdout: "ca-pem"}, nil
			}

			e, _ := newEngine(t, nil, func(c *k3sengine.Config) { c.Runtime = sr })
			ca, err := e.ServingCA(t.Context(), "c1")
			require.NoError(t, err)
			assert.Equal(t, "ca-pem", string(ca))
			assert.Equal(t, 3, calls)
			assert.Equal(t, []string{"cat", k3sengine.CACertFile}, sr.execCmds[0])
		})
	})

	t.Run("runtime without exec", func(t *testing.T) {
		t.Parallel()

		e, _ := newEngine(t, &runtime{}, nil)

		_, err := e.ServingCA(t.Context(), "c1")
		require.ErrorIs(t, err, container.ErrExecUnsupported)

		_, err = e.Exec(t.Context(), "c1", []string{"ls"}, nil)
		require.ErrorIs(t, err, container.ErrExecUnsupported)
	})
}

func TestRotateCertificates(t *testing.T) {
	t.Parallel()

	tests := []struct {
		wantErr   error
		name      string
		wantCalls []string
		exit      int
	}{
		{name: "rotated and restarted", wantCalls: []string{"stop:c1", "start:c1"}},
		{name: "rotate refused leaves the node running", exit: 1, wantErr: k3sengine.ErrRotateFailed},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sr := &stateRuntime{stoppableRuntime: &stoppableRuntime{runtime: &runtime{}}}
			sr.execFn = func([]string) (container.ExecResult, error) {
				return container.ExecResult{ExitCode: tt.exit, Stderr: "no"}, nil
			}

			e, _ := newEngine(t, nil, func(c *k3sengine.Config) {
				c.Runtime = sr
				c.StartTimeout = time.Minute
				c.Probe = func(context.Context, string, string) ([]byte, error) { return []byte("served-ca"), nil }
			})

			ca, err := e.RotateCertificates(t.Context(), "c1", "127.0.0.1:1")
			require.Len(t, sr.execCmds[0], 3)
			assert.Equal(t, "/bin/sh", sr.execCmds[0][0])
			assert.Contains(t, sr.execCmds[0][2], "/bin/k3s certificate rotate-ca --path=")
			assert.NotContains(t, sr.execCmds[0][2], "PRIVATE KEY", "key material must stay out of argv")
			assert.Contains(t, sr.execCmds[0][2], "touch /var/lib/rancher/k3s/server/tls/.gopherstack-rotated",
				"the next start must drop the cached serving certificate")
			require.Len(t, sr.execEnvs, 1)
			assert.Len(t, sr.execEnvs[0], 10, "a cert and key for each of the five CAs travel in the environment")
			assert.Equal(t, tt.wantCalls, sr.calls)

			if tt.wantErr != nil {
				require.ErrorIs(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, "served-ca", string(ca))
		})
	}
}
