package app

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errKillFake = errors.New("no console")

// fgRuntime fakes a runtime whose container stays up until exited or stopped.
type fgRuntime struct {
	ready *atomic.Bool
	exit  chan int
	fakeLifecycleRuntime
	stopOnce sync.Once
	stopCode int
}

func newFGRuntime() *fgRuntime {
	return &fgRuntime{ready: &atomic.Bool{}, exit: make(chan int, 1)}
}

func (f *fgRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	f.ready.Store(true)

	return f.fakeLifecycleRuntime.CreateAndStart(ctx, spec)
}

func (f *fgRuntime) StopAndRemove(ctx context.Context, id string) error {
	if f.ready.Load() && len(f.exit) == 0 {
		f.stopOnce.Do(func() { f.exit <- f.stopCode })
	}

	return f.fakeLifecycleRuntime.StopAndRemove(ctx, id)
}

func (f *fgRuntime) ReadLogs(ctx context.Context, _, _ string, follow bool, stdout, _ io.Writer) error {
	_, _ = stdout.Write([]byte("hello from container\n"))

	if follow {
		<-ctx.Done()
	}

	return nil
}

func (f *fgRuntime) WaitExit(ctx context.Context, _ string) (int, error) {
	select {
	case code := <-f.exit:
		f.exit <- code

		return code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func readyServer(t *testing.T, ready *atomic.Bool) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if !ready.Load() {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		_ = json.NewEncoder(w).Encode(healthResponse{Status: "ok"})
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	return u.Port()
}

func TestLifecycleForeground(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		wantCode int
		cancel   bool
		exitCode int
		stopCode int
	}{
		{name: "container exits zero", exitCode: 0},
		{name: "container exit code propagates", exitCode: 3, wantCode: 3},
		{name: "signal stops gracefully", cancel: true, stopCode: 0},
		{name: "signal stop non zero", cancel: true, stopCode: 143, wantCode: 143},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := newFGRuntime()
			rt.stopCode = tt.stopCode
			d, out, dir := testLifecycleDeps(t, rt)
			port := readyServer(t, rt.ready)

			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()

			if !tt.cancel {
				rt.exit <- tt.exitCode
			}

			cmd := StartCmd{
				StateDir: dir, Tag: "latest", Bind: "127.0.0.1", Port: port,
				Timeout: lifecycleReadyTimeout, Foreground: true,
			}

			done := make(chan error, 1)
			started := make(chan struct{})
			d.Out = &syncWriter{w: out, started: started, marker: "gopherstack started"}

			go func() { done <- cmd.run(ctx, d) }()

			if tt.cancel {
				<-started
				cancel()
			}

			err := <-done
			code, isExit := lifecycleExitCode(err)

			if tt.wantCode == 0 {
				require.NoError(t, err)
			} else {
				require.True(t, isExit, "err=%v", err)
				assert.Equal(t, tt.wantCode, code)
			}

			assert.Contains(t, out.String(), "hello from container")
			_, ok := d.loadState(cmd.LifecycleTarget)
			assert.False(t, ok, "state dropped")
		})
	}
}

func TestLifecycleForegroundNeedsWaiter(t *testing.T) {
	t.Parallel()

	rt := &fakeLifecycleRuntime{}
	d, _, dir := testLifecycleDeps(t, rt)
	cmd := StartCmd{
		StateDir: dir, Tag: "latest", Bind: "127.0.0.1", Port: "1",
		Timeout: lifecycleReadyTimeout, Foreground: true,
	}

	require.ErrorIs(t, cmd.run(t.Context(), d), errLifecycleNoRuntime)
	assert.Empty(t, rt.specs)
}

func TestLifecycleStartValidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		want error
		name string
		cmd  StartCmd
	}{
		{name: "host foreground", cmd: StartCmd{Host: true, Foreground: true}, want: errLifecycleHostOnly},
		{name: "host socket", cmd: StartCmd{Host: true, DockerSocket: true}, want: errLifecycleHostOnly},
		{name: "path without flag", cmd: StartCmd{DockerSocketPath: "/x"}, want: errLifecycleSocketPath},
		{name: "ok", cmd: StartCmd{DockerSocket: true, Foreground: true}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			err := tt.cmd.validate()
			if tt.want == nil {
				require.NoError(t, err)

				return
			}

			require.ErrorIs(t, err, tt.want)
		})
	}
}

func TestLifecycleDockerSocketMount(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		dockerEnv string
		wantMount string
		cmd       StartCmd
		noSocket  bool
	}{
		{name: "default off", noSocket: true},
		{
			name: "default path", cmd: StartCmd{DockerSocket: true},
			wantMount: "/var/run/docker.sock:/var/run/docker.sock",
		},
		{
			name: "docker host env", cmd: StartCmd{DockerSocket: true}, dockerEnv: "unix:///run/user/1/docker.sock",
			wantMount: "/run/user/1/docker.sock:/var/run/docker.sock",
		},
		{
			name: "explicit path wins", cmd: StartCmd{DockerSocket: true, DockerSocketPath: "/s.sock"},
			dockerEnv: "unix:///other", wantMount: "/s.sock:/var/run/docker.sock",
		},
		{
			name: "tcp docker host ignored", cmd: StartCmd{DockerSocket: true}, dockerEnv: "tcp://h:2375",
			wantMount: "/var/run/docker.sock:/var/run/docker.sock",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeLifecycleRuntime{}
			d, out, dir := testLifecycleDeps(t, rt)
			d.Getenv = func(k string) string {
				if k == "DOCKER_HOST" {
					return tt.dockerEnv
				}

				return ""
			}

			cmd := tt.cmd
			cmd.StateDir, cmd.Tag, cmd.Bind, cmd.Timeout = dir, "latest", "127.0.0.1", lifecycleReadyTimeout
			cmd.Port = "1"

			// readiness fails on port 1; only the created spec matters here.
			cmd.Timeout = 1
			_ = cmd.run(t.Context(), d)

			require.Len(t, rt.specs, 1)

			spec := rt.specs[0]
			if tt.noSocket {
				assert.NotContains(t, strings.Join(spec.Mounts, ","), "docker.sock")
				assert.NotContains(t, strings.Join(spec.Env, ","), "DOCKER_HOST")
				assert.NotContains(t, out.String(), "warning")

				return
			}

			assert.Contains(t, spec.Mounts, tt.wantMount)
			assert.Contains(t, spec.Env, "DOCKER_HOST=unix:///var/run/docker.sock")
			assert.Contains(t, out.String(), "root-equivalent")
		})
	}
}

type fakeKiller struct {
	breakErr error
	calls    []string
}

func (f *fakeKiller) CtrlBreak(int) error {
	f.calls = append(f.calls, "break")

	return f.breakErr
}

func (f *fakeKiller) Kill(int) error {
	f.calls = append(f.calls, "kill")

	return nil
}

func TestTerminateWith(t *testing.T) {
	t.Parallel()

	tests := []struct {
		breakErr error
		name     string
		want     []string
		force    bool
	}{
		{name: "graceful", want: []string{"break"}},
		{name: "graceful falls back to kill", breakErr: errKillFake, want: []string{"break", "kill"}},
		{name: "force kills only", force: true, want: []string{"kill"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			k := &fakeKiller{breakErr: tt.breakErr}
			require.NoError(t, terminateWith(k, 42, tt.force))
			assert.Equal(t, tt.want, k.calls)
		})
	}
}

func TestLifecycleStatusServiceHealth(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/_gopherstack/health":
			_ = json.NewEncoder(w).Encode(healthResponse{
				Status: "ok", Version: "v1", UptimeSecs: 5, Goroutines: 12, HeapAllocB: 3 << 20,
				Services: []string{"S3", "SQS"}, Disabled: []string{"SNS"},
			})
		case "/_localstack/health":
			_ = json.NewEncoder(w).Encode(localstackHealthResponse{Services: map[string]string{"s3": "available"}})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	d, out, dir := testLifecycleDeps(t, nil)
	cmd := StatusCmd{StateDir: dir, Endpoint: srv.URL, JSON: true}
	require.NoError(t, cmd.run(t.Context(), d))

	var rep statusReport
	require.NoError(t, json.Unmarshal(out.Bytes(), &rep))

	got := map[string]serviceState{}
	for _, s := range rep.Clouds["aws"] {
		got[s.Name] = s
	}

	assert.Equal(t, serviceState{Name: "S3", State: "enabled", Port: u.Port(), Health: "healthy"}, got["S3"])
	assert.Equal(t, "unavailable", got["SQS"].Health)
	assert.Equal(t, serviceState{Name: "SNS", State: "disabled", Health: "disabled"}, got["SNS"])
	assert.Equal(t, 12, rep.Goroutines)
	assert.Equal(t, uint64(3<<20), rep.HeapAllocB)

	out.Reset()
	cmd.JSON = false
	require.NoError(t, cmd.run(t.Context(), d))
	assert.Regexp(t, `aws\s+S3\s+enabled\s+`+u.Port()+`\s+healthy`, out.String())
	assert.Regexp(t, `aws\s+SNS\s+disabled\s+-\s+disabled`, out.String())
	assert.Contains(t, out.String(), "3.0 MiB heap, 12 goroutines")
}

// syncWriter closes started once marker is written, serialising writes.
type syncWriter struct {
	w       io.Writer
	started chan struct{}
	marker  string
	mu      sync.Mutex
	once    sync.Once
}

func (s *syncWriter) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	n, err := s.w.Write(p)
	if strings.Contains(string(p), s.marker) {
		s.once.Do(func() { close(s.started) })
	}

	return n, err
}
