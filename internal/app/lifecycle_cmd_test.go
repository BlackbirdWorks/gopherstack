package app

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/alecthomas/kong"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
)

var errLifecycleFake = errors.New("boom")

type fakeLifecycleRuntime struct {
	stopErr  error
	specs    []container.Spec
	stopped  []string
	logsArgs []string
	logText  string
	mu       sync.Mutex
}

func (f *fakeLifecycleRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.specs = append(f.specs, spec)

	return "abcdef0123456789", nil
}

func (f *fakeLifecycleRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.stopped = append(f.stopped, id)

	return f.stopErr
}

func (f *fakeLifecycleRuntime) ReadLogs(
	_ context.Context, id, tail string, follow bool, stdout, _ io.Writer,
) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.logsArgs = []string{id, tail, strconv.FormatBool(follow)}
	_, err := stdout.Write([]byte(f.logText))

	return err
}

func testLifecycleDeps(t *testing.T, rt lifecycleRuntime) (*lifecycleDeps, *bytes.Buffer, string) {
	t.Helper()

	out := &bytes.Buffer{}
	dir := t.TempDir()
	d := &lifecycleDeps{
		Out:  out,
		Err:  out,
		HTTP: httpclient.New(5 * time.Second),
		Runtime: func() (lifecycleRuntime, error) {
			if rt == nil {
				return nil, container.ErrUnavailable
			}

			return rt, nil
		},
		Getenv:     func(k string) string { return map[string]string{"FROM_SHELL": "yes"}[k] },
		Home:       func() (string, error) { return dir, nil },
		NewContext: func() (context.Context, context.CancelFunc) { return context.WithCancel(t.Context()) },
	}

	return d, out, dir
}

func healthServer(t *testing.T, code int, h healthResponse) (*httptest.Server, string) {
	t.Helper()

	var mu sync.Mutex

	var posts []string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/_gopherstack/health":
			w.WriteHeader(code)
			_ = json.NewEncoder(w).Encode(h)
		case r.URL.Path == "/_gopherstack/reset" && r.Method == http.MethodPost:
			mu.Lock()
			posts = append(posts, r.URL.RawQuery)
			mu.Unlock()
			_, _ = w.Write([]byte(`{"status":"ok","message":"reset 3 service(s)"}`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(srv.Close)

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	return srv, u.Port()
}

func TestLifecycleParse(t *testing.T) {
	t.Parallel()

	tests := []struct {
		check func(t *testing.T, r *rootCLI)
		name  string
		cmd   string
		args  []string
	}{
		{
			name: "start defaults", cmd: "start", args: []string{"start"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.Equal(t, "ghcr.io/blackbirdworks/gopherstack", r.Start.Image)
				assert.Equal(t, "latest", r.Start.Tag)
				assert.Equal(t, "127.0.0.1", r.Start.Bind)
				assert.False(t, r.Start.Host)
			},
		},
		{
			name: "start flags", cmd: "start",
			args: []string{"start", "--host", "--persist", "-e", "A=b", "-e", "C", "-p", "9000", "--tag", "v1", "--port", "9001"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.True(t, r.Start.Host)
				assert.True(t, r.Start.Persist)
				assert.Equal(t, []string{"A=b", "C"}, r.Start.Env)
				assert.Equal(t, []string{"9000"}, r.Start.Publish)
				assert.Equal(t, "v1", r.Start.Tag)
				assert.Equal(t, "9001", r.Start.Port)
			},
		},
		{
			name: "reset services", cmd: "reset",
			args: []string{"reset", "--service", "S3", "--service", "SQS"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.Equal(t, []string{"S3", "SQS"}, r.Reset.Services)
			},
		},
		{
			name: "snapshot save", cmd: "snapshot save <name>", args: []string{"snapshot", "save", "x", "-m", "msg"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.Equal(t, "x", r.Snapshot.Save.Name)
				assert.Equal(t, "msg", r.Snapshot.Save.Message)
			},
		},
		{
			name: "snapshot load", cmd: "snapshot load <name>", args: []string{"snapshot", "load", "x"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.Equal(t, "latest", r.Snapshot.Load.Version)
			},
		},
		{name: "snapshot list", cmd: "snapshot list", args: []string{"snapshot", "list"}, check: func(*testing.T, *rootCLI) {}},
		{
			name: "logs", cmd: "logs", args: []string{"logs", "-f", "-n", "5"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.True(t, r.Logs.Follow)
				assert.Equal(t, 5, r.Logs.Tail)
			},
		},
		{
			name: "status json", cmd: "status", args: []string{"status", "--json", "--endpoint", "http://x:1"},
			check: func(t *testing.T, r *rootCLI) {
				t.Helper()
				assert.True(t, r.Status.JSON)
				assert.Equal(t, "http://x:1", r.Status.Endpoint)
			},
		},
		{name: "stop", cmd: "stop", args: []string{"stop", "--name", "n"}, check: func(t *testing.T, r *rootCLI) {
			t.Helper()
			assert.Equal(t, "n", r.Stop.Name)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var root rootCLI

			parser, err := kong.New(&root, kong.Name("gopherstack"), kong.Writers(nil, nil))
			require.NoError(t, err)

			kctx, err := parser.Parse(tt.args)
			require.NoError(t, err)
			assert.Equal(t, tt.cmd, kctx.Command())
			tt.check(t, &root)
		})
	}
}

func TestLifecycleStartAlreadyRunning(t *testing.T) {
	t.Parallel()

	rt := &fakeLifecycleRuntime{}
	d, out, dir := testLifecycleDeps(t, rt)
	_, port := healthServer(t, http.StatusOK, healthResponse{Status: "ok"})
	cmd := StartCmd{Bind: "127.0.0.1", Port: port, Timeout: time.Second, LifecycleTarget: LifecycleTarget{StateDir: dir}}

	require.NoError(t, cmd.run(t.Context(), d))
	assert.Contains(t, out.String(), "already running")
	assert.Empty(t, rt.specs)
}

func TestLifecycleStartCreatesContainer(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		cmd       StartCmd
		wantEnv   []string
		wantPorts []string
		mount     bool
	}{
		{
			name:      "plain",
			cmd:       StartCmd{Tag: "latest"},
			wantEnv:   []string{"PORT=8000"},
			wantPorts: []string{"127.0.0.1:%s:8000"},
		},
		{
			name:      "persist env publish",
			cmd:       StartCmd{Tag: "v2", Persist: true, Env: []string{"A=b", "FROM_SHELL", "MISSING"}, Publish: []string{"9100", "9200:9300"}},
			wantEnv:   []string{"PORT=8000", "A=b", "FROM_SHELL=yes", "PERSIST=true", "GOPHERSTACK_DATA_DIR=/data"},
			wantPorts: []string{"127.0.0.1:%s:8000", "127.0.0.1:9100:9100", "127.0.0.1:9200:9300"},
			mount:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeLifecycleRuntime{}
			d, out, dir := testLifecycleDeps(t, rt)

			var healthy bool

			var mu sync.Mutex

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				mu.Lock()
				defer mu.Unlock()

				if !healthy {
					healthy = true
					w.WriteHeader(http.StatusServiceUnavailable)

					return
				}

				_, _ = w.Write([]byte(`{"status":"ok"}`))
			}))
			defer srv.Close()

			u, err := url.Parse(srv.URL)
			require.NoError(t, err)

			cmd := tt.cmd
			cmd.Image = "img"
			cmd.Bind = "127.0.0.1"
			cmd.Port = u.Port()
			cmd.Timeout = 10 * time.Second
			cmd.StateDir = dir
			cmd.Name = "n1"

			// First health probe (already-running check) gets 503; the readiness loop then succeeds.
			require.NoError(t, cmd.run(t.Context(), d))
			require.Len(t, rt.specs, 1)

			spec := rt.specs[0]
			assert.Equal(t, "img:"+cmd.Tag, spec.Image)
			assert.Equal(t, "n1", spec.Name)
			assert.ElementsMatch(t, tt.wantEnv, spec.Env)

			wantPorts := make([]string, len(tt.wantPorts))
			for i, p := range tt.wantPorts {
				wantPorts[i] = strings.ReplaceAll(p, "%s", u.Port())
			}

			assert.Equal(t, wantPorts, spec.Ports)

			if tt.mount {
				require.Len(t, spec.Mounts, 1)
				assert.True(t, strings.HasSuffix(spec.Mounts[0], ":/data"))
				assert.True(t, strings.HasPrefix(spec.Mounts[0], filepath.Join(dir, "data-n1")))
			} else {
				assert.Empty(t, spec.Mounts)
			}

			assert.Contains(t, out.String(), "started")

			st, ok := d.loadState(cmd.LifecycleTarget)
			require.True(t, ok)
			assert.Equal(t, lifecycleModeContainer, st.Mode)
		})
	}
}

func TestLifecycleStartFailureTailsLogsAndCleansUp(t *testing.T) {
	t.Parallel()

	rt := &fakeLifecycleRuntime{logText: "panic: bad config\n"}
	d, _, dir := testLifecycleDeps(t, rt)
	_, port := healthServer(t, http.StatusServiceUnavailable, healthResponse{})

	cmd := StartCmd{
		Image: "img", Tag: "t", Bind: "127.0.0.1", Port: port, Timeout: 50 * time.Millisecond,
		LifecycleTarget: LifecycleTarget{Name: "x", StateDir: dir},
	}

	err := cmd.run(t.Context(), d)
	require.ErrorIs(t, err, errLifecycleStartFail)
	assert.Contains(t, err.Error(), "panic: bad config")
	assert.Equal(t, "abcdef0123456789", rt.stopped[len(rt.stopped)-1])

	_, ok := d.loadState(cmd.LifecycleTarget)
	assert.False(t, ok)
}

func TestLifecycleStartBadEnv(t *testing.T) {
	t.Parallel()

	for _, bad := range []string{"", "=x"} {
		t.Run("env_"+bad, func(t *testing.T) {
			t.Parallel()

			rt := &fakeLifecycleRuntime{}
			d, _, dir := testLifecycleDeps(t, rt)
			cmd := StartCmd{Env: []string{bad}, Bind: "127.0.0.1", Port: "1", Timeout: time.Second, LifecycleTarget: LifecycleTarget{StateDir: dir}}

			require.ErrorIs(t, cmd.run(t.Context(), d), errLifecycleBadEnv)
			assert.Empty(t, rt.specs)
		})
	}
}

func TestLifecycleStartHostProcessExits(t *testing.T) {
	t.Parallel()

	d, _, dir := testLifecycleDeps(t, nil)

	var killed bool

	d.Spawn = func(s hostSpawn) (int, error) {
		return 4242, os.WriteFile(s.LogFile, []byte("line1\nfatal: nope\n"), 0o600)
	}
	d.Alive = func(int) bool { return false }
	d.Terminate = func(int, bool) error { killed = true; return nil }

	_, port := healthServer(t, http.StatusServiceUnavailable, healthResponse{})
	cmd := StartCmd{
		Host: true, Bind: "127.0.0.1", Port: port, Timeout: 5 * time.Second,
		LifecycleTarget: LifecycleTarget{Name: "h", StateDir: dir},
	}

	err := cmd.run(t.Context(), d)
	require.ErrorIs(t, err, errLifecycleStartFail)
	assert.Contains(t, err.Error(), "fatal: nope")
	assert.True(t, killed)

	_, ok := d.loadState(cmd.LifecycleTarget)
	assert.False(t, ok)
}

func TestLifecycleStartHostSpawnsProcess(t *testing.T) {
	t.Parallel()

	d, out, dir := testLifecycleDeps(t, nil)

	var spawned hostSpawn

	var mu sync.Mutex

	first := true
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		mu.Lock()
		defer mu.Unlock()

		if first {
			first = false
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer srv.Close()

	u, err := url.Parse(srv.URL)
	require.NoError(t, err)

	d.Spawn = func(s hostSpawn) (int, error) { spawned = s; return 4242, nil }
	d.Alive = func(int) bool { return true }

	cmd := StartCmd{
		Host: true, Persist: true, Env: []string{"A=b"}, Bind: "127.0.0.1", Port: u.Port(), Timeout: 10 * time.Second,
		LifecycleTarget: LifecycleTarget{Name: "h", StateDir: dir},
	}

	require.NoError(t, cmd.run(t.Context(), d))
	assert.Contains(t, spawned.Env, "PORT="+u.Port())
	assert.Contains(t, spawned.Env, "GOPHERSTACK_HOST=127.0.0.1")
	assert.Contains(t, spawned.Env, "PERSIST=true")
	assert.Contains(t, spawned.Env, "A=b")
	assert.Contains(t, out.String(), "pid 4242")

	st, ok := d.loadState(cmd.LifecycleTarget)
	require.True(t, ok)
	assert.Equal(t, 4242, st.PID)
}

func TestLifecycleStop(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stopErr    error
		name       string
		wantOut    string
		wantStops  int
		noRuntime  bool
		wantErr    bool
		withState  bool
		wantTarget string
	}{
		{name: "stops container by name", wantStops: 1, wantOut: "stopped", wantTarget: "gopherstack"},
		{name: "stops by recorded id", withState: true, wantStops: 1, wantOut: "stopped", wantTarget: "abcdef0123456789"},
		{name: "already gone is idempotent", stopErr: errors.New("No such container: gopherstack"), wantStops: 1, wantOut: "not running"},
		{name: "no runtime and no state", noRuntime: true, wantOut: "not running"},
		{name: "other error", stopErr: errLifecycleFake, wantStops: 1, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rt := &fakeLifecycleRuntime{stopErr: tt.stopErr}

			var rtArg lifecycleRuntime = rt
			if tt.noRuntime {
				rtArg = nil
			}

			d, out, dir := testLifecycleDeps(t, rtArg)
			target := LifecycleTarget{Name: "gopherstack", StateDir: dir}

			if tt.withState {
				require.NoError(t, d.saveState(target, lifecycleState{Mode: lifecycleModeContainer, ContainerID: "abcdef0123456789"}))
			}

			err := (&StopCmd{LifecycleTarget: target}).run(t.Context(), d)
			if tt.wantErr {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Contains(t, out.String(), tt.wantOut)
			}

			assert.Len(t, rt.stopped, tt.wantStops)

			if tt.wantTarget != "" {
				assert.Equal(t, tt.wantTarget, rt.stopped[0])
			}

			_, ok := d.loadState(target)
			assert.False(t, ok)
		})
	}
}

func TestLifecycleStopHost(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		aliveSeq   []bool
		wantOut    string
		wantForce  bool
		wantSignal bool
	}{
		{name: "dead pid", aliveSeq: []bool{false}, wantOut: "not running"},
		{name: "graceful", aliveSeq: []bool{true, false}, wantOut: "stopped (pid 7)", wantSignal: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			d, out, dir := testLifecycleDeps(t, nil)
			target := LifecycleTarget{Name: "h", StateDir: dir}
			require.NoError(t, d.saveState(target, lifecycleState{Mode: lifecycleModeHost, PID: 7}))

			var i int

			var signalled, forced bool

			d.Alive = func(int) bool {
				v := tt.aliveSeq[min(i, len(tt.aliveSeq)-1)]
				i++

				return v
			}
			d.Terminate = func(_ int, force bool) error { signalled = true; forced = force; return nil }

			require.NoError(t, (&StopCmd{LifecycleTarget: target}).run(t.Context(), d))
			assert.Contains(t, out.String(), tt.wantOut)
			assert.Equal(t, tt.wantSignal, signalled)
			assert.Equal(t, tt.wantForce, forced)
		})
	}
}

func TestLifecycleStatus(t *testing.T) {
	t.Parallel()

	h := healthResponse{
		Status: "ok", Version: "v9", UptimeSecs: 125,
		Services: []string{"S3", "AzureBlob", "GCPFunctions", "DynamoDB"}, Disabled: []string{"SQS"},
	}

	t.Run("table", func(t *testing.T) {
		t.Parallel()

		srv, _ := healthServer(t, http.StatusOK, h)
		d, out, dir := testLifecycleDeps(t, nil)
		cmd := StatusCmd{LifecycleTarget: LifecycleTarget{StateDir: dir}, PodTarget: PodTarget{Endpoint: srv.URL}}

		require.NoError(t, cmd.run(t.Context(), d))

		s := out.String()
		assert.Contains(t, s, "Version:  v9")
		assert.Contains(t, s, "Uptime:   2m5s")
		assert.Regexp(t, `aws\s+DynamoDB\s+enabled`, s)
		assert.Regexp(t, `aws\s+SQS\s+disabled`, s)
		assert.Regexp(t, `azure\s+AzureBlob\s+enabled`, s)
		assert.Regexp(t, `gcp\s+GCPFunctions\s+enabled`, s)
	})

	t.Run("json", func(t *testing.T) {
		t.Parallel()

		srv, _ := healthServer(t, http.StatusOK, h)
		d, out, dir := testLifecycleDeps(t, nil)
		cmd := StatusCmd{LifecycleTarget: LifecycleTarget{StateDir: dir}, PodTarget: PodTarget{Endpoint: srv.URL}, JSON: true}

		require.NoError(t, cmd.run(t.Context(), d))

		var rep statusReport
		require.NoError(t, json.Unmarshal(out.Bytes(), &rep))
		assert.True(t, rep.Running)
		assert.Equal(t, 4, rep.EnabledCount)
		assert.Equal(t, 1, rep.DisabledCount)
		assert.Len(t, rep.Clouds["aws"], 3)
		assert.Len(t, rep.Clouds["azure"], 1)
		assert.Equal(t, int64(125), rep.UptimeSecs)
	})

	t.Run("uses managed endpoint", func(t *testing.T) {
		t.Parallel()

		srv, _ := healthServer(t, http.StatusOK, h)
		d, out, dir := testLifecycleDeps(t, nil)
		target := LifecycleTarget{StateDir: dir}
		require.NoError(t, d.saveState(target, lifecycleState{Mode: lifecycleModeContainer, Name: "gopherstack", Endpoint: srv.URL}))

		cmd := StatusCmd{LifecycleTarget: target, PodTarget: PodTarget{Endpoint: podDefaultEndpoint}}
		require.NoError(t, cmd.run(t.Context(), d))
		assert.Contains(t, out.String(), "Managed:  container gopherstack")
	})

	for _, asJSON := range []bool{false, true} {
		t.Run("not running json="+strconv.FormatBool(asJSON), func(t *testing.T) {
			t.Parallel()

			srv, _ := healthServer(t, http.StatusOK, h)
			endpoint := srv.URL
			srv.Close()

			d, out, dir := testLifecycleDeps(t, nil)
			cmd := StatusCmd{LifecycleTarget: LifecycleTarget{StateDir: dir}, PodTarget: PodTarget{Endpoint: endpoint}, JSON: asJSON}

			require.ErrorIs(t, cmd.run(t.Context(), d), errLifecycleNotRunning)
			assert.NotEmpty(t, out.String())
		})
	}
}

func TestLifecycleReset(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		services []string
		wantHits int
	}{
		{name: "all", wantHits: 1},
		{name: "per service", services: []string{"S3", "SQS"}, wantHits: 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var mu sync.Mutex

			var queries []string

			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal(t, http.MethodPost, r.Method)
				assert.Equal(t, "/_gopherstack/reset", r.URL.Path)
				mu.Lock()
				queries = append(queries, r.URL.RawQuery)
				mu.Unlock()
				_, _ = w.Write([]byte(`{"message":"reset 1 service(s)"}`))
			}))
			defer srv.Close()

			d, out, _ := testLifecycleDeps(t, nil)
			cmd := ResetCmd{PodTarget: PodTarget{Endpoint: srv.URL}, Services: tt.services}

			require.NoError(t, cmd.run(t.Context(), d))
			assert.Len(t, queries, tt.wantHits)
			assert.Equal(t, tt.wantHits, strings.Count(out.String(), "reset 1 service(s)"))

			for i, s := range tt.services {
				assert.Equal(t, "service="+s, queries[i])
			}
		})
	}

	t.Run("server error", func(t *testing.T) {
		t.Parallel()

		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			http.Error(w, "nope", http.StatusInternalServerError)
		}))
		defer srv.Close()

		d, _, _ := testLifecycleDeps(t, nil)
		err := (&ResetCmd{PodTarget: PodTarget{Endpoint: srv.URL}}).run(t.Context(), d)
		require.ErrorIs(t, err, errLifecycleRequest)
	})
}

func TestLifecycleLogs(t *testing.T) {
	t.Parallel()

	t.Run("container", func(t *testing.T) {
		t.Parallel()

		rt := &fakeLifecycleRuntime{logText: "hello\n"}
		d, out, dir := testLifecycleDeps(t, rt)
		cmd := LogsCmd{LifecycleTarget: LifecycleTarget{Name: "c", StateDir: dir}, Tail: 7, Follow: true}

		require.NoError(t, cmd.run(t.Context(), d))
		assert.Equal(t, []string{"c", "7", "true"}, rt.logsArgs)
		assert.Equal(t, "hello\n", out.String())
	})

	t.Run("container gone", func(t *testing.T) {
		t.Parallel()

		d, _, dir := testLifecycleDeps(t, &errLogsRuntime{})
		cmd := LogsCmd{LifecycleTarget: LifecycleTarget{StateDir: dir}, Tail: 7}
		require.ErrorIs(t, cmd.run(t.Context(), d), errLifecycleNotRunning)
	})

	t.Run("host tail", func(t *testing.T) {
		t.Parallel()

		d, out, dir := testLifecycleDeps(t, nil)
		logf := filepath.Join(dir, "h.log")
		require.NoError(t, os.WriteFile(logf, []byte("a\nb\nc\nd\n"), 0o600))

		target := LifecycleTarget{Name: "h", StateDir: dir}
		require.NoError(t, d.saveState(target, lifecycleState{Mode: lifecycleModeHost, LogFile: logf}))

		require.NoError(t, (&LogsCmd{LifecycleTarget: target, Tail: 2}).run(t.Context(), d))
		assert.Equal(t, "c\nd\n", out.String())
	})
}

type errLogsRuntime struct{ fakeLifecycleRuntime }

func (*errLogsRuntime) ReadLogs(context.Context, string, string, bool, io.Writer, io.Writer) error {
	return errors.New("No such container: x")
}
