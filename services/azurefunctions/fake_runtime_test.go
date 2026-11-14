package azurefunctions_test

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
)

type hostRequest struct {
	Headers http.Header
	Host    string
	Path    string
	Query   string
	Body    string
}

type fakeContainer struct {
	srv  *http.Server
	spec container.Spec
}

// fakeRuntime stands in for the container runtime: every container serves HTTP on its published port and
// echoes the request it received.
type fakeRuntime struct {
	boxes map[string]*fakeContainer
	// onExec, when set, handles Exec on a container started without ports (a build container).
	onExec  func(spec container.Spec, cmd []string) container.ExecResult
	started []container.Spec
	reqs    []hostRequest
	removed []string
	execs   [][]string
	pulls   []string
	seq     atomic.Int64
	// unhealthy is how many health-check probes still get a 503.
	unhealthy atomic.Int64
	mu        sync.Mutex
}

func newFakeRuntime() *fakeRuntime { return &fakeRuntime{boxes: map[string]*fakeContainer{}} }

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	if len(spec.Ports) == 0 {
		id := fmt.Sprintf("fake-%d", f.seq.Add(1))

		f.mu.Lock()
		f.boxes[id] = &fakeContainer{spec: spec}
		f.started = append(f.started, spec)
		f.mu.Unlock()

		return id, nil
	}

	_, host, _, err := container.ParsePortSpec(spec.Ports[0])
	if err != nil {
		return "", err
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", net.JoinHostPort("127.0.0.1", host))
	if err != nil {
		return "", err
	}

	c := &fakeContainer{spec: spec}
	c.srv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() == gcpserve.ProbeUserAgent {
			return
		}

		if r.UserAgent() == "HealthCheck/1.0" && f.unhealthy.Add(-1) >= 0 {
			w.WriteHeader(http.StatusServiceUnavailable)

			return
		}

		body, _ := io.ReadAll(r.Body)

		f.mu.Lock()
		f.reqs = append(f.reqs, hostRequest{
			Headers: r.Header.Clone(), Host: r.Host, Path: r.URL.Path, Query: r.URL.RawQuery, Body: string(body),
		})
		f.mu.Unlock()

		if r.URL.Path == "/admin/functions" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = w.Write([]byte(`[{"name":"hello","language":"node","isDisabled":false,` +
				`"config":{"bindings":[{"type":"httpTrigger","authLevel":"anonymous","direction":"in","name":"req"}]}},` +
				`{"name":"tick","language":"node","isDisabled":true,"config":{"bindings":[{"type":"timerTrigger"}]}}]`))

			return
		}

		_, _ = fmt.Fprintf(w, "echo %s", r.URL.Path)
	})}

	go func() { _ = c.srv.Serve(l) }()

	id := fmt.Sprintf("fake-%d", f.seq.Add(1))

	f.mu.Lock()
	f.boxes[id] = c
	f.started = append(f.started, spec)
	f.mu.Unlock()

	return id, nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	c := f.boxes[id]
	delete(f.boxes, id)
	f.removed = append(f.removed, id)
	f.mu.Unlock()

	if c != nil && c.srv != nil {
		_ = c.srv.Close()
	}

	return nil
}

func (f *fakeRuntime) Exec(_ context.Context, id string, cmd []string) (container.ExecResult, error) {
	f.mu.Lock()
	c := f.boxes[id]
	f.execs = append(f.execs, cmd)
	hook := f.onExec
	f.mu.Unlock()

	if c == nil || hook == nil {
		return container.ExecResult{}, container.ErrContainerNotFound
	}

	return hook(c.spec, cmd), nil
}

func (f *fakeRuntime) ReadLogs(ctx context.Context, _, _ string, follow bool, stdout, _ io.Writer) error {
	_, _ = io.WriteString(stdout, "host log line\n")

	if follow {
		<-ctx.Done()
	}

	return nil
}

func (f *fakeRuntime) PullImageWithCredentials(_ context.Context, image, user, pass string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pulls = append(f.pulls, image+"|"+user+"|"+pass)

	return nil
}

func (f *fakeRuntime) pulled() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.pulls...)
}

// ExecStream writes the hook's output, then keeps running like a long-lived process until ctx ends.
func (f *fakeRuntime) ExecStream(ctx context.Context, id string, cmd []string, out io.Writer) (int, error) {
	f.mu.Lock()
	c := f.boxes[id]
	f.execs = append(f.execs, cmd)
	hook := f.onExec
	f.mu.Unlock()

	if c != nil && hook != nil {
		_, _ = io.WriteString(out, hook(c.spec, cmd).Stdout)
	}

	<-ctx.Done()

	return 0, nil
}

func (f *fakeRuntime) running() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.boxes)
}

func (f *fakeRuntime) specs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.started...)
}

func (f *fakeRuntime) requests() []hostRequest {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]hostRequest(nil), f.reqs...)
}

func envOf(spec container.Spec) map[string]string {
	m := map[string]string{}

	for _, e := range spec.Env {
		k, v, _ := strings.Cut(e, "=")
		m[k] = v
	}

	return m
}

// mountSource returns the host directory bind-mounted at target.
func mountSource(spec container.Spec, target string) string {
	for _, m := range spec.Mounts {
		parts := strings.Split(m, ":")
		if len(parts) >= 2 && parts[1] == target {
			return parts[0]
		}
	}

	return ""
}

func readFile(path string) (string, error) {
	raw, err := os.ReadFile(filepath.Clean(path))

	return string(raw), err
}
