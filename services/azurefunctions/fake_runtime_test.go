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
	boxes   map[string]*fakeContainer
	started []container.Spec
	reqs    []hostRequest
	removed []string
	seq     atomic.Int64
	mu      sync.Mutex
}

func newFakeRuntime() *fakeRuntime { return &fakeRuntime{boxes: map[string]*fakeContainer{}} }

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
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

	if c != nil {
		_ = c.srv.Close()
	}

	return nil
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
