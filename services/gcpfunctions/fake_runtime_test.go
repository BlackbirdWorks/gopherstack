package gcpfunctions_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

type received struct {
	Headers map[string]string `json:"headers"`
	Env     map[string]string `json:"env"`
	Path    string            `json:"path"`
	Body    string            `json:"body"`
	Host    string            `json:"host"`
}

type fakeContainer struct {
	srv  *http.Server
	spec container.Spec
}

// fakeRuntime stands in for the container runtime: every container serves HTTP on its published port and
// answers with a JSON description of the request it received.
type fakeRuntime struct {
	failRun error
	boxes   map[string]*fakeContainer
	started []container.Spec
	reqs    []received
	removed []string
	reply   atomic.Int32
	seq     atomic.Int64
	mu      sync.Mutex
}

func newFakeRuntime() *fakeRuntime {
	f := &fakeRuntime{boxes: map[string]*fakeContainer{}}
	f.reply.Store(http.StatusOK)

	return f
}

func envMap(spec container.Spec) map[string]string {
	m := map[string]string{}

	for _, e := range spec.Env {
		k, v, _ := strings.Cut(e, "=")
		m[k] = v
	}

	return m
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	if f.failRun != nil {
		return "", f.failRun
	}

	_, host, _, err := container.ParsePortSpec(spec.Ports[0])
	if err != nil {
		return "", err
	}

	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", host))
	if err != nil {
		return "", err
	}

	env := envMap(spec)
	c := &fakeContainer{spec: spec}
	c.srv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		hdr := map[string]string{}

		for k := range r.Header {
			hdr[strings.ToLower(k)] = r.Header.Get(k)
		}

		rec := received{Headers: hdr, Env: env, Path: r.URL.Path, Body: string(body), Host: r.Host}

		f.mu.Lock()
		f.reqs = append(f.reqs, rec)
		f.mu.Unlock()

		w.WriteHeader(int(f.reply.Load()))
		_ = json.NewEncoder(w).Encode(rec)
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

func (f *fakeRuntime) requests() []received {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]received(nil), f.reqs...)
}
