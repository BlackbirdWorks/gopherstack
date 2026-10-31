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
	"github.com/blackbirdworks/gopherstack/pkgs/gcpserve"
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
	gone chan struct{}
	spec container.Spec
}

// fakeRuntime stands in for the container runtime: every container serves HTTP on its published port and
// answers with a JSON description of the request it received.
type fakeRuntime struct {
	failRun error
	gate    chan struct{}
	boxes   map[string]*fakeContainer
	hold    atomic.Pointer[chan struct{}]
	started []container.Spec
	reqs    []received
	lines   []fakeLine
	removed []string
	streams atomic.Int64
	seq     atomic.Int64
	mu      sync.Mutex
	reply   atomic.Int32
}

type fakeLine struct {
	text   string
	stderr bool
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

	addr := net.JoinHostPort("127.0.0.1", host)

	var l net.Listener

	if f.gate == nil {
		if l, err = net.Listen("tcp", addr); err != nil {
			return "", err
		}
	}

	env := envMap(spec)
	c := &fakeContainer{spec: spec, gone: make(chan struct{})}
	c.srv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.UserAgent() == gcpserve.ProbeUserAgent {
			return
		}

		body, _ := io.ReadAll(r.Body)
		hdr := map[string]string{}

		for k := range r.Header {
			hdr[strings.ToLower(k)] = r.Header.Get(k)
		}

		rec := received{Headers: hdr, Env: env, Path: r.URL.Path, Body: string(body), Host: r.Host}

		f.mu.Lock()
		f.reqs = append(f.reqs, rec)
		f.mu.Unlock()

		if h := f.hold.Load(); h != nil {
			select {
			case <-*h:
			case <-r.Context().Done():
				return
			}
		}

		w.WriteHeader(int(f.reply.Load()))
		_ = json.NewEncoder(w).Encode(rec)
	})}

	go f.serve(c, l, addr)

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

		close(c.gone)
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

// serve starts listening immediately, or once the readiness gate opens when one is set.
func (f *fakeRuntime) serve(c *fakeContainer, l net.Listener, addr string) {
	if l == nil {
		<-f.gate

		var err error
		if l, err = net.Listen("tcp", addr); err != nil {
			return
		}
	}

	_ = c.srv.Serve(l)
}

// StreamLogs emits the configured lines, then blocks until the container is removed or ctx ends.
func (f *fakeRuntime) StreamLogs(ctx context.Context, id string, stdout, stderr io.Writer) error {
	f.mu.Lock()
	c := f.boxes[id]
	f.mu.Unlock()

	if c == nil {
		return nil
	}

	f.streams.Add(1)
	defer f.streams.Add(-1)

	for _, l := range f.lines {
		w := stdout
		if l.stderr {
			w = stderr
		}

		if _, err := io.WriteString(w, l.text+"\n"); err != nil {
			return err
		}
	}

	select {
	case <-c.gone:
	case <-ctx.Done():
	}

	return nil
}

// holdRequests makes every request block until the returned function is called.
func (f *fakeRuntime) holdRequests() func() {
	ch := make(chan struct{})
	f.hold.Store(&ch)

	var once sync.Once

	return func() { once.Do(func() { close(ch) }) }
}
