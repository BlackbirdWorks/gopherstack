package gcprun_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errNoBox = errors.New("no such container")

type fakeContainer struct {
	srv    *http.Server
	exited chan struct{}
	spec   container.Spec
	code   int
	once   sync.Once
}

func (c *fakeContainer) env(key string) string {
	for _, e := range c.spec.Env {
		if v, ok := strings.CutPrefix(e, key+"="); ok {
			return v
		}
	}

	return ""
}

func (c *fakeContainer) exit(code int) {
	c.once.Do(func() {
		c.code = code
		close(c.exited)
	})
}

// fakeRuntime stands in for the container runtime: containers with published ports serve HTTP,
// the others run until exitFn / gate lets them exit.
type fakeRuntime struct {
	failRun error
	boxes   map[string]*fakeContainer
	exitFn  func(env func(string) string) int
	gate    chan struct{}
	lines   []fakeLine
	started []container.Spec
	removed []string
	streams atomic.Int64
	seq     atomic.Int64
	mu      sync.Mutex
}

type fakeLine struct {
	text   string
	stderr bool
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{boxes: map[string]*fakeContainer{}}
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	if f.failRun != nil {
		return "", f.failRun
	}

	c := &fakeContainer{spec: spec, exited: make(chan struct{})}

	if len(spec.Ports) > 0 {
		if err := f.serve(c); err != nil {
			return "", err
		}
	}

	id := fmt.Sprintf("fake-%d", f.seq.Add(1))

	f.mu.Lock()
	f.boxes[id] = c
	f.started = append(f.started, spec)
	f.mu.Unlock()

	if len(spec.Ports) == 0 {
		go f.runJob(c)
	}

	return id, nil
}

func (f *fakeRuntime) serve(c *fakeContainer) error {
	_, host, _, err := container.ParsePortSpec(c.spec.Ports[0])
	if err != nil {
		return err
	}

	l, err := net.Listen("tcp", net.JoinHostPort("127.0.0.1", host))
	if err != nil {
		return err
	}

	c.srv = &http.Server{Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = fmt.Fprintf(
			w,
			"revision=%s port=%s host=%s path=%s",
			c.env("K_REVISION"),
			c.env("PORT"),
			r.Host,
			r.URL.Path,
		)
	})}

	go func() { _ = c.srv.Serve(l) }()

	return nil
}

func (f *fakeRuntime) runJob(c *fakeContainer) {
	if f.gate != nil {
		select {
		case <-f.gate:
		case <-c.exited:
			return
		}
	}

	code := 0
	if f.exitFn != nil {
		code = f.exitFn(c.env)
	}

	c.exit(code)
}

func (f *fakeRuntime) WaitExit(ctx context.Context, id string) (int, error) {
	f.mu.Lock()
	c := f.boxes[id]
	f.mu.Unlock()

	if c == nil {
		return 0, fmt.Errorf("%w: %s", errNoBox, id)
	}

	select {
	case <-c.exited:
		return c.code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	c := f.boxes[id]
	delete(f.boxes, id)
	f.removed = append(f.removed, id)
	f.mu.Unlock()

	if c == nil {
		return nil
	}

	c.exit(137)

	if c.srv != nil {
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

func envOf(spec container.Spec, key string) string {
	for _, e := range spec.Env {
		if v, ok := strings.CutPrefix(e, key+"="); ok {
			return v
		}
	}

	return ""
}

// StreamLogs emits the configured lines, then blocks until the container exits or is removed.
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
	case <-c.exited:
	case <-ctx.Done():
	}

	return nil
}
