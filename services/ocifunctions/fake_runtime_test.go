package ocifunctions_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

var errNoSocketDir = errors.New("no socket directory mounted at /tmp/iofs")

type fnCall struct {
	Header http.Header
	Body   string
	Box    string
}

type fakeBox struct {
	srv  *http.Server
	spec container.Spec
}

// fakeRuntime plays the container runtime: every container serves the Fn FDK http-stream contract on the
// unix socket named by its FN_LISTENER inside the bind-mounted socket directory.
type fakeRuntime struct {
	boxes     map[string]*fakeBox
	createErr error
	release   chan struct{}
	started   []container.Spec
	calls     []fnCall
	removed   []string
	pulls     []string
	seq       atomic.Int64
	mu        sync.Mutex
	hasImage  bool
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{boxes: map[string]*fakeBox{}, release: make(chan struct{}), hasImage: true}
}

func (f *fakeRuntime) HasImage(context.Context, string) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.hasImage, nil
}

func (f *fakeRuntime) PullImage(_ context.Context, ref string) error {
	f.mu.Lock()
	f.pulls = append(f.pulls, ref)
	f.mu.Unlock()

	return nil
}

func specEnv(spec container.Spec) map[string]string {
	m := map[string]string{}

	for _, e := range spec.Env {
		k, v, _ := strings.Cut(e, "=")
		m[k] = v
	}

	return m
}

func (f *fakeRuntime) handler(id string, spec container.Spec) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)

		f.mu.Lock()
		f.calls = append(f.calls, fnCall{Header: r.Header.Clone(), Body: string(body), Box: id})
		f.mu.Unlock()

		switch string(body) {
		case "fail":
			w.WriteHeader(http.StatusBadGateway)
			_, _ = w.Write([]byte("boom"))
		case "hang":
			<-r.Context().Done()
		case "block":
			select {
			case <-f.release:
			case <-r.Context().Done():
			}

			_, _ = w.Write([]byte("released"))
		default:
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Fn-Http-Status", "200")
			w.Header().Set("Fn-Http-H-X-Echo", "1")
			_ = json.NewEncoder(w).
				Encode(map[string]any{"body": string(body), "env": specEnv(spec), "path": r.URL.Path})
		}
	})
}

func (f *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	if f.createErr != nil {
		return "", f.createErr
	}

	var dir string

	for _, m := range spec.Mounts {
		if src, dst, ok := strings.Cut(m, ":"); ok && dst == "/tmp/iofs" {
			dir = src
		}
	}

	if dir == "" {
		return "", errNoSocketDir
	}

	l, err := (&net.ListenConfig{}).Listen(context.Background(), "unix", filepath.Join(dir, "lsnr.sock"))
	if err != nil {
		return "", fmt.Errorf("listen: %w", err)
	}

	id := fmt.Sprintf("fake-%d", f.seq.Add(1))
	b := &fakeBox{spec: spec, srv: &http.Server{Handler: f.handler(id, spec)}}

	go func() { _ = b.srv.Serve(l) }()

	f.mu.Lock()
	f.boxes[id] = b
	f.started = append(f.started, spec)
	f.mu.Unlock()

	return id, nil
}

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	b := f.boxes[id]
	delete(f.boxes, id)
	f.removed = append(f.removed, id)
	f.mu.Unlock()

	if b != nil {
		_ = b.srv.Close()
	}

	return nil
}

func (f *fakeRuntime) startedSpecs() []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]container.Spec(nil), f.started...)
}

func (f *fakeRuntime) removedIDs() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.removed...)
}

func (f *fakeRuntime) recorded() []fnCall {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]fnCall(nil), f.calls...)
}

func (f *fakeRuntime) running() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.boxes)
}
