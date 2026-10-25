package sagemaker_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/sagemaker"
)

var errFakeNoObject = errors.New("no such object")

// fakeContainer is one started container: either an HTTP server (model) or a one-shot job.
type fakeContainer struct {
	srv      *http.Server
	stop     chan struct{}
	done     chan struct{}
	spec     container.Spec
	exit     int
	stopOnce sync.Once
}

func (c *fakeContainer) halt() {
	c.stopOnce.Do(func() {
		close(c.stop)

		if c.srv != nil {
			_ = c.srv.Close()
		}
	})
}

// fakeRuntime is a container runtime double. serve builds the HTTP handler for model containers
// (matched on image); job runs the body of job containers and returns the exit code.
type fakeRuntime struct {
	serve   func(spec container.Spec) http.Handler
	job     func(spec container.Spec, stop <-chan struct{}) int
	logs    map[string]string
	started map[string]*fakeContainer
	removed []string
	seq     atomic.Int64
	mu      sync.Mutex
}

func newFakeRuntime() *fakeRuntime {
	return &fakeRuntime{started: map[string]*fakeContainer{}, logs: map[string]string{}}
}

func (r *fakeRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	id := fmt.Sprintf("fake%08d", r.seq.Add(1))
	c := &fakeContainer{spec: spec, stop: make(chan struct{}), done: make(chan struct{})}

	if len(spec.Ports) > 0 {
		parts := strings.Split(spec.Ports[0], ":")

		ln, err := net.Listen("tcp", parts[0]+":"+parts[1])
		if err != nil {
			return "", err
		}

		c.srv = &http.Server{Handler: r.serve(spec)}

		go func() {
			_ = c.srv.Serve(ln)
			close(c.done)
		}()
	} else {
		go func() {
			c.exit = r.job(spec, c.stop)
			close(c.done)
		}()
	}

	r.mu.Lock()
	r.started[id] = c
	r.mu.Unlock()

	return id, nil
}

func (r *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	r.mu.Lock()
	c := r.started[id]
	r.removed = append(r.removed, id)
	r.mu.Unlock()

	if c != nil {
		c.halt()
	}

	return nil
}

func (r *fakeRuntime) WaitExit(ctx context.Context, id string) (int, error) {
	r.mu.Lock()
	c := r.started[id]
	r.mu.Unlock()

	select {
	case <-c.done:
		return c.exit, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (r *fakeRuntime) StreamLogs(ctx context.Context, id string, stdout, _ io.Writer) error {
	r.mu.Lock()
	c := r.started[id]
	text := r.logs[c.spec.Image]
	r.mu.Unlock()

	if text != "" {
		_, _ = io.WriteString(stdout, text)
	}

	select {
	case <-c.done:
	case <-ctx.Done():
	}

	return nil
}

func (r *fakeRuntime) removedCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()

	return len(r.removed)
}

func (r *fakeRuntime) lastSpec(image string) (container.Spec, bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	ids := make([]string, 0, len(r.started))
	for id := range r.started {
		ids = append(ids, id)
	}

	sort.Strings(ids)

	for _, id := range slices.Backward(ids) {
		if c := r.started[id]; c.spec.Image == image {
			return c.spec, true
		}
	}

	return container.Spec{}, false
}

// mountSource returns the host directory bind-mounted at target.
func mountSource(spec container.Spec, target string) string {
	for _, m := range spec.Mounts {
		if src, dst, ok := strings.Cut(m, ":"); ok && strings.TrimSuffix(dst, ":ro") == target {
			return src
		}
	}

	return ""
}

func envValue(spec container.Spec, key string) string {
	for _, e := range spec.Env {
		if v, ok := strings.CutPrefix(e, key+"="); ok {
			return v
		}
	}

	return ""
}

// fakeS3 is an in-memory ObjectStore.
type fakeS3 struct {
	objects map[string][]byte
	mu      sync.Mutex
}

func newFakeS3() *fakeS3 { return &fakeS3{objects: map[string][]byte{}} }

func (s *fakeS3) GetObjectBytes(_ context.Context, bucket, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if v, ok := s.objects[bucket+"/"+key]; ok {
		return v, nil
	}

	return nil, fmt.Errorf("%w: %s/%s", errFakeNoObject, bucket, key)
}

func (s *fakeS3) PutObjectBytes(_ context.Context, bucket, key string, data []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[bucket+"/"+key] = data

	return nil
}

func (s *fakeS3) ListObjectKeys(_ context.Context, bucket, prefix string) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	var keys []string

	for k := range s.objects {
		if key, ok := strings.CutPrefix(k, bucket+"/"); ok && strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}

	sort.Strings(keys)

	return keys, nil
}

func (s *fakeS3) get(bucket, key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.objects[bucket+"/"+key]

	return v, ok
}

// fakeLogs records CloudWatch Logs lines per group/stream.
type fakeLogs struct {
	lines map[string][]string
	mu    sync.Mutex
}

func newFakeLogs() *fakeLogs { return &fakeLogs{lines: map[string][]string{}} }

func (l *fakeLogs) EnsureLogGroupAndStream(_ context.Context, _, group, stream string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if _, ok := l.lines[group+"|"+stream]; !ok {
		l.lines[group+"|"+stream] = nil
	}

	return nil
}

func (l *fakeLogs) PutLogLines(_ context.Context, _, group, stream string, lines []string) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.lines[group+"|"+stream] = append(l.lines[group+"|"+stream], lines...)

	return nil
}

// groupLines returns every line written to log streams of group.
func (l *fakeLogs) groupLines(group string) []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	var out []string

	for k, v := range l.lines {
		if strings.HasPrefix(k, group+"|") {
			out = append(out, v...)
		}
	}

	return out
}

// fakeSNS records published messages by topic.
type fakeSNS struct {
	msgs map[string][]string
	mu   sync.Mutex
}

func newFakeSNS() *fakeSNS { return &fakeSNS{msgs: map[string][]string{}} }

func (n *fakeSNS) Publish(_ context.Context, _, topic, message string) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	n.msgs[topic] = append(n.msgs[topic], message)

	return nil
}

func (n *fakeSNS) messages(topic string) []string {
	n.mu.Lock()
	defer n.mu.Unlock()

	return append([]string(nil), n.msgs[topic]...)
}

// engineRig bundles a backend running the container engine against the fakes.
type engineRig struct {
	b    *sagemaker.InMemoryBackend
	rt   *fakeRuntime
	s3   *fakeS3
	logs *fakeLogs
	sns  *fakeSNS
}

func newEngineRig(t *testing.T, cfg sagemaker.EngineConfig) *engineRig {
	t.Helper()

	rig := &engineRig{
		b: sagemaker.NewInMemoryBackend("000000000000", "us-east-1"), rt: newFakeRuntime(),
		s3: newFakeS3(), logs: newFakeLogs(), sns: newFakeSNS(),
	}

	if cfg.Runtime == nil && cfg.Reason == "" {
		cfg.Runtime = rig.rt
	}

	if cfg.WorkDir == "" {
		cfg.WorkDir = t.TempDir()
	}

	rig.b.EnableEngine(cfg)
	rig.b.SetEngineServices(sagemaker.EngineServices{Store: rig.s3, Logs: rig.logs, Notifier: rig.sns})

	t.Cleanup(func() { rig.b.Shutdown(context.Background()) })

	return rig
}

func readFileOrEmpty(path string) string {
	b, err := os.ReadFile(filepath.Clean(path))
	if err != nil {
		return ""
	}

	return string(b)
}

func requireNoErr(t *testing.T, err error) {
	t.Helper()
	require.NoError(t, err)
}
