package gcpcloudbuild_test

import (
	"context"
	"io"
	"slices"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/logging/apiv2/loggingpb"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
)

// behavior is what a fake step container does; its return value is the exit code.
type behavior func(ctx context.Context, spec container.Spec, out io.Writer) int

type fakeContainer struct {
	pr   *io.PipeReader
	exit chan int
	spec container.Spec
}

// fakeRuntime runs steps in-process, keyed by image name.
type fakeRuntime struct {
	behaviors map[string]behavior
	boxes     map[string]*fakeContainer
	started   []string
	removed   []string
	pushed    []string
	specs     []container.Spec
	mu        sync.Mutex
	next      int
}

func newFakeRuntime(b map[string]behavior) *fakeRuntime {
	return &fakeRuntime{behaviors: b, boxes: map[string]*fakeContainer{}}
}

func (f *fakeRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	run, ok := f.behaviors[spec.Image]
	if !ok {
		return "", &missingImageError{image: spec.Image}
	}

	f.next++
	id := "fake-" + string(rune('a'+f.next))
	pr, pw := io.Pipe()
	box := &fakeContainer{spec: spec, pr: pr, exit: make(chan int, 1)}
	f.boxes[id] = box
	f.started = append(f.started, spec.Image)
	f.specs = append(f.specs, spec)

	go func() {
		code := run(ctx, spec, pw)
		_ = pw.Close()
		box.exit <- code
	}()

	return id, nil
}

type missingImageError struct{ image string }

func (e *missingImageError) Error() string { return "No such image: " + e.image }

func (f *fakeRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed = append(f.removed, id)

	return nil
}

func (f *fakeRuntime) box(id string) *fakeContainer {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.boxes[id]
}

func (f *fakeRuntime) WaitExit(ctx context.Context, id string) (int, error) {
	select {
	case code := <-f.box(id).exit:
		return code, nil
	case <-ctx.Done():
		return 0, ctx.Err()
	}
}

func (f *fakeRuntime) StreamLogs(_ context.Context, id string, stdout, _ io.Writer) error {
	_, err := io.Copy(stdout, f.box(id).pr)

	return err
}

func (f *fakeRuntime) TagAndPush(_ context.Context, source, target string) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if strings.Contains(source, "missing") {
		return "", &missingImageError{image: source}
	}

	f.pushed = append(f.pushed, source+" -> "+target)

	return "sha256:feedface", nil
}

func (f *fakeRuntime) startedImages() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.started)
}

func (f *fakeRuntime) removals() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.removed)
}

func (f *fakeRuntime) pushes() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.Clone(f.pushed)
}

func (f *fakeRuntime) workspaceOf(image string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, s := range f.specs {
		if s.Image == image {
			host, _, _ := strings.Cut(s.Mounts[0], ":")

			return host
		}
	}

	return ""
}

func (f *fakeRuntime) specOf(image string) container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	for _, s := range f.specs {
		if s.Image == image {
			return s
		}
	}

	return container.Spec{}
}

// exits returns a behavior that prints lines and exits with code.
func exits(code int, lines ...string) behavior {
	return func(_ context.Context, _ container.Spec, out io.Writer) int {
		for _, l := range lines {
			_, _ = io.WriteString(out, l+"\n")
		}

		return code
	}
}

// blocks returns a behavior that runs until its context ends.
func blocks(started chan<- struct{}) behavior {
	return func(ctx context.Context, _ container.Spec, _ io.Writer) int {
		if started != nil {
			close(started)
		}

		<-ctx.Done()

		return 137
	}
}

type fakeStore struct {
	objects map[string][]byte
	buckets map[string]bool
	mu      sync.Mutex
}

func newFakeStore() *fakeStore {
	return &fakeStore{objects: map[string][]byte{}, buckets: map[string]bool{}}
}

func (s *fakeStore) ReadObject(bucket, object string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	data, ok := s.objects[bucket+"/"+object]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}

	return data, nil
}

func (s *fakeStore) WriteObject(bucket, object string, data []byte, _ string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.objects[bucket+"/"+object] = slices.Clone(data)

	return nil
}

func (s *fakeStore) EnsureBucket(_, bucket string) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.buckets[bucket] = true

	return nil
}

func (s *fakeStore) get(key string) ([]byte, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	v, ok := s.objects[key]

	return v, ok
}

type published struct {
	attrs map[string]string
	topic string
	data  []byte
}

type fakePublisher struct {
	msgs []published
	mu   sync.Mutex
}

func (p *fakePublisher) Publish(project, topic string, data []byte, attrs map[string]string) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	p.msgs = append(
		p.msgs,
		published{topic: "projects/" + project + "/topics/" + topic, data: data, attrs: attrs},
	)

	return nil
}

func (p *fakePublisher) statuses() []string {
	p.mu.Lock()
	defer p.mu.Unlock()

	out := make([]string, len(p.msgs))
	for i, m := range p.msgs {
		out[i] = m.attrs["status"]
	}

	return out
}

type fakeSecrets map[string]string

func (s fakeSecrets) AccessSecret(name string) ([]byte, error) {
	v, ok := s[name]
	if !ok {
		return nil, io.ErrUnexpectedEOF
	}

	return []byte(v), nil
}

type fakeLogs struct {
	entries []*loggingpb.LogEntry
	mu      sync.Mutex
}

func (l *fakeLogs) WriteLogEntries(_ context.Context, req *loggingpb.WriteLogEntriesRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.entries = append(l.entries, req.GetEntries()...)

	return nil
}

func (l *fakeLogs) lines() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, len(l.entries))
	for i, e := range l.entries {
		out[i] = e.GetTextPayload()
	}

	return out
}

// manualTimers hands out timers the test fires by hand.
type manualTimers struct {
	chans []chan time.Time
	durs  []time.Duration
	mu    sync.Mutex
}

func (m *manualTimers) start(d time.Duration) (<-chan time.Time, func() bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	ch := make(chan time.Time, 1)
	m.chans = append(m.chans, ch)
	m.durs = append(m.durs, d)

	return ch, func() bool { return true }
}

// fire triggers the first timer started with duration d.
func (m *manualTimers) fire(d time.Duration) bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	for i, got := range m.durs {
		if got == d {
			m.chans[i] <- time.Now()

			return true
		}
	}

	return false
}
