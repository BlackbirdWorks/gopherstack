package gcpbatch_test

import (
	"bytes"
	"context"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	batch "cloud.google.com/go/batch/apiv1"
	"cloud.google.com/go/batch/apiv1/batchpb"
	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/gcpbatch"
)

const (
	project = "test-project"
	region  = "us-central1"
	parent  = "projects/" + project + "/locations/" + region
)

type fakeClock struct{ ns atomic.Int64 }

func newClock() *fakeClock {
	c := &fakeClock{}
	c.ns.Store(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano())

	return c
}

func (c *fakeClock) Now() time.Time { return time.Unix(0, c.ns.Load()).UTC() }

func (c *fakeClock) Advance(d time.Duration) { c.ns.Add(int64(d)) }

type env struct {
	backend *gcpbatch.InMemoryBackend
	clock   *fakeClock
	client  *batch.Client
}

type option2 func(*gcpbatch.InMemoryBackend)

func newEnv(t *testing.T, opts ...option2) *env {
	t.Helper()

	clk := newClock()
	b := gcpbatch.NewInMemoryBackend()
	b.SetClock(clk.Now)
	b.SetTiming(gcpbatch.Timing{})

	for _, o := range opts {
		o(b)
	}

	h := gcpbatch.NewHandler(b)
	h.Port = 0
	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	conn, err := grpc.NewClient(h.Addr(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })

	client, err := batch.NewClient(t.Context(), option.WithGRPCConn(conn))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	return &env{backend: b, clock: clk, client: client}
}

func withTiming(tm gcpbatch.Timing) option2 {
	return func(b *gcpbatch.InMemoryBackend) { b.SetTiming(tm) }
}

func withRuntime(rt gcpbatch.Runtime) option2 {
	return func(b *gcpbatch.InMemoryBackend) { b.SetRuntime(rt, "script:img") }
}

func withServices(s gcpbatch.Services) option2 {
	return func(b *gcpbatch.InMemoryBackend) { b.SetServices(s) }
}

func withTimer(f func(time.Duration) (<-chan time.Time, func() bool)) option2 {
	return func(b *gcpbatch.InMemoryBackend) { b.SetTimer(f) }
}

func containerRunnable(image string, cmds ...string) *batchpb.Runnable {
	return &batchpb.Runnable{Executable: &batchpb.Runnable_Container_{
		Container: &batchpb.Runnable_Container{ImageUri: image, Commands: cmds},
	}}
}

func basicJob(count int64, rs ...*batchpb.Runnable) *batchpb.Job {
	if len(rs) == 0 {
		rs = []*batchpb.Runnable{containerRunnable("ok:1")}
	}

	return &batchpb.Job{
		TaskGroups: []*batchpb.TaskGroup{{
			TaskCount: count, TaskSpec: &batchpb.TaskSpec{Runnables: rs},
		}},
		LogsPolicy: &batchpb.LogsPolicy{Destination: batchpb.LogsPolicy_CLOUD_LOGGING},
	}
}

func (e *env) create(t *testing.T, id string, job *batchpb.Job) *batchpb.Job {
	t.Helper()

	got, err := e.client.CreateJob(t.Context(), &batchpb.CreateJobRequest{Parent: parent, JobId: id, Job: job})
	require.NoError(t, err)

	return got
}

func (e *env) get(t *testing.T, id string) *batchpb.Job {
	t.Helper()

	got, err := e.client.GetJob(t.Context(), &batchpb.GetJobRequest{Name: parent + "/jobs/" + id})
	require.NoError(t, err)

	return got
}

func (e *env) tasks(t *testing.T, id string) []*batchpb.Task {
	t.Helper()

	it := e.client.ListTasks(
		t.Context(),
		&batchpb.ListTasksRequest{Parent: parent + "/jobs/" + id + "/taskGroups/group0"},
	)

	var out []*batchpb.Task

	for {
		task, err := it.Next()
		if err != nil {
			break
		}

		out = append(out, task)
	}

	return out
}

// behavior is what a fake container does; its return value is the exit code.
type behavior func(ctx context.Context, spec container.Spec, out io.Writer) int

type fakeBox struct {
	exit chan int
	done chan struct{}
	buf  bytes.Buffer
	mu   sync.Mutex
}

func (b *fakeBox) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return b.buf.Write(p)
}

// fakeRuntime runs containers in-process, keyed by image name.
type fakeRuntime struct {
	behaviors map[string]behavior
	boxes     map[string]*fakeBox
	specs     []container.Spec
	removed   int
	mu        sync.Mutex
	next      int
}

func newFakeRuntime(b map[string]behavior) *fakeRuntime {
	return &fakeRuntime{behaviors: b, boxes: map[string]*fakeBox{}}
}

func (f *fakeRuntime) CreateAndStart(ctx context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	run, ok := f.behaviors[spec.Image]
	if !ok {
		return "", io.ErrUnexpectedEOF
	}

	f.next++
	id := "fake-" + string(rune('a'+f.next%26)) + string(rune('a'+f.next/26))
	box := &fakeBox{exit: make(chan int, 1), done: make(chan struct{})}
	f.boxes[id] = box
	f.specs = append(f.specs, spec)

	go func() {
		code := run(ctx, spec, box)
		close(box.done)
		box.exit <- code
	}()

	return id, nil
}

func (f *fakeRuntime) StopAndRemove(context.Context, string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.removed++

	return nil
}

func (f *fakeRuntime) box(id string) *fakeBox {
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

func (f *fakeRuntime) StreamLogs(ctx context.Context, id string, stdout, _ io.Writer) error {
	box := f.box(id)

	select {
	case <-box.done:
	case <-ctx.Done():
	}

	box.mu.Lock()
	defer box.mu.Unlock()

	_, err := stdout.Write(box.buf.Bytes())

	return err
}

func (f *fakeRuntime) specsFor(image string) []container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	var out []container.Spec

	for _, s := range f.specs {
		if s.Image == image {
			out = append(out, s)
		}
	}

	return out
}

func envOf(spec container.Spec, key string) string {
	for _, kv := range spec.Env {
		if v, ok := strings.CutPrefix(kv, key+"="); ok {
			return v
		}
	}

	return ""
}

func exitsOK() behavior {
	return func(context.Context, container.Spec, io.Writer) int { return 0 }
}

func blocks() behavior {
	return func(ctx context.Context, _ container.Spec, _ io.Writer) int {
		<-ctx.Done()

		return 137
	}
}

type fakeSecrets map[string]string

func (f fakeSecrets) AccessSecret(name string) ([]byte, error) {
	v, ok := f[name]
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

func (l *fakeLogs) texts() []string {
	l.mu.Lock()
	defer l.mu.Unlock()

	out := make([]string, 0, len(l.entries))
	for _, e := range l.entries {
		out = append(out, e.GetTextPayload())
	}

	slices.Sort(out)

	return out
}
