package gcplogging_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"cloud.google.com/go/logging"
	logv2 "cloud.google.com/go/logging/apiv2"
	"cloud.google.com/go/logging/logadmin"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blackbirdworks/gopherstack/services/gcplogging"
)

const (
	testProject = "test-project"
	proj        = "projects/" + testProject
)

type clock struct{ ns atomic.Int64 }

func (c *clock) now() time.Time          { return time.Unix(0, c.ns.Load()).UTC() }
func (c *clock) advance(d time.Duration) { c.ns.Add(int64(d)) }

var errEmptySchema = errors.New("empty schema")

type fakePub struct {
	msgs map[string][]string
	attr map[string][]map[string]string
	mu   sync.Mutex
}

func (f *fakePub) Publish(topic string, data []byte, attrs map[string]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.msgs == nil {
		f.msgs, f.attr = map[string][]string{}, map[string][]map[string]string{}
	}

	f.msgs[topic] = append(f.msgs[topic], string(data))
	f.attr[topic] = append(f.attr[topic], attrs)

	return nil
}

func (f *fakePub) topic(name string) []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]string(nil), f.msgs[name]...)
}

type fakeObjs struct {
	objs map[string][]byte
	mu   sync.Mutex
}

func (f *fakeObjs) ReadObject(bucket, name string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if d, ok := f.objs[bucket+"/"+name]; ok {
		return d, nil
	}

	return nil, gcplogging.ErrObjectNotFound
}

func (f *fakeObjs) WriteObject(bucket, name string, data []byte, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.objs == nil {
		f.objs = map[string][]byte{}
	}

	f.objs[bucket+"/"+name] = append([]byte(nil), data...)

	return nil
}

func (f *fakeObjs) get(key string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return string(f.objs[key])
}

func (f *fakeObjs) names() []string {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.objs))
	for k := range f.objs {
		out = append(out, k)
	}

	return out
}

type fakeTables struct {
	rows   map[string][]map[string]any
	create map[string]int
	mu     sync.Mutex
}

func (f *fakeTables) EnsureTable(project, dataset, table string, schema []map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.create == nil {
		f.create, f.rows = map[string]int{}, map[string][]map[string]any{}
	}

	if len(schema) == 0 {
		return errEmptySchema
	}

	f.create[project+"."+dataset+"."+table]++

	return nil
}

func (f *fakeTables) InsertRows(project, dataset, table string, rows []map[string]any) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	k := project + "." + dataset + "." + table
	f.rows[k] = append(f.rows[k], rows...)

	return nil
}

func (f *fakeTables) table(key string) []map[string]any {
	f.mu.Lock()
	defer f.mu.Unlock()

	return append([]map[string]any(nil), f.rows[key]...)
}

type env struct {
	handler *gcplogging.Handler
	clk     *clock
	pub     *fakePub
	objs    *fakeObjs
	tables  *fakeTables
	opts    []option.ClientOption
}

// newEnv starts a listener on a free port. fixedClock pins the backend clock to 2030-01-01.
func newEnv(t *testing.T, fixedClock bool) *env {
	t.Helper()

	h := gcplogging.NewHandler(gcplogging.NewInMemoryBackend())
	h.Port = 0

	e := &env{handler: h, clk: &clock{}, pub: &fakePub{}, objs: &fakeObjs{}, tables: &fakeTables{}}
	e.clk.ns.Store(time.Date(2030, 1, 1, 12, 30, 0, 0, time.UTC).UnixNano())

	if fixedClock {
		h.Backend.SetClock(e.clk.now)
	}

	h.Backend.SetPublisher(e.pub)
	h.Backend.SetObjectStore(e.objs)
	h.Backend.SetTableStore(e.tables)

	require.NoError(t, h.StartWorker(t.Context()))
	t.Cleanup(func() { h.Shutdown(context.Background()) })

	e.opts = []option.ClientOption{
		option.WithEndpoint(h.Addr()),
		option.WithoutAuthentication(),
		option.WithGRPCDialOption(grpc.WithTransportCredentials(insecure.NewCredentials())),
	}

	return e
}

func (e *env) logger(t *testing.T) *logging.Client {
	t.Helper()

	c, err := logging.NewClient(t.Context(), proj, e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) admin(t *testing.T) *logadmin.Client {
	t.Helper()

	c, err := logadmin.NewClient(t.Context(), testProject, e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) raw(t *testing.T) *logv2.Client {
	t.Helper()

	c, err := logv2.NewClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) config(t *testing.T) *logv2.ConfigClient {
	t.Helper()

	c, err := logv2.NewConfigClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}

func (e *env) metrics(t *testing.T) *logv2.MetricsClient {
	t.Helper()

	c, err := logv2.NewMetricsClient(t.Context(), e.opts...)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	return c
}
