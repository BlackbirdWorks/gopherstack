package gcpsql_test

import (
	"context"
	"errors"
	"maps"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/api/option"
	sqladmin "google.golang.org/api/sqladmin/v1"

	"github.com/blackbirdworks/gopherstack/pkgs/dbrepl"
	"github.com/blackbirdworks/gopherstack/services/gcpsql"
)

const project = "test-project"

var (
	errFakeLaunch  = errors.New("launch refused")
	errFakeExists  = errors.New("already exists")
	errFakeRole    = errors.New("role rejected")
	errFakeMissing = errors.New("no such object")
)

type fakeEngine struct {
	enableBase   time.Time
	users        map[string]string
	dbs          map[string]bool
	replSrc      gcpsql.ReplicaSource
	rows         [][]string
	inserted     [][]string
	flags        []gcpsql.Flag
	seed         []byte
	pitrDBs      []string
	applied      []gcpsql.Recovery
	prepared     []time.Time
	replCalls    []bool
	restored     []string
	pitrEnabled  int
	starts       int
	stops        int
	restarts     int
	port         int
	pitrDisabled int
	attached     int
	promoted     int
	statusLag    float64
	mu           sync.Mutex
	closed       bool
	purged       bool
	persistent   bool
}

func (f *fakeEngine) Endpoint() (string, int) { return "127.0.0.1", f.port }

func (f *fakeEngine) CreateDatabase(_ context.Context, name, _, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.dbs[name] {
		return errFakeExists
	}

	f.dbs[name] = true

	return nil
}

func (f *fakeEngine) DropDatabase(_ context.Context, name string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.dbs, name)

	return nil
}

func (f *fakeEngine) ListDatabases(context.Context) ([]string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	out := make([]string, 0, len(f.dbs))
	for n := range f.dbs {
		out = append(out, n)
	}

	sort.Strings(out)

	return out, nil
}

func (f *fakeEngine) UpsertUser(_ context.Context, u gcpsql.UserSpec) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	if u.Name == "boom" {
		return errFakeRole
	}

	f.users[u.Name+"@"+u.Host] = u.Password

	return nil
}

func (f *fakeEngine) DropUser(_ context.Context, name, host string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.users, name+"@"+host)

	return nil
}

func (f *fakeEngine) Restart(_ context.Context, flags []gcpsql.Flag) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.restarts++
	f.flags = flags

	return nil
}

func (f *fakeEngine) Stop(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++

	return nil
}

func (f *fakeEngine) Start(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++

	return nil
}

func (f *fakeEngine) Dump(_ context.Context, dbs []string, opts gcpsql.DumpOptions) ([]byte, error) {
	var sb strings.Builder

	for _, d := range dbs {
		if opts.Markers {
			sb.WriteString("\\connect \"" + d + "\"\n")
		}

		sb.WriteString("-- dump of " + d + "\n")
	}

	return []byte(sb.String()), nil
}

func (f *fakeEngine) Restore(_ context.Context, db string, data []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.restored = append(f.restored, db+"|"+string(data))
	if strings.Contains(string(data), "newdb") {
		f.dbs["newdb"] = true
	}

	return nil
}

func (f *fakeEngine) Query(context.Context, string, string) ([][]string, error) {
	return f.rows, nil
}

func (f *fakeEngine) InsertRows(_ context.Context, _, table string, _ []string, rows [][]string) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.inserted = append(f.inserted, append([]string{table}, rows[0]...))

	return nil
}

func (f *fakeEngine) Close(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.closed = true
}

func (f *fakeEngine) ReplicationSource(context.Context) (gcpsql.ReplicaSource, error) {
	return gcpsql.ReplicaSource{Network: "net-" + strconv.Itoa(f.port), Host: "ctr-" + strconv.Itoa(f.port), Port: 5432,
		Password: "repl-pw"}, nil
}

func (f *fakeEngine) ReplicaSeed(_ context.Context, dbs []string) ([]byte, error) {
	return []byte("seed:" + strings.Join(dbs, ",")), nil
}

func (f *fakeEngine) AttachReplica(_ context.Context, src gcpsql.ReplicaSource, seed []byte) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.replSrc, f.seed = src, seed
	f.attached++

	return nil
}

func (f *fakeEngine) Promote(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.promoted++

	return nil
}

func (f *fakeEngine) SetReplication(_ context.Context, on bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.replCalls = append(f.replCalls, on)

	return nil
}

func (f *fakeEngine) ReplicationStatus(context.Context) (dbrepl.Status, error) {
	return dbrepl.Status{Replicating: true, LagSeconds: f.statusLag}, nil
}

func (f *fakeEngine) EnablePITR(_ context.Context, dbs []string) (time.Time, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.pitrEnabled++
	f.pitrDBs = dbs

	if f.enableBase.IsZero() {
		f.enableBase = time.Now().UTC().Add(-time.Hour)
	}

	return f.enableBase, nil
}

func (f *fakeEngine) DisablePITR(context.Context) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.pitrDisabled++

	return nil
}

func (f *fakeEngine) PrepareRecovery(_ context.Context, at time.Time) (gcpsql.Recovery, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.prepared = append(f.prepared, at)

	return gcpsql.Recovery{At: at, Volume: "pitr-vol", Script: []byte("replay")}, nil
}

func (f *fakeEngine) ApplyRecovery(_ context.Context, rec gcpsql.Recovery) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.applied = append(f.applied, rec)

	return nil
}

func (f *fakeEngine) Purge(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.purged, f.closed = true, true
}

func (f *fakeEngine) RemoveVolumes(context.Context) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.purged = true
}

func (f *fakeEngine) Persistent() bool { return f.persistent }

func (f *fakeEngine) snapshot() fakeEngine {
	f.mu.Lock()
	defer f.mu.Unlock()

	return fakeEngine{
		restarts: f.restarts, stops: f.stops, starts: f.starts, closed: f.closed, flags: f.flags,
		restored: append([]string(nil), f.restored...), inserted: append([][]string(nil), f.inserted...),
		dbs: copyBool(f.dbs), users: copyStr(f.users), replSrc: f.replSrc, seed: f.seed,
		replCalls: append([]bool(nil), f.replCalls...), prepared: append([]time.Time(nil), f.prepared...),
		applied: append([]gcpsql.Recovery(nil), f.applied...), pitrDBs: f.pitrDBs, pitrEnabled: f.pitrEnabled,
		pitrDisabled: f.pitrDisabled, attached: f.attached, promoted: f.promoted, purged: f.purged,
	}
}

func copyBool(m map[string]bool) map[string]bool {
	return maps.Clone(m)
}

func copyStr(m map[string]string) map[string]string {
	return maps.Clone(m)
}

type fakeLauncher struct {
	err     error
	swept   []map[string]bool
	engines []*fakeEngine
	specs   []gcpsql.LaunchSpec
	mu      sync.Mutex
	persist bool
}

func (l *fakeLauncher) Launch(_ context.Context, spec gcpsql.LaunchSpec) (gcpsql.Engine, error) {
	if l.err != nil {
		return nil, l.err
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	e := &fakeEngine{
		dbs: map[string]bool{}, users: map[string]string{}, port: 40000 + len(l.engines), persistent: l.persist,
		rows: [][]string{{"id", "name"}, {"1", "a,b"}, {"2", "c"}},
	}
	l.engines = append(l.engines, e)
	l.specs = append(l.specs, spec)

	return e, nil
}

func (l *fakeLauncher) SweepVolumes(_ context.Context, keep map[string]bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	l.swept = append(l.swept, keep)
}

func (l *fakeLauncher) engine(i int) *fakeEngine {
	l.mu.Lock()
	defer l.mu.Unlock()

	return l.engines[i]
}

type fakeObjects struct {
	data map[string][]byte
	mu   sync.Mutex
}

func (f *fakeObjects) ReadObject(bucket, name string) ([]byte, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	d, ok := f.data[bucket+"/"+name]
	if !ok {
		return nil, errFakeMissing
	}

	return d, nil
}

func (f *fakeObjects) WriteObject(bucket, name string, data []byte, _ string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[bucket+"/"+name] = data

	return nil
}

func (f *fakeObjects) get(key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.data[key]
}

type env struct {
	svc      *sqladmin.Service
	backend  *gcpsql.InMemoryBackend
	launcher *fakeLauncher
	objects  *fakeObjects
	url      string
	timeout  time.Duration
}

func newEnv(t *testing.T) *env {
	t.Helper()

	return newEnvWith(t, &fakeLauncher{})
}

func newEnvWith(t *testing.T, l *fakeLauncher) *env {
	t.Helper()

	backend := gcpsql.NewInMemoryBackend()
	if l != nil {
		backend.SetLauncher(l)
	}

	objects := &fakeObjects{data: map[string][]byte{}}
	backend.SetObjectStore(objects)

	e := echo.New()
	e.Any("/*", gcpsql.NewHandler(backend).Handler())

	srv := httptest.NewServer(e)
	t.Cleanup(srv.Close)
	t.Cleanup(backend.Close)

	svc, err := sqladmin.NewService(t.Context(), option.WithEndpoint(srv.URL), option.WithoutAuthentication())
	require.NoError(t, err)

	return &env{svc: svc, backend: backend, launcher: l, objects: objects, url: srv.URL, timeout: 10 * time.Second}
}

// wait polls an operation until it is DONE and returns it.
func (e *env) wait(t *testing.T, op *sqladmin.Operation) *sqladmin.Operation {
	t.Helper()

	var got *sqladmin.Operation

	require.Eventually(t, func() bool {
		var err error

		got, err = e.svc.Operations.Get(project, op.Name).Context(t.Context()).Do()

		return err == nil && got.Status == "DONE"
	}, e.timeout, 5*time.Millisecond)

	return got
}

func (e *env) mustWait(t *testing.T, op *sqladmin.Operation, err error) *sqladmin.Operation {
	t.Helper()
	require.NoError(t, err)

	done := e.wait(t, op)
	if done.Error != nil {
		require.Failf(t, "operation failed", "%+v", done.Error.Errors[0])
	}

	return done
}

func (e *env) createInstance(
	t *testing.T,
	name, version string,
	settings *sqladmin.Settings,
) *sqladmin.DatabaseInstance {
	t.Helper()

	if settings == nil {
		settings = &sqladmin.Settings{}
	}

	if settings.Tier == "" {
		settings.Tier = "db-custom-1-3840"
	}

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: version, Region: "us-central1", Settings: settings, RootPassword: "pw-root",
	}).Context(t.Context()).Do()
	e.mustWait(t, op, err)

	inst, err := e.svc.Instances.Get(project, name).Context(t.Context()).Do()
	require.NoError(t, err)
	assert.Equal(t, "RUNNABLE", inst.State)

	return inst
}

func exportReq(ec *sqladmin.ExportContext) *sqladmin.InstancesExportRequest {
	return &sqladmin.InstancesExportRequest{ExportContext: ec}
}

func importReq(ic *sqladmin.ImportContext) *sqladmin.InstancesImportRequest {
	return &sqladmin.InstancesImportRequest{ImportContext: ic}
}
