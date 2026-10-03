package rds_test

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

const engineWait = 10 * time.Second

type fakeEngineRuntime struct {
	running   map[string]bool
	createErr error
	specs     []container.Spec
	mu        sync.Mutex
	stops     int
	starts    int
}

func newFakeEngineRuntime() *fakeEngineRuntime {
	return &fakeEngineRuntime{running: make(map[string]bool)}
}

func (f *fakeEngineRuntime) CreateAndStart(_ context.Context, spec container.Spec) (string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	if f.createErr != nil {
		return "", f.createErr
	}

	f.specs = append(f.specs, spec)
	id := "ctr-" + strconv.Itoa(len(f.specs))
	f.running[id] = true

	return id, nil
}

func (f *fakeEngineRuntime) StopAndRemove(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	delete(f.running, id)

	return nil
}

func (f *fakeEngineRuntime) StopContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.stops++
	f.running[id] = false

	return nil
}

func (f *fakeEngineRuntime) StartContainer(_ context.Context, id string) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.starts++
	f.running[id] = true

	return nil
}

func (f *fakeEngineRuntime) containers() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.running)
}

func (f *fakeEngineRuntime) firstSpec() container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.specs[0]
}

func (f *fakeEngineRuntime) counts() (int, int) {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.stops, f.starts
}

type engineFixture struct {
	rt       *fakeEngineRuntime
	b        *rds.InMemoryBackend
	probeErr error
	passwds  []string
	logins   []string
	mu       sync.Mutex
}

func newEngineFixture(t *testing.T, startTimeout time.Duration) *engineFixture {
	t.Helper()

	f := &engineFixture{rt: newFakeEngineRuntime(), b: rds.NewInMemoryBackend("123456789012", "us-east-1")}
	t.Cleanup(f.b.Close)
	f.b.EnableEngine(rds.EngineConfig{
		Runtime:      f.rt,
		StartTimeout: startTimeout,
		Probe: func(_ context.Context, _ rds.EngineLogin) error {
			f.mu.Lock()
			defer f.mu.Unlock()

			return f.probeErr
		},
		SetPassword: func(_ context.Context, l rds.EngineLogin, newPassword string) error {
			f.mu.Lock()
			defer f.mu.Unlock()
			f.logins = append(f.logins, l.Password)
			f.passwds = append(f.passwds, newPassword)

			return nil
		},
	})

	return f
}

func (f *engineFixture) createInstance(t *testing.T, id, engine, version, password string) {
	t.Helper()

	_, err := f.b.CreateDBInstance(id, engine, "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{EngineVersion: version, MasterUserPassword: password})
	require.NoError(t, err)
}

func (f *engineFixture) instance(t *testing.T, id string) rds.DBInstance {
	t.Helper()

	got, err := f.b.DescribeDBInstances(id)
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

func (f *engineFixture) cluster(t *testing.T, id string) rds.DBCluster {
	t.Helper()

	got, err := f.b.DescribeDBClusters(id)
	require.NoError(t, err)
	require.Len(t, got, 1)

	return got[0]
}

func (f *engineFixture) waitInstance(t *testing.T, id, status string) rds.DBInstance {
	t.Helper()
	require.Eventually(t, func() bool { return f.instance(t, id).DBInstanceStatus == status },
		engineWait, 5*time.Millisecond, "instance %s never reached %s", id, status)

	return f.instance(t, id)
}

func hostPort(t *testing.T, spec container.Spec) int {
	t.Helper()
	require.Len(t, spec.Ports, 1)

	_, host, _, _ := container.ParsePortSpec(spec.Ports[0])
	p, err := strconv.Atoi(host)
	require.NoError(t, err)

	return p
}

func TestEngineImageAndEnvSelection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		engine   string
		version  string
		image    string
		wantPort string
		wantEnv  []string
	}{
		{name: "postgres version", engine: "postgres", version: "16.4", image: "postgres:16",
			wantEnv:  []string{"POSTGRES_USER=master", "POSTGRES_PASSWORD=pw", "POSTGRES_DB=appdb"},
			wantPort: ":5432"},
		{name: "postgres default", engine: "postgres", image: "postgres:17", wantPort: ":5432"},
		{name: "postgres unsupported", engine: "postgres", version: "9.6.1", image: "postgres:17", wantPort: ":5432"},
		{name: "aurora postgres", engine: "aurora-postgresql", version: "15.4", image: "postgres:15",
			wantPort: ":5432"},
		{name: "mysql 80", engine: "mysql", version: "8.0.36", image: "mysql:8.0",
			wantEnv: []string{"MYSQL_ROOT_PASSWORD=pw", "MYSQL_USER=master", "MYSQL_PASSWORD=pw",
				"MYSQL_DATABASE=appdb"},
			wantPort: ":3306"},
		{name: "mysql default", engine: "mysql", image: "mysql:8.4", wantPort: ":3306"},
		{name: "aurora mysql", engine: "aurora-mysql", version: "8.0.mysql_aurora.3.05.2", image: "mysql:8.0",
			wantPort: ":3306"},
		{name: "mariadb", engine: "mariadb", version: "10.11.6", image: "mariadb:10.11",
			wantEnv: []string{"MYSQL_DATABASE=appdb"}, wantPort: ":3306"},
		{name: "mariadb default", engine: "mariadb", image: "mariadb:11.4", wantPort: ":3306"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.createInstance(t, "db1", tt.engine, tt.version, "pw")
			f.waitInstance(t, "db1", "available")

			spec := f.rt.firstSpec()
			assert.Equal(t, tt.image, spec.Image)
			assert.Contains(t, spec.Ports[0], tt.wantPort)
			assert.Subset(t, spec.Env, tt.wantEnv)
		})
	}
}

func TestEngineInstanceStateMachineAndEndpoint(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)

	f.mu.Lock()
	f.probeErr = assert.AnError
	f.mu.Unlock()

	f.createInstance(t, "db1", "postgres", "16.4", "pw")
	assert.Equal(t, "creating", f.instance(t, "db1").DBInstanceStatus)

	require.Eventually(t, func() bool { return f.rt.containers() == 1 }, engineWait, 5*time.Millisecond)
	assert.Equal(t, "creating", f.instance(t, "db1").DBInstanceStatus)

	f.mu.Lock()
	f.probeErr = nil
	f.mu.Unlock()

	inst := f.waitInstance(t, "db1", "available")
	assert.Equal(t, "127.0.0.1", inst.Endpoint)
	assert.Equal(t, hostPort(t, f.rt.firstSpec()), inst.Port)
}

func TestEngineStartFailure(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup func(f *engineFixture)
		name  string
	}{
		{func(f *engineFixture) { f.probeErr = assert.AnError }, "probe never succeeds"},
		{func(f *engineFixture) { f.rt.createErr = assert.AnError }, "container create fails"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, 20*time.Millisecond)
			tt.setup(f)
			f.createInstance(t, "db1", "mysql", "", "pw")

			f.waitInstance(t, "db1", "failed")
			require.Eventually(t, func() bool { return f.rt.containers() == 0 }, engineWait, 5*time.Millisecond)
		})
	}
}

func TestEngineDeleteRemovesContainer(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.createInstance(t, "db1", "postgres", "", "pw")
	f.waitInstance(t, "db1", "available")
	require.Equal(t, 1, f.rt.containers())

	_, err := f.b.DeleteDBInstanceWithOptions("db1", true, "", true)
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.rt.containers() == 0 }, engineWait, 5*time.Millisecond)
}

func TestEngineResetAndCloseRemoveContainers(t *testing.T) {
	t.Parallel()

	tests := []struct {
		stop func(f *engineFixture)
		name string
	}{
		{func(f *engineFixture) { f.b.Reset() }, "reset"},
		{func(f *engineFixture) { f.b.Close() }, "close"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.createInstance(t, "db1", "postgres", "", "pw")
			f.createInstance(t, "db2", "mysql", "", "pw")
			f.waitInstance(t, "db1", "available")
			f.waitInstance(t, "db2", "available")

			tt.stop(f)
			require.Eventually(t, func() bool { return f.rt.containers() == 0 }, engineWait, 5*time.Millisecond)
		})
	}
}

func TestEngineAuroraClusterSharesOneContainer(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	_, err := f.b.CreateDBCluster("c1", "aurora-mysql", "master", "appdb", "", 0, nil,
		rds.DBClusterOptions{MasterUserPassword: "pw", EngineVersion: "8.0.mysql_aurora.3.05.2"})
	require.NoError(t, err)
	assert.Equal(t, "creating", f.cluster(t, "c1").Status)

	for _, id := range []string{"w1", "r1"} {
		_, err = f.b.CreateDBInstance(id, "aurora-mysql", "db.r5.large", "", "master", "", 0,
			rds.DBInstanceOptions{DBClusterIdentifier: "c1"})
		require.NoError(t, err)
	}

	require.Eventually(t, func() bool { return f.cluster(t, "c1").Status == "available" },
		engineWait, 5*time.Millisecond)

	port := hostPort(t, f.rt.firstSpec())
	cl := f.cluster(t, "c1")
	assert.Equal(t, "127.0.0.1", cl.Endpoint)
	assert.Equal(t, "127.0.0.1", cl.ReaderEndpoint)
	assert.Equal(t, port, cl.Port)

	for _, id := range []string{"w1", "r1"} {
		inst := f.waitInstance(t, id, "available")
		assert.Equal(t, "127.0.0.1", inst.Endpoint)
		assert.Equal(t, port, inst.Port)
	}

	assert.Equal(t, 1, f.rt.containers())

	_, err = f.b.DeleteDBCluster("c1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.rt.containers() == 0 }, engineWait, 5*time.Millisecond)
}

func TestEngineModifyMasterPassword(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.createInstance(t, "db1", "postgres", "", "first")
	f.waitInstance(t, "db1", "available")

	_, err := f.b.ModifyDBInstance("db1", "", 0,
		rds.DBInstanceOptions{MasterUserPassword: "second", ApplyImmediately: true})
	require.NoError(t, err)
	f.waitInstance(t, "db1", "available")

	_, err = f.b.ModifyDBInstance("db1", "", 0,
		rds.DBInstanceOptions{MasterUserPassword: "third", ApplyImmediately: true})
	require.NoError(t, err)

	f.mu.Lock()
	defer f.mu.Unlock()
	assert.Equal(t, []string{"first", "second"}, f.logins)
	assert.Equal(t, []string{"second", "third"}, f.passwds)
}

func TestEngineModifyWhileCreatingRejected(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)

	f.mu.Lock()
	f.probeErr = assert.AnError
	f.mu.Unlock()

	f.createInstance(t, "db1", "mysql", "", "pw")

	_, err := f.b.ModifyDBInstance("db1", "", 0, rds.DBInstanceOptions{MasterUserPassword: "new"})
	require.ErrorIs(t, err, rds.ErrInvalidDBInstanceState)
}

func TestEngineStopStartReboot(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.createInstance(t, "db1", "postgres", "", "pw")
	f.waitInstance(t, "db1", "available")

	inst, err := f.b.StopDBInstance("db1")
	require.NoError(t, err)
	assert.Equal(t, "stopping", inst.DBInstanceStatus)
	f.waitInstance(t, "db1", "stopped")

	inst, err = f.b.StartDBInstance("db1")
	require.NoError(t, err)
	assert.Equal(t, "starting", inst.DBInstanceStatus)
	f.waitInstance(t, "db1", "available")

	inst, err = f.b.RebootDBInstance("db1")
	require.NoError(t, err)
	assert.Equal(t, "rebooting", inst.DBInstanceStatus)
	f.waitInstance(t, "db1", "available")

	stops, starts := f.rt.counts()
	assert.Equal(t, 2, stops)
	assert.Equal(t, 2, starts)
	assert.Equal(t, 1, f.rt.containers())
}

func TestEngineRestoreRelaunchesEmpty(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.createInstance(t, "db1", "postgres", "", "pw")
	f.waitInstance(t, "db1", "available")
	snap := f.b.Snapshot(t.Context())

	g := newEngineFixture(t, engineWait)
	require.NoError(t, g.b.Restore(t.Context(), snap))
	g.waitInstance(t, "db1", "available")

	assert.Equal(t, 1, g.rt.containers())
	assert.Equal(t, hostPort(t, g.rt.firstSpec()), g.instance(t, "db1").Port)
}

func TestEngineUnsupportedEngineStaysMetadataOnly(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.createInstance(t, "db1", "oracle-ee", "", "pw")
	f.waitInstance(t, "db1", "available")

	assert.Equal(t, 0, f.rt.containers())
	assert.Contains(t, f.instance(t, "db1").Endpoint, "rds.amazonaws.com")
}

func TestEngineRejectsUnprovisionableUsername(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name string
		user string
	}{
		{"quote", "bad'user"},
		{"leading digit", "1abc"},
		{"too long", "abcdefghijklmnopq"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			_, err := f.b.CreateDBInstance("db1", "mysql", "db.t3.micro", "", tt.user, "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "pw"})
			require.ErrorIs(t, err, rds.ErrInvalidParameter)
			assert.Equal(t, 0, f.rt.containers())
		})
	}
}
