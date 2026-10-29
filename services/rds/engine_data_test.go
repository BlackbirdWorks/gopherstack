package rds_test

import (
	"context"
	"encoding/base64"
	"net"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func (f *fakeEngineRuntime) ExecEnv(
	ctx context.Context, id string, cmd, env []string,
) (container.ExecResult, error) {
	f.mu.Lock()
	f.envs = append(f.envs, slices.Clone(env))
	f.mu.Unlock()

	return f.Exec(ctx, id, cmd)
}

func (f *fakeEngineRuntime) Exec(_ context.Context, id string, cmd []string) (container.ExecResult, error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.execs = append(f.execs, slices.Clone(cmd))

	if f.execErr != nil {
		return container.ExecResult{}, f.execErr
	}

	switch {
	case f.dumpText != "" && (slices.Contains(cmd, "pg_dump") || slices.Contains(cmd, "mysqldump") ||
		slices.Contains(cmd, "mariadb-dump")):
		return container.ExecResult{Stdout: f.dumpText}, nil
	case slices.Contains(cmd, "pg_dump"):
		return container.ExecResult{Stdout: "pgdump-of-" + id}, nil
	case slices.Contains(cmd, "SHOW DATABASES"):
		return container.ExecResult{Stdout: "appdb\nmysql\nsys\n"}, nil
	case slices.Contains(cmd, "mysqldump"), slices.Contains(cmd, "mariadb-dump"):
		return container.ExecResult{Stdout: "sqldump-of-" + id}, nil
	case len(cmd) == 5 && cmd[0] == "sh":
		f.staged[cmd[3]] += cmd[4]
	case slices.Contains(cmd, "sh"):
		staged, isLoad := f.staged[cmd[slices.Index(cmd, "-c")+2]]
		if !isLoad {
			break
		}

		raw, err := base64.StdEncoding.DecodeString(staged)
		if err != nil {
			return container.ExecResult{ExitCode: 1}, nil
		}

		f.loaded[id] = string(raw)
	}

	return container.ExecResult{}, nil
}

func (f *fakeEngineRuntime) setDump(text string) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.dumpText = text
}

func (f *fakeEngineRuntime) loadedBy(id string) string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.loaded[id]
}

func (f *fakeEngineRuntime) secondLoaded() string {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.loaded["ctr-2"]
}

func (f *fakeEngineRuntime) execSeen(want ...string) bool {
	f.mu.Lock()
	defer f.mu.Unlock()

	return slices.ContainsFunc(f.execs, func(cmd []string) bool {
		return !slices.ContainsFunc(want, func(w string) bool { return !slices.Contains(cmd, w) })
	})
}

func (f *fakeEngineRuntime) spec(i int) container.Spec {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.specs[i]
}

type dataCase struct {
	name     string
	engine   string
	dumper   string
	wantPass string
	dump     string
}

func dataCases() []dataCase {
	const (
		pgPass    = "POSTGRES_PASSWORD=pw"
		mysqlPass = "MYSQL_ROOT_PASSWORD=pw"
		pgDump    = "pgdump-of-ctr-1"
		sqlDump   = "sqldump-of-ctr-1"
	)

	return []dataCase{
		{name: "postgres", engine: "postgres", dumper: "pg_dump", wantPass: pgPass, dump: pgDump},
		{name: "mysql", engine: "mysql", dumper: "mysqldump", wantPass: mysqlPass, dump: sqlDump},
		{name: "mariadb", engine: "mariadb", dumper: "mariadb-dump", wantPass: mysqlPass, dump: sqlDump},
	}
}

func (f *engineFixture) readySource(t *testing.T, engine string) {
	t.Helper()
	f.createInstance(t, "src", engine, "", "pw")
	f.waitInstance(t, "src", "available")
}

func TestEngineSnapshotRestoreCarriesData(t *testing.T) {
	t.Parallel()

	for _, tt := range dataCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.readySource(t, tt.engine)

			_, err := f.b.CreateDBSnapshot("snap1", "src")
			require.NoError(t, err)
			assert.True(t, f.rt.execSeen(tt.dumper))

			_, err = f.b.RestoreDBInstanceFromDBSnapshot("copy", "snap1", rds.DBInstanceOptions{})
			require.NoError(t, err)

			got := f.waitInstance(t, "copy", "available")
			assert.Equal(t, "appdb", got.DBName)
			assert.Equal(t, tt.dump, f.rt.secondLoaded())
			assert.Subset(t, f.rt.spec(1).Env, []string{tt.wantPass})
			assert.Equal(t, 2, f.rt.containers())
		})
	}
}

func TestEngineExecPasswordInEnvNotArgv(t *testing.T) {
	t.Parallel()

	for _, tt := range dataCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.readySource(t, tt.engine)

			_, err := f.b.CreateDBSnapshot("snap1", "src")
			require.NoError(t, err)

			wantEnv := "MYSQL_PWD=pw"
			if tt.engine == "postgres" {
				wantEnv = "PGPASSWORD=pw"
			}

			f.rt.mu.Lock()
			defer f.rt.mu.Unlock()

			assert.Contains(t, f.rt.envs, []string{wantEnv})

			for _, cmd := range f.rt.execs {
				for _, a := range cmd {
					assert.NotContains(t, a, "MYSQL_PWD")
					assert.NotContains(t, a, "PGPASSWORD")
				}
			}
		})
	}
}

func TestEngineReadReplicaGetsOwnEngineWithData(t *testing.T) {
	t.Parallel()

	for _, tt := range dataCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.readySource(t, tt.engine)

			rep, err := f.b.CreateDBInstanceReadReplica("rep", "src", "", "", "", rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "creating", rep.DBInstanceStatus)

			got := f.waitInstance(t, "rep", "available")
			assert.Equal(t, "127.0.0.1", got.Endpoint)
			assert.Equal(t, 2, f.rt.containers())
			assert.Equal(t, f.rt.spec(0).Image, f.rt.spec(1).Image)
			assert.Subset(t, f.rt.spec(1).Env, []string{tt.wantPass})
			assert.Equal(t, tt.dump, f.rt.secondLoaded())
			assert.NotEqual(t, hostPort(t, f.rt.spec(0)), hostPort(t, f.rt.spec(1)))

			_, err = f.b.DeleteDBInstanceWithOptions("rep", true, "", true)
			require.NoError(t, err)
			require.Eventually(t, func() bool { return f.rt.containers() == 1 }, engineWait, 5*time.Millisecond)
		})
	}
}

func TestEnginePointInTimeRestoreCopiesSourceData(t *testing.T) {
	t.Parallel()

	for _, tt := range dataCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.readySource(t, tt.engine)

			got, err := f.b.RestoreDBInstanceToPointInTime("pitr", "src", rds.DBInstanceOptions{})
			require.NoError(t, err)
			assert.Equal(t, "creating", got.DBInstanceStatus)

			f.waitInstance(t, "pitr", "available")
			assert.Equal(t, tt.dump, f.rt.secondLoaded())
			assert.Subset(t, f.rt.spec(1).Env, []string{tt.wantPass})
		})
	}
}

func TestEngineFinalSnapshotKeepsData(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.readySource(t, "postgres")

	_, err := f.b.DeleteDBInstanceWithOptions("src", false, "final1", true)
	require.NoError(t, err)

	_, err = f.b.RestoreDBInstanceFromDBSnapshot("again", "final1", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitInstance(t, "again", "available")
	assert.Equal(t, "pgdump-of-ctr-1", f.rt.secondLoaded())
}

func TestEngineCopiedSnapshotKeepsData(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.readySource(t, "postgres")

	_, err := f.b.CreateDBSnapshot("snap1", "src")
	require.NoError(t, err)
	_, err = f.b.CopyDBSnapshot("snap1", "snap2", rds.CopyDBSnapshotOptions{})
	require.NoError(t, err)
	_, err = f.b.DeleteDBSnapshot("snap1")
	require.NoError(t, err)

	_, err = f.b.RestoreDBInstanceFromDBSnapshot("copy", "snap2", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitInstance(t, "copy", "available")
	assert.Equal(t, "pgdump-of-ctr-1", f.rt.secondLoaded())
}

type noExecRuntime struct{ rds.EngineRuntime }

func TestEngineRestoreWithoutExecIsEmpty(t *testing.T) {
	t.Parallel()

	rt := newFakeEngineRuntime()
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	t.Cleanup(b.Close)
	b.EnableEngine(rds.EngineConfig{
		Runtime: noExecRuntime{rt}, StartTimeout: engineWait,
		Probe: func(context.Context, rds.EngineLogin) error { return nil },
	})

	_, err := b.CreateDBInstance("src", "postgres", "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "pw"})
	require.NoError(t, err)
	waitStatus(t, b, "src", "available")

	_, err = b.CreateDBSnapshot("snap1", "src")
	require.NoError(t, err)
	_, err = b.RestoreDBInstanceFromDBSnapshot("copy", "snap1", rds.DBInstanceOptions{})
	require.NoError(t, err)
	waitStatus(t, b, "copy", "available")

	assert.Empty(t, rt.secondLoaded())
	assert.Subset(t, rt.spec(1).Env, []string{"POSTGRES_PASSWORD=pw", "POSTGRES_DB=appdb"})
	assert.Equal(t, 2, rt.containers())
}

func TestEngineRestoreWhenExecUnsupportedKeepsCredentials(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.rt.execErr = container.ErrExecUnsupported
	f.readySource(t, "postgres")

	_, err := f.b.CreateDBSnapshot("snap1", "src")
	require.NoError(t, err)
	_, err = f.b.RestoreDBInstanceFromDBSnapshot("copy", "snap1", rds.DBInstanceOptions{})
	require.NoError(t, err)
	f.waitInstance(t, "copy", "available")

	assert.Empty(t, f.rt.secondLoaded())
	assert.Subset(t, f.rt.spec(1).Env, []string{"POSTGRES_PASSWORD=pw"})
}

func waitStatus(t *testing.T, b *rds.InMemoryBackend, id, status string) {
	t.Helper()

	require.Eventually(t, func() bool {
		got, err := b.DescribeDBInstances(id)

		return err == nil && got[0].DBInstanceStatus == status
	}, engineWait, 5*time.Millisecond)
}

func (f *engineFixture) setProbeErr(err error) {
	f.mu.Lock()
	defer f.mu.Unlock()

	f.probeErr = err
}

func (f *engineFixture) readyCluster(t *testing.T, id string) {
	t.Helper()

	_, err := f.b.CreateDBCluster(id, "aurora-mysql", "master", "appdb", "", 0, nil,
		rds.DBClusterOptions{MasterUserPassword: "pw", EngineVersion: "8.0.mysql_aurora.3.05.2"})
	require.NoError(t, err)
	require.Eventually(t, func() bool { return f.cluster(t, id).Status == "available" },
		engineWait, 5*time.Millisecond)
}

func TestEngineClusterSnapshotAndPointInTimeRestore(t *testing.T) {
	t.Parallel()

	tests := []struct {
		restore func(f *engineFixture) error
		name    string
	}{
		{name: "snapshot", restore: func(f *engineFixture) error {
			if _, err := f.b.CreateDBClusterSnapshot("csnap", "c1"); err != nil {
				return err
			}
			_, err := f.b.RestoreDBClusterFromSnapshot("c2", "csnap", "", rds.DBClusterOptions{})

			return err
		}},
		{name: "point in time", restore: func(f *engineFixture) error {
			_, err := f.b.RestoreDBClusterToPointInTime("c2", "c1", rds.DBClusterOptions{})

			return err
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.readyCluster(t, "c1")

			f.setProbeErr(assert.AnError)
			require.NoError(t, tt.restore(f))
			assert.Equal(t, "creating", f.cluster(t, "c2").Status)
			f.setProbeErr(nil)

			require.Eventually(t, func() bool { return f.cluster(t, "c2").Status == "available" },
				engineWait, 5*time.Millisecond)
			assert.Equal(t, 2, f.rt.containers())
			assert.Equal(t, "sqldump-of-ctr-1", f.rt.secondLoaded())
			assert.Equal(t, "appdb", f.cluster(t, "c2").DatabaseName)
			assert.Subset(t, f.rt.spec(1).Env, []string{"MYSQL_ROOT_PASSWORD=pw"})
		})
	}
}

func freePort(t *testing.T) int {
	t.Helper()

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)

	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())

	return port
}

func dialable(t *testing.T, port int) bool {
	t.Helper()

	c, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp",
		net.JoinHostPort("127.0.0.1", strconv.Itoa(port)))
	if err != nil {
		return false
	}

	_ = c.Close()

	return true
}

func TestEngineModifyDBPortNumberRebindsEndpoint(t *testing.T) {
	t.Parallel()

	for _, tt := range dataCases() {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			f := newEngineFixture(t, engineWait)
			f.readySource(t, tt.engine)

			oldPort := f.instance(t, "src").Port
			newPort := freePort(t)

			_, err := f.b.ModifyDBInstance("src", "", 0, rds.DBInstanceOptions{DBPortNumber: newPort})
			require.NoError(t, err)
			assert.Equal(t, newPort, f.instance(t, "src").Port)
			assert.True(t, dialable(t, newPort))
			assert.Equal(t, 1, f.rt.containers())

			if tt.engine == "postgres" {
				require.Eventually(t, func() bool { return !dialable(t, oldPort) }, engineWait, 5*time.Millisecond)
			}
		})
	}
}

func TestEngineModifyDBPortNumberRejectsBusyPort(t *testing.T) {
	t.Parallel()

	f := newEngineFixture(t, engineWait)
	f.readySource(t, "postgres")

	oldPort := f.instance(t, "src").Port

	l, err := (&net.ListenConfig{}).Listen(t.Context(), "tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = l.Close() })

	busy := l.Addr().(*net.TCPAddr).Port

	_, err = f.b.ModifyDBInstance("src", "", 0, rds.DBInstanceOptions{DBPortNumber: busy})
	require.ErrorIs(t, err, rds.ErrInvalidParameter)
	assert.Equal(t, oldPort, f.instance(t, "src").Port)
	assert.True(t, dialable(t, oldPort))
}
