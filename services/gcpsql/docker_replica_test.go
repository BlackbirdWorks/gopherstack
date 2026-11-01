package gcpsql_test

import (
	"context"
	"database/sql"
	"fmt"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

const (
	pgVersion    = "POSTGRES_16"
	mysqlVersion = "MYSQL_8_4"
	itemCount    = "SELECT count(*) FROM items"
)

func shortKind(version string) string {
	if version == mysqlVersion {
		return "my"
	}

	return "pg"
}

func dockerVersions() []string { return []string{pgVersion, mysqlVersion} }

func dockerSettings(version string, pitr bool) *sqladmin.Settings {
	s := &sqladmin.Settings{Tier: "db-custom-1-3840"}

	if version == mysqlVersion {
		s.BackupConfiguration = &sqladmin.BackupConfiguration{Enabled: true, BinaryLogEnabled: true}

		return s
	}

	if pitr {
		s.BackupConfiguration = &sqladmin.BackupConfiguration{Enabled: true, PointInTimeRecoveryEnabled: true}
	}

	return s
}

// seedInstance creates an instance with database app, user alice and a table holding rows 1 and 2.
func seedInstance(ctx context.Context, t *testing.T, e *env, name, version string, pitr bool) *sql.DB {
	t.Helper()

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: version, Region: "us-central1", RootPassword: "root-pw",
		Settings: dockerSettings(version, pitr),
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	db := openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	_, err = db.ExecContext(ctx, "CREATE TABLE items (id int primary key, name varchar(50))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
	require.NoError(t, err)

	return db
}

func eventuallyCount(t *testing.T, db *sql.DB, want string, msg string) {
	t.Helper()

	require.Eventually(t, func() bool {
		var n string

		return db.QueryRowContext(t.Context(), itemCount).Scan(&n) == nil && n == want
	}, 90*time.Second, 250*time.Millisecond, msg)
}

func TestDockerReadReplica(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "rprim-pg", "rrep-pg", "rprim-my", "rrep-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runReplicaFlow(ctx, t, e, version)
		})
	}
}

func runReplicaFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	prim, rep := "rprim-"+shortKind(version), "rrep-"+shortKind(version)

	primary := seedInstance(ctx, t, e, prim, version, false)

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: rep, DatabaseVersion: version, MasterInstanceName: prim,
		Settings: &sqladmin.Settings{Tier: "db-custom-1-3840"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	inst, err := e.svc.Instances.Get(project, rep).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "READ_REPLICA_INSTANCE", inst.InstanceType)

	replica := openDB(t, version, e.endpoint(t, rep), "alice", "alice-pw")
	eventuallyCount(t, replica, "2", "the replica starts with the primary's rows")

	_, err = replica.ExecContext(ctx, "INSERT INTO items VALUES (99, 'nope')")
	require.Error(t, err, "a replica rejects writes")

	_, err = primary.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma')")
	require.NoError(t, err)
	eventuallyCount(t, replica, "3", "a write on the primary becomes readable on the replica")

	op, err = e.svc.Users.Insert(project, prim, &sqladmin.User{Name: "bob", Host: "%", Password: "bob-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	require.Eventually(t, func() bool {
		return openDB(t, version, e.endpoint(t, rep), "bob", "bob-pw").PingContext(ctx) == nil
	}, 90*time.Second, 250*time.Millisecond, "an account created on the primary can log in to the replica")

	replicating, _, err := e.backend.ReplicaStatus(ctx, project, rep)
	require.NoError(t, err)
	assert.True(t, replicating)

	stopStartReplica(ctx, t, e, rep, primary, replica)

	op, err = e.svc.Instances.PromoteReplica(project, rep).Context(ctx).Do()
	e.mustWait(t, op, err)

	_, err = replica.ExecContext(ctx, "INSERT INTO items VALUES (50, 'solo')")
	require.NoError(t, err, "a promoted replica accepts writes")

	prom, err := e.svc.Instances.Get(project, rep).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, "CLOUD_SQL_INSTANCE", prom.InstanceType)

	pinst, err := e.svc.Instances.Get(project, prim).Context(ctx).Do()
	require.NoError(t, err)
	assert.Empty(t, pinst.ReplicaNames)
}

func stopStartReplica(ctx context.Context, t *testing.T, e *env, rep string, primary, replica *sql.DB) {
	t.Helper()

	op, err := e.svc.Instances.StopReplica(project, rep).Context(ctx).Do()
	e.mustWait(t, op, err)

	require.Eventually(t, func() bool {
		ok, _, serr := e.backend.ReplicaStatus(ctx, project, rep)

		return serr == nil && !ok
	}, 60*time.Second, 250*time.Millisecond, "stopReplica ends replication")

	_, err = primary.ExecContext(ctx, "INSERT INTO items VALUES (4, 'delta')")
	require.NoError(t, err)
	assert.Equal(t, "3", scalar(t, replica, itemCount), "a stopped replica does not follow the primary")

	op, err = e.svc.Instances.StartReplica(project, rep).Context(ctx).Do()
	e.mustWait(t, op, err)
	eventuallyCount(t, replica, "4", "startReplica catches the replica up")
}

func TestDockerPointInTimeRecovery(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "psrc-pg", "pclone-pg", "psrc-my", "pclone-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runPITRFlow(ctx, t, e, version)
		})
	}
}

func dbClock(t *testing.T, db *sql.DB, version string) time.Time {
	t.Helper()

	query := `SELECT to_char(clock_timestamp() AT TIME ZONE 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"')`
	if version == mysqlVersion {
		query = `SELECT DATE_FORMAT(UTC_TIMESTAMP(6), '%Y-%m-%dT%H:%i:%s.%fZ')`
	}

	at, err := time.Parse(time.RFC3339Nano, scalar(t, db, query))
	require.NoError(t, err)

	return at
}

func runPITRFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	db := seedInstance(ctx, t, e, "psrc-"+shortKind(version), version, true)

	win, err := e.svc.Projects.Instances.GetLatestRecoveryTime(project, "psrc-"+shortKind(version)).Context(ctx).Do()
	require.NoError(t, err)
	assert.NotEmpty(t, win.EarliestRecoveryTime)

	at := dbClock(t, db, version)

	if version == mysqlVersion {
		require.Eventually(t, func() bool {
			return dbClock(t, db, version).Unix() > at.Unix()
		}, 10*time.Second, 50*time.Millisecond, "binary log timestamps have one second resolution")
	}

	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (3, 'after')")
	require.NoError(t, err)

	op, err := e.svc.Instances.Clone(project, "psrc-"+shortKind(version), &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{
			DestinationInstanceName: "pclone-" + shortKind(version), PointInTime: at.Format(time.RFC3339Nano),
		},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	clone := openDB(t, version, e.endpoint(t, "pclone-"+shortKind(version)), "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, clone, itemCount), "the clone has only the rows committed before the point in time")
	assert.Equal(t, "0", scalar(t, clone, "SELECT count(*) FROM items WHERE id = 3"))

	assert.Equal(t, "3", scalar(t, db, itemCount), "the source keeps its later row")

	_, err = e.svc.Instances.Clone(project, "psrc-"+shortKind(version), &sqladmin.InstancesCloneRequest{
		CloneContext: &sqladmin.CloneContext{
			DestinationInstanceName: "pfuture-" + shortKind(
				version,
			), PointInTime: time.Now().Add(time.Hour).UTC().Format(time.RFC3339Nano),
		},
	}).Context(ctx).Do()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "after the latest recovery time")
}

func TestDockerEnginePersistsAcrossRestart(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "keep-pg", "keep-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			seedInstance(ctx, t, e, "keep-"+shortKind(version), version, false)
			snap := e.backend.Snapshot(ctx)
			e.backend.Close()

			h := newDockerHandler(t)
			t.Cleanup(h.Backend.Close)
			t.Cleanup(h.Backend.Reset)
			require.NoError(t, h.Backend.Restore(ctx, snap))

			require.Eventually(t, func() bool {
				in, err := h.Backend.GetInstance(project, "keep-"+shortKind(version))

				return err == nil && in.State == "RUNNABLE"
			}, dockerWait, 250*time.Millisecond)

			in, err := h.Backend.GetInstance(project, "keep-"+shortKind(version))
			require.NoError(t, err)

			db := openDB(t, version, in.GopherstackEndpoint, "alice", "alice-pw")
			assert.Equal(t, "alpha", scalar(t, db, "SELECT name FROM items WHERE id = 1"),
				"rows survive the restart, they are not rebuilt from records")
		})
	}
}

func TestDockerTierLimitsReachContainer(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "lim")

	e := dockerEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
	defer cancel()

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: "lim", DatabaseVersion: pgVersion, RootPassword: "root-pw",
		Settings: &sqladmin.Settings{Tier: "db-custom-1-2048"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	out := dockerInspect(t, "{{.HostConfig.Memory}} {{.HostConfig.NanoCpus}}", "gopherstack-gcpsql-lim-")
	assert.Equal(t, fmt.Sprintf("%d 1000000000", int64(2048)<<20), out)
}

func dockerInspect(t *testing.T, format, namePrefix string) string {
	t.Helper()

	names := dockerOut(t, "ps", "-q", "--filter", "name="+namePrefix)
	require.NotEmpty(t, names, "no container named %s*", namePrefix)

	return dockerOut(t, "inspect", "-f", format, strings.Fields(names)[0])
}

func dockerOut(t *testing.T, args ...string) string {
	t.Helper()

	out, err := exec.CommandContext(t.Context(), "docker", args...).Output()
	require.NoError(t, err)

	return strings.TrimSpace(string(out))
}
