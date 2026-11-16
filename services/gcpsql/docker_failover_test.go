package gcpsql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func regionalDockerSettings(version string) *sqladmin.Settings {
	s := dockerSettings(version, false)
	s.AvailabilityType = "REGIONAL"

	return s
}

func TestDockerFailover(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "hafo-pg", "hafo-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runFailoverFlow(ctx, t, e, version)
		})
	}
}

func seedRegional(ctx context.Context, t *testing.T, e *env, name, version string) {
	t.Helper()

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: version, Region: "us-central1", RootPassword: "root-pw",
		Settings: regionalDockerSettings(version),
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)
}

func runFailoverFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	name := "hafo-" + shortKind(version)
	seedRegional(ctx, t, e, name, version)

	db := openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	_, err := db.ExecContext(ctx, "CREATE TABLE items (id int primary key, name varchar(50))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
	require.NoError(t, err)

	before := e.endpoint(t, name)

	op, err := e.svc.Instances.Failover(project, name, &sqladmin.InstancesFailoverRequest{}).Context(ctx).Do()
	e.mustWait(t, op, err)

	after := e.endpoint(t, name)
	assert.Equal(t, before, after, "clients keep the same address across a failover")

	db = openDB(t, version, after, "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, db, itemCount), "rows written before the failover are present")

	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma')")
	require.NoError(t, err, "the promoted engine accepts writes")

	op, err = e.svc.Instances.Failover(project, name, &sqladmin.InstancesFailoverRequest{}).Context(ctx).Do()
	e.mustWait(t, op, err)

	db = openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	assert.Equal(t, "3", scalar(t, db, itemCount), "a second failover keeps every row")
}

func TestDockerSwitchover(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "hasw-pg", "hasw-my", "hasw-pg-r", "hasw-my-r", "hasw-pg-rs", "hasw-my-rs")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runSwitchoverFlow(ctx, t, e, version)
		})
	}
}

func runSwitchoverFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	prim, rep := "hasw-"+shortKind(version), "hasw-"+shortKind(version)+"-r"
	seedInstance(ctx, t, e, prim, version, false)

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: rep, DatabaseVersion: version, MasterInstanceName: prim,
		Settings: &sqladmin.Settings{Tier: "db-custom-1-3840"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	sib := rep + "s"

	op, err = e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: sib, DatabaseVersion: version, MasterInstanceName: prim,
		Settings: &sqladmin.Settings{Tier: "db-custom-1-3840"},
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Instances.Switchover(project, rep).Context(ctx).Do()
	e.mustWait(t, op, err)

	promoted := openDB(t, version, e.endpoint(t, rep), "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, promoted, itemCount), "the promoted replica holds the data")

	_, err = promoted.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma')")
	require.NoError(t, err, "the promoted replica accepts writes")

	rejoined := openDB(t, version, e.endpoint(t, prim), "alice", "alice-pw")
	eventuallyCount(t, rejoined, "3", "the old primary follows the new one")

	_, err = rejoined.ExecContext(ctx, "INSERT INTO items VALUES (4, 'delta')")
	require.Error(t, err, "the old primary is read-only now")

	sibling := openDB(t, version, e.endpoint(t, sib), "alice", "alice-pw")
	eventuallyCount(t, sibling, "3", "the other replica follows the new primary")
}

func TestDockerCascadingReplica(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "hacs-pg", "hacs-my", "hacs-pg-m", "hacs-my-m", "hacs-pg-l", "hacs-my-l")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			prim := "hacs-" + shortKind(version)
			mid, leaf := prim+"-m", prim+"-l"
			primary := seedInstance(ctx, t, e, prim, version, false)

			for _, r := range [][2]string{{mid, prim}, {leaf, mid}} {
				op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
					Name: r[0], DatabaseVersion: version, MasterInstanceName: r[1],
					Settings: dockerSettings(version, false),
				}).Context(ctx).Do()
				e.mustWait(t, op, err)
			}

			leafDB := openDB(t, version, e.endpoint(t, leaf), "alice", "alice-pw")
			eventuallyCount(t, leafDB, "2", "data reaches the second-level replica")

			_, err := primary.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma')")
			require.NoError(t, err)
			eventuallyCount(t, leafDB, "3", "new writes flow through the intermediate replica")
		})
	}
}
