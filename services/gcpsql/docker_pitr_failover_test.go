package gcpsql_test

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestDockerPITRSurvivesFailover(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "pfo-pg", "pfo-clone1", "pfo-clone2")

	e := dockerEnv(t)
	ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
	defer cancel()

	runPITRFailoverFlow(ctx, t, e)
}

func runPITRFailoverFlow(ctx context.Context, t *testing.T, e *env) {
	t.Helper()

	const name = "pfo-pg"

	settings := dockerSettings(pgVersion, true)
	settings.AvailabilityType = "REGIONAL"

	op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: name, DatabaseVersion: pgVersion, Region: "us-central1", RootPassword: "root-pw", Settings: settings,
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	db := openDB(t, pgVersion, e.endpoint(t, name), "alice", "alice-pw")
	_, err = db.ExecContext(ctx, "CREATE TABLE items (id int primary key, name varchar(50))")
	require.NoError(t, err)
	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (1, 'alpha'), (2, 'beta')")
	require.NoError(t, err)

	before, err := e.svc.Projects.Instances.GetLatestRecoveryTime(project, name).Context(ctx).Do()
	require.NoError(t, err)

	beforeFailover := dbClock(t, db, pgVersion)

	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma')")
	require.NoError(t, err)

	op, err = e.svc.Instances.Failover(project, name, &sqladmin.InstancesFailoverRequest{}).Context(ctx).Do()
	e.mustWait(t, op, err)

	db = openDB(t, pgVersion, e.endpoint(t, name), "alice", "alice-pw")
	afterFailover := dbClock(t, db, pgVersion)

	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (4, 'delta')")
	require.NoError(t, err)

	after, err := e.svc.Projects.Instances.GetLatestRecoveryTime(project, name).Context(ctx).Do()
	require.NoError(t, err)
	assert.Equal(t, before.EarliestRecoveryTime, after.EarliestRecoveryTime,
		"the recovery window does not restart at the failover")

	tests := []struct {
		clone string
		at    time.Time
		want  string
		gone  string
	}{
		{clone: "pfo-clone1", at: beforeFailover, want: "2", gone: "3"},
		{clone: "pfo-clone2", at: afterFailover, want: "3", gone: "4"},
	}

	for _, tt := range tests {
		op, err = e.svc.Instances.Clone(project, name, &sqladmin.InstancesCloneRequest{
			CloneContext: &sqladmin.CloneContext{
				DestinationInstanceName: tt.clone, PointInTime: tt.at.Format(time.RFC3339Nano),
			},
		}).Context(ctx).Do()
		e.mustWait(t, op, err)

		clone := openDB(t, pgVersion, e.endpoint(t, tt.clone), "alice", "alice-pw")
		assert.Equal(t, tt.want, scalar(t, clone, itemCount), tt.clone)
		assert.Equal(t, "0", scalar(t, clone, "SELECT count(*) FROM items WHERE id = "+tt.gone), tt.clone)
	}

	for _, tt := range tests {
		op, err = e.svc.Instances.Delete(project, tt.clone).Context(ctx).Do()
		e.mustWait(t, op, err)
	}

	op, err = e.svc.Instances.Delete(project, name).Context(ctx).Do()
	e.mustWait(t, op, err)

	out, err := exec.CommandContext(ctx, "docker", "volume", "ls", "-q", "--filter",
		"name=gopherstack-gcpsql-pitr-"+project+"-"+name).Output()
	require.NoError(t, err)
	assert.Empty(t, strings.TrimSpace(string(out)), "deleting the failed-over instance removes its log archive")
}
