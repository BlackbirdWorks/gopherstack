package gcpsql_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestDockerProjectBackups(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "dbk-pg", "dbk2-pg", "dbk-my", "dbk2-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			runProjectBackupFlow(ctx, t, e, version)
		})
	}
}

func runProjectBackupFlow(ctx context.Context, t *testing.T, e *env, version string) {
	t.Helper()

	name, other := "dbk-"+shortKind(version), "dbk2-"+shortKind(version)
	db := seedInstance(ctx, t, e, name, version, false)

	op, err := e.svc.Backups.CreateBackup("projects/"+project, &sqladmin.Backup{Instance: name, Description: "keep"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	list, err := e.svc.Backups.ListBackups("projects/" + project).Filter("instance:" + name).Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, list.Backups, 1)
	assert.Equal(t, "SUCCESSFUL", list.Backups[0].State)
	assert.Positive(t, list.Backups[0].MaxChargeableBytes)

	_, err = db.ExecContext(ctx, "INSERT INTO items VALUES (3, 'gamma')")
	require.NoError(t, err)
	assert.Equal(t, "3", scalar(t, db, itemCount))

	op, err = e.svc.Instances.RestoreBackup(project, name, &sqladmin.InstancesRestoreBackupRequest{
		Backup: list.Backups[0].Name,
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	db = openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, db, itemCount), "the restore returns the instance to the backup")

	op, err = e.svc.Instances.Delete(project, name).EnableFinalBackup(true).FinalBackupTtlDays(7).Context(ctx).Do()
	e.mustWait(t, op, err)

	finals, err := e.svc.Backups.ListBackups("projects/" + project).Filter("type:FINAL").Context(ctx).Do()
	require.NoError(t, err)
	require.Len(t, finals.Backups, 1, "the final backup outlives the instance")

	op, err = e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
		Name: other, DatabaseVersion: version, Region: "us-central1", RootPassword: "root-pw",
		Settings: dockerSettings(version, false),
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Users.Insert(project, other, &sqladmin.User{Name: "alice", Host: "%", Password: "alice-pw"}).
		Context(ctx).Do()
	e.mustWait(t, op, err)

	op, err = e.svc.Instances.RestoreBackup(project, other, &sqladmin.InstancesRestoreBackupRequest{
		Backup: finals.Backups[0].Name,
	}).Context(ctx).Do()
	e.mustWait(t, op, err)

	restored := openDB(t, version, e.endpoint(t, other), "alice", "alice-pw")
	assert.Equal(t, "2", scalar(t, restored, itemCount), "a final backup restores into another instance")
}
