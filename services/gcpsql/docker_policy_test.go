package gcpsql_test

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	sqladmin "google.golang.org/api/sqladmin/v1"
)

func TestDockerPasswordPolicyChangesEnginePassword(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "dpol-pg", "dpol-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			name := "dpol-" + shortKind(version)
			s := dockerSettings(version, false)
			s.PasswordValidationPolicy = &sqladmin.PasswordValidationPolicy{
				EnablePasswordPolicy: true, MinLength: 10, Complexity: "COMPLEXITY_DEFAULT", ReuseInterval: 2,
				DisallowUsernameSubstring: true,
			}

			op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
				Name: name, DatabaseVersion: version, Region: "us-central1", RootPassword: "Root-pw-1234!", Settings: s,
			}).Context(ctx).Do()
			e.mustWait(t, op, err)

			op, err = e.svc.Databases.Insert(project, name, &sqladmin.Database{Name: "app"}).Context(ctx).Do()
			e.mustWait(t, op, err)

			const first, second = "First-Pw-1234!", "Second-Pw-5678!"

			op, err = e.svc.Users.Insert(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: first}).
				Context(ctx).Do()
			e.mustWait(t, op, err)

			assert.Equal(t, "1", scalar(t, openDB(t, version, e.endpoint(t, name), "alice", first), "SELECT 1"))

			_, err = e.svc.Users.Update(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: "weak"}).
				Host("%").Name("alice").Context(ctx).Do()
			require.Error(t, err, "a weak password is rejected before it reaches the engine")

			upd := func(pw string) error {
				o, uerr := e.svc.Users.Update(project, name, &sqladmin.User{Name: "alice", Host: "%", Password: pw}).
					Host("%").Name("alice").Context(ctx).Do()
				if uerr != nil {
					return uerr
				}

				e.mustWait(t, o, nil)

				return nil
			}

			require.Error(t, upd(first), "the current password counts as used")
			require.NoError(t, upd(second))

			assert.Equal(t, "1", scalar(t, openDB(t, version, e.endpoint(t, name), "alice", second), "SELECT 1"))

			old := openDB(t, version, e.endpoint(t, name), "alice", first)
			assert.Error(t, old.PingContext(ctx), "the previous password no longer logs in")
		})
	}
}

func TestDockerScheduledBackupIsRestorable(t *testing.T) {
	t.Parallel()
	assertNoContainers(t, "dsch-pg", "dsch-my")

	for _, version := range dockerVersions() {
		t.Run(version, func(t *testing.T) {
			t.Parallel()

			e := dockerEnv(t)
			ctx, cancel := context.WithTimeout(t.Context(), dockerWait)
			defer cancel()

			name := "dsch-" + shortKind(version)
			s := dockerSettings(version, false)
			s.BackupConfiguration = &sqladmin.BackupConfiguration{
				Enabled: true, StartTime: "03:00", BinaryLogEnabled: version == mysqlVersion,
			}

			op, err := e.svc.Instances.Insert(project, &sqladmin.DatabaseInstance{
				Name: name, DatabaseVersion: version, Region: "us-central1", RootPassword: "root-pw", Settings: s,
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

			require.Equal(t, 1, e.backend.RunScheduledBackups(time.Now().UTC().Truncate(24*time.Hour).Add(4*time.Hour)))
			e.backend.Drain()

			runs, err := e.svc.BackupRuns.List(project, name).Context(ctx).Do()
			require.NoError(t, err)
			require.Len(t, runs.Items, 1)
			require.Equal(t, "AUTOMATED", runs.Items[0].Type)
			require.Equal(t, "SUCCESSFUL", runs.Items[0].Status)

			_, err = db.ExecContext(ctx, "DELETE FROM items")
			require.NoError(t, err)

			op, err = e.svc.Instances.RestoreBackup(project, name, &sqladmin.InstancesRestoreBackupRequest{
				RestoreBackupContext: &sqladmin.RestoreBackupContext{BackupRunId: runs.Items[0].Id},
			}).Context(ctx).Do()
			e.mustWait(t, op, err)

			restored := openDB(t, version, e.endpoint(t, name), "alice", "alice-pw")
			assert.Equal(t, "2", scalar(t, restored, "SELECT count(*) FROM items"))
		})
	}
}
