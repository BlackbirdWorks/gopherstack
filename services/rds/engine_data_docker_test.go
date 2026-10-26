package rds_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

func awaitDockerInstance(t *testing.T, b *rds.InMemoryBackend, id string) rds.DBInstance {
	t.Helper()

	var inst rds.DBInstance

	require.Eventually(t, func() bool {
		got, err := b.DescribeDBInstances(id)
		require.NoError(t, err)
		inst = got[0]
		requireNotFailed(t, b, id, inst.DBInstanceStatus)

		return inst.DBInstanceStatus == "available"
	}, dockerEngineWait, 500*time.Millisecond)

	return inst
}

func TestEngineDockerSnapshotReplicaAndPortChange(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		engine string
	}{
		{name: "postgres", engine: "postgres"},
		{name: "mysql", engine: "mysql"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b, rec := newDockerEngineBackend(t)

			_, err := b.CreateDBInstance("src", tt.engine, "db.t3.micro", "appdb", "master", "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "firstpass1"})
			require.NoError(t, err)

			src := awaitDockerInstance(t, b, "src")
			db := openEngineDB(t, tt.engine, src, "firstpass1")
			_, err = db.ExecContext(dbCtx(t), createItems)
			require.NoError(t, err)
			_, err = db.ExecContext(dbCtx(t), insertItem)
			require.NoError(t, err)

			_, err = b.CreateDBSnapshot("snap1", "src")
			require.NoError(t, err)

			_, err = b.RestoreDBInstanceFromDBSnapshot("restored", "snap1", rds.DBInstanceOptions{})
			require.NoError(t, err)

			_, err = b.CreateDBInstanceReadReplica("replica", "src", "", "", "", rds.DBInstanceOptions{})
			require.NoError(t, err)

			for _, id := range []string{"restored", "replica"} {
				inst := awaitDockerInstance(t, b, id)
				assert.NotEqual(t, src.Port, inst.Port)

				var name string

				copyDB := openEngineDB(t, tt.engine, inst, "firstpass1")
				require.NoError(t, copyDB.QueryRowContext(dbCtx(t),
					"SELECT name FROM items WHERE id = 1").Scan(&name), id)
				assert.Equal(t, "alpha", name, id)
			}

			newPort := freePort(t)
			_, err = b.ModifyDBInstance("src", "", 0, rds.DBInstanceOptions{DBPortNumber: newPort})
			require.NoError(t, err)

			moved := src
			moved.Port = newPort

			var name string

			require.NoError(t, openEngineDB(t, tt.engine, moved, "firstpass1").QueryRowContext(dbCtx(t),
				"SELECT name FROM items WHERE id = 1").Scan(&name))
			assert.Equal(t, "alpha", name)

			ids := rec.containerIDs()
			require.Len(t, ids, 3)

			for _, id := range []string{"restored", "replica", "src"} {
				_, err = b.DeleteDBInstanceWithOptions(id, true, "", true)
				require.NoError(t, err)
			}

			for _, id := range ids {
				require.Eventually(t, func() bool { return !dockerContainerExists(t, id) },
					time.Minute, 250*time.Millisecond)
			}
		})
	}
}
