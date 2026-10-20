package rds_test

import (
	"database/sql"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

const replicaPoll = 500 * time.Millisecond

type replicaDockerCase struct {
	name        string
	engine      string
	kind        string
	readOnlyMsg string
}

func replicaDockerCases() []replicaDockerCase {
	return []replicaDockerCase{
		{name: "postgres", engine: "postgres", kind: "postgres", readOnlyMsg: "read-only transaction"},
		{name: "mysql", engine: "mysql", kind: "mysql", readOnlyMsg: "read-only"},
		{name: "mariadb", engine: "mariadb", kind: "mysql", readOnlyMsg: "read-only"},
	}
}

func newDockerReplicaBackend(t *testing.T) (*rds.InMemoryBackend, *recordingEngineRuntime) {
	t.Helper()

	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker CLI not available")
	}

	rt, err := container.NewRuntime(container.Config{})
	if err != nil {
		t.Skipf("container runtime unavailable: %v", err)
	}

	er, ok := rt.(rds.EngineRuntime)
	if !ok {
		t.Skip("container runtime cannot stop/start containers")
	}

	if err = rt.Ping(t.Context()); err != nil {
		t.Skipf("container daemon unreachable: %v", err)
	}

	rec := &recordingEngineRuntime{EngineRuntime: er}
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	b.EnableEngine(rds.EngineConfig{Runtime: rec, ReplicaPollInterval: replicaPoll, RestorePointInterval: -1})
	t.Cleanup(b.Close)

	return b, rec
}

func countItems(t *testing.T, db *sql.DB) (int, error) {
	t.Helper()

	var n int

	err := db.QueryRowContext(t.Context(), "SELECT count(*) FROM items").Scan(&n)

	return n, err
}

func awaitItems(t *testing.T, db *sql.DB, want int) {
	t.Helper()

	require.Eventually(t, func() bool {
		n, err := countItems(t, db)

		return err == nil && n == want
	}, time.Minute, 250*time.Millisecond)
}

func createDockerSource(t *testing.T, b *rds.InMemoryBackend, tc replicaDockerCase) *sql.DB {
	t.Helper()

	_, err := b.CreateDBInstance("src", tc.engine, "db.t3.micro", "appdb", "master", "", 20,
		rds.DBInstanceOptions{MasterUserPassword: "firstpass1"})
	require.NoError(t, err)

	db := openEngineDB(t, tc.kind, awaitDockerInstance(t, b, "src"), "firstpass1")
	_, err = db.ExecContext(t.Context(), createItems)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), insertItem)
	require.NoError(t, err)

	return db
}

func requireReplicating(t *testing.T, b *rds.InMemoryBackend, id string) {
	t.Helper()

	require.Eventually(t, func() bool {
		got, err := b.DescribeDBInstances(id)

		return err == nil && len(got[0].StatusInfos) == 1 && got[0].StatusInfos[0].Status == "replicating"
	}, time.Minute, replicaPoll)

	got, err := b.DescribeDBInstances(id)
	require.NoError(t, err)
	assert.Equal(t, "read replication", got[0].StatusInfos[0].StatusType)
	assert.True(t, got[0].StatusInfos[0].Normal)

	_, ok := b.ReplicaLag(id)
	assert.True(t, ok)
}

func requireGone(t *testing.T, rec *recordingEngineRuntime) {
	t.Helper()

	for _, id := range rec.containerIDs() {
		require.Eventually(t, func() bool { return !dockerContainerExists(t, id) }, time.Minute, 250*time.Millisecond)
	}

	for _, id := range rec.networkIDs() {
		require.Eventually(t, func() bool {
			out, err := exec.CommandContext(t.Context(), "docker", "network", "ls", "-q", "--no-trunc",
				"--filter", "id="+id).Output()

			return err == nil && strings.TrimSpace(string(out)) == ""
		}, time.Minute, 250*time.Millisecond)
	}
}

func TestEngineDockerStreamingReplicaAndPromote(t *testing.T) {
	t.Parallel()

	for _, tc := range replicaDockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, rec := newDockerReplicaBackend(t)
			srcDB := createDockerSource(t, b, tc)

			_, err := b.CreateDBInstanceReadReplica("replica", "src", "", "", "", rds.DBInstanceOptions{})
			require.NoError(t, err)

			replica := awaitDockerInstance(t, b, "replica")
			repDB := openEngineDB(t, tc.kind, replica, "firstpass1")

			awaitItems(t, repDB, 1)
			_, err = srcDB.ExecContext(t.Context(), "INSERT INTO items VALUES (2, 'beta')")
			require.NoError(t, err)
			awaitItems(t, repDB, 2)
			requireReplicating(t, b, "replica")

			_, err = repDB.ExecContext(t.Context(), "INSERT INTO items VALUES (3, 'gamma')")
			require.Error(t, err)
			assert.Contains(t, err.Error(), tc.readOnlyMsg)

			_, err = b.PromoteReadReplica("replica")
			require.NoError(t, err)
			awaitDockerInstance(t, b, "replica")

			require.Eventually(t, func() bool {
				_, werr := repDB.ExecContext(t.Context(), "INSERT INTO items VALUES (3, 'gamma')")

				return werr == nil
			}, time.Minute, 250*time.Millisecond)

			_, err = srcDB.ExecContext(t.Context(), "INSERT INTO items VALUES (4, 'delta')")
			require.NoError(t, err)

			n, err := countItems(t, repDB)
			require.NoError(t, err)
			assert.Equal(t, 3, n)

			got, err := b.DescribeDBInstances("replica")
			require.NoError(t, err)
			assert.Empty(t, got[0].StatusInfos)
			assert.Empty(t, got[0].ReplicaSourceDBInstanceIdentifier)

			for _, id := range []string{"replica", "src"} {
				_, err = b.DeleteDBInstanceWithOptions(id, true, "", true)
				require.NoError(t, err)
			}

			requireGone(t, rec)
		})
	}
}

func TestEngineDockerDeleteSourcePromotesReplica(t *testing.T) {
	t.Parallel()

	for _, tc := range replicaDockerCases() {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			b, rec := newDockerReplicaBackend(t)
			srcDB := createDockerSource(t, b, tc)

			_, err := b.CreateDBInstanceReadReplica("replica", "src", "", "", "", rds.DBInstanceOptions{})
			require.NoError(t, err)

			repDB := openEngineDB(t, tc.kind, awaitDockerInstance(t, b, "replica"), "firstpass1")
			awaitItems(t, repDB, 1)
			require.NoError(t, srcDB.Close())

			_, err = b.DeleteDBInstanceWithOptions("src", true, "", true)
			require.NoError(t, err)

			require.Eventually(t, func() bool {
				_, werr := repDB.ExecContext(t.Context(), "INSERT INTO items VALUES (9, 'solo')")

				return werr == nil
			}, time.Minute, 250*time.Millisecond)

			got, err := b.DescribeDBInstances("replica")
			require.NoError(t, err)
			assert.Empty(t, got[0].ReplicaSourceDBInstanceIdentifier)
			assert.Equal(t, "available", got[0].DBInstanceStatus)

			_, err = b.DeleteDBInstanceWithOptions("replica", true, "", true)
			require.NoError(t, err)

			requireGone(t, rec)
		})
	}
}
