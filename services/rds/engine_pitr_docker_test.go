package rds_test

import (
	"context"
	"net"
	"os/exec"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/services/rds"
)

func newDockerPITRBackend(t *testing.T) (*rds.InMemoryBackend, *fakeClock) {
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

	clock := &fakeClock{now: time.Now().UTC().Truncate(time.Second)}
	b := rds.NewInMemoryBackend("123456789012", "us-east-1")
	b.EnableEngine(rds.EngineConfig{
		Runtime: &recordingEngineRuntime{EngineRuntime: er}, Now: clock.Now, RestorePointInterval: -1,
	})
	t.Cleanup(b.Close)

	return b, clock
}

func queryNames(t *testing.T, kind string, inst rds.DBInstance) []string {
	t.Helper()

	db := openEngineDB(t, kind, inst, "firstpass1")
	rows, err := db.QueryContext(t.Context(), "SELECT name FROM items ORDER BY id")
	require.NoError(t, err)

	defer func() { _ = rows.Close() }()

	var names []string

	for rows.Next() {
		var n string
		require.NoError(t, rows.Scan(&n))
		names = append(names, n)
	}

	require.NoError(t, rows.Err())

	return names
}

func TestEngineDockerPointInTimeRestore(t *testing.T) {
	t.Parallel()

	for _, engine := range []string{"postgres", "mysql"} {
		t.Run(engine, func(t *testing.T) {
			t.Parallel()

			b, clock := newDockerPITRBackend(t)
			t0 := clock.Now()

			_, err := b.CreateDBInstance("src", engine, "db.t3.micro", "appdb", "master", "", 20,
				rds.DBInstanceOptions{MasterUserPassword: "firstpass1", BackupRetentionPeriod: 7})
			require.NoError(t, err)

			src := awaitDockerInstance(t, b, "src")
			require.Eventually(t, func() bool {
				got, derr := b.DescribeDBInstances("src")

				return derr == nil && !got[0].LatestRestorableTime.IsZero()
			}, dockerEngineWait, 500*time.Millisecond)

			db := openEngineDB(t, engine, src, "firstpass1")
			_, err = db.ExecContext(t.Context(), createItems)
			require.NoError(t, err)
			_, err = db.ExecContext(t.Context(), insertItem)
			require.NoError(t, err)

			clock.advance(10 * time.Minute)
			rds.RecordRestorePointsForTest(b)

			_, err = db.ExecContext(t.Context(), "INSERT INTO items VALUES (2, 'beta')")
			require.NoError(t, err)

			clock.advance(10 * time.Minute)
			rds.RecordRestorePointsForTest(b)

			for _, tc := range []struct {
				id   string
				want []string
				at   time.Duration
			}{
				{id: "early", at: 15 * time.Minute, want: []string{"alpha"}},
				{id: "late", at: 20 * time.Minute, want: []string{"alpha", "beta"}},
			} {
				_, err = b.RestoreDBInstanceToPointInTime(tc.id, "src",
					rds.DBInstanceOptions{RestoreTime: t0.Add(tc.at)})
				require.NoError(t, err)

				restored := awaitDockerInstance(t, b, tc.id)
				assert.Equal(t, tc.want, queryNames(t, engine, restored), tc.id)
			}

			_, err = b.RestoreDBInstanceToPointInTime("toolate", "src",
				rds.DBInstanceOptions{RestoreTime: t0.Add(time.Hour)})
			require.ErrorIs(t, err, rds.ErrInvalidRestore)
		})
	}
}

func TestEngineDockerClusterEndpointListener(t *testing.T) {
	t.Parallel()

	b, _ := newDockerPITRBackend(t)

	_, err := b.CreateDBCluster("c1", "aurora-postgresql", "master", "appdb", "", 0, nil,
		rds.DBClusterOptions{MasterUserPassword: "firstpass1"})
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		got, derr := b.DescribeDBClusters("c1")
		require.NoError(t, derr)
		require.NotEqual(t, "failed", got[0].Status)

		return got[0].Status == "available"
	}, dockerEngineWait, 500*time.Millisecond)

	_, err = b.CreateDBInstance("w1", "aurora-postgresql", "db.r5.large", "", "master", "", 0,
		rds.DBInstanceOptions{DBClusterIdentifier: "c1"})
	require.NoError(t, err)
	rds.FlushInstanceLifecycle(b)

	ep, err := b.CreateDBClusterEndpoint("ep1", "c1", "WRITER", rds.ClusterEndpointOptions{})
	require.NoError(t, err)

	host, port, err := splitHostPort(ep.Endpoint)
	require.NoError(t, err)

	inst := rds.DBInstance{Endpoint: host, Port: port}

	db := openEngineDB(t, "postgres", inst, "firstpass1")
	_, err = db.ExecContext(t.Context(), createItems)
	require.NoError(t, err)
	_, err = db.ExecContext(t.Context(), insertItem)
	require.NoError(t, err)

	assert.Equal(t, []string{"alpha"}, queryNames(t, "postgres", inst))

	_, err = b.ModifyDBClusterEndpoint("ep1", "", rds.ClusterEndpointOptions{ExcludedMembers: []string{"w1"}})
	require.NoError(t, err)

	other := openEngineDB(t, "postgres", inst, "firstpass1")
	require.Error(t, other.PingContext(context.Background()))

	_, err = b.DeleteDBClusterEndpoint("ep1")
	require.NoError(t, err)
	require.Eventually(t, func() bool { return !dialable(t, port) }, time.Minute, 100*time.Millisecond)
}

func splitHostPort(addr string) (string, int, error) {
	host, p, err := net.SplitHostPort(addr)
	if err != nil {
		return "", 0, err
	}

	n, err := strconv.Atoi(p)

	return host, n, err
}
