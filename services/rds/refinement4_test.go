package rds_test

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/rds"
)

// TestInstanceCreateTime verifies that InstanceCreateTime is set on CreateDBInstance.
func TestInstanceCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	before := time.Now().UTC()
	inst, err := b.CreateDBInstance("db-ts", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, inst.InstanceCreateTime.IsZero(), "InstanceCreateTime should be set")
	assert.False(t, inst.InstanceCreateTime.Before(before), "InstanceCreateTime should be after test start")
	assert.False(t, inst.InstanceCreateTime.After(after), "InstanceCreateTime should be before test end")
}

// TestSnapshotCreateTime verifies SnapshotCreateTime and PercentProgress on CreateDBSnapshot.
func TestSnapshotCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-snap", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	before := time.Now().UTC()
	snap, err := b.CreateDBSnapshot("snap-1", "db-snap")
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, snap.SnapshotCreateTime.IsZero(), "SnapshotCreateTime should be set")
	assert.False(t, snap.SnapshotCreateTime.Before(before))
	assert.False(t, snap.SnapshotCreateTime.After(after))
	assert.Equal(t, 100, snap.PercentProgress, "completed snapshot should have 100% progress")
}

// TestClusterCreateTime verifies ClusterCreateTime is set on CreateDBCluster.
func TestClusterCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	before := time.Now().UTC()
	c, err := b.CreateDBCluster("cluster-ts", "aurora-postgresql", "admin", "mydb", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, c.ClusterCreateTime.IsZero(), "ClusterCreateTime should be set")
	assert.False(t, c.ClusterCreateTime.Before(before))
	assert.False(t, c.ClusterCreateTime.After(after))
}

// TestClusterSnapshotCreateTime verifies new fields on CreateDBClusterSnapshot.
func TestClusterSnapshotCreateTime(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("cluster-snap", "aurora-postgresql", "admin", "", "", 0, nil,
		rds.DBClusterOptions{EngineVersion: "15.3", StorageEncrypted: true})
	require.NoError(t, err)
	snap, err := b.CreateDBClusterSnapshot("cluster-snap-1", "cluster-snap")
	require.NoError(t, err)
	assert.False(t, snap.SnapshotCreateTime.IsZero(), "SnapshotCreateTime should be set")
	assert.Equal(t, 100, snap.PercentProgress)
	assert.Equal(t, "15.3", snap.EngineVersion, "EngineVersion should be copied from cluster")
	assert.True(t, snap.StorageEncrypted, "StorageEncrypted should be copied from cluster")
}

// TestDescribeDBInstancesSorted verifies deterministic sort order.
func TestDescribeDBInstancesSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, id := range []string{"db-z", "db-a", "db-m"} {
		_, err := b.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeDBInstances("")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "db-a", got[0].DBInstanceIdentifier)
	assert.Equal(t, "db-m", got[1].DBInstanceIdentifier)
	assert.Equal(t, "db-z", got[2].DBInstanceIdentifier)
}

// TestDescribeDBClustersSorted verifies deterministic sort order.
func TestDescribeDBClustersSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, id := range []string{"cluster-z", "cluster-a", "cluster-m"} {
		_, err := b.CreateDBCluster(id, "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeDBClusters("")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "cluster-a", got[0].DBClusterIdentifier)
	assert.Equal(t, "cluster-m", got[1].DBClusterIdentifier)
	assert.Equal(t, "cluster-z", got[2].DBClusterIdentifier)
}

// TestDescribeDBSnapshotsSorted verifies deterministic sort order.
func TestDescribeDBSnapshotsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-sort", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	for _, id := range []string{"snap-z", "snap-a", "snap-m"} {
		_, snapErr := b.CreateDBSnapshot(id, "db-sort")
		require.NoError(t, snapErr)
	}
	got, err := b.DescribeDBSnapshots("", "")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "snap-a", got[0].DBSnapshotIdentifier)
	assert.Equal(t, "snap-m", got[1].DBSnapshotIdentifier)
	assert.Equal(t, "snap-z", got[2].DBSnapshotIdentifier)
}

// TestDescribeDBClusterSnapshotsSorted verifies deterministic sort order.
func TestDescribeDBClusterSnapshotsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("cluster-sort", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	for _, id := range []string{"csnap-z", "csnap-a", "csnap-m"} {
		_, snapErr := b.CreateDBClusterSnapshot(id, "cluster-sort")
		require.NoError(t, snapErr)
	}
	got, err := b.DescribeDBClusterSnapshots("", "")
	require.NoError(t, err)
	require.Len(t, got, 3)
	assert.Equal(t, "csnap-a", got[0].DBClusterSnapshotIdentifier)
	assert.Equal(t, "csnap-m", got[1].DBClusterSnapshotIdentifier)
	assert.Equal(t, "csnap-z", got[2].DBClusterSnapshotIdentifier)
}

// TestDescribeEventsSorted verifies events are returned sorted by creation time.
func TestDescribeEventsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	// Create instances to generate events (each CreateDBInstance publishes an event).
	for _, id := range []string{"db-ev1", "db-ev2", "db-ev3"} {
		_, err := b.CreateDBInstance(id, "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
		require.NoError(t, err)
	}
	got, err := b.DescribeEvents("", "", 0)
	require.NoError(t, err)
	for i := 1; i < len(got); i++ {
		assert.False(t, got[i].CreatedAt.Before(got[i-1].CreatedAt),
			"events should be sorted ascending by creation time")
	}
}

// TestDeleteDBInstanceClearsAutomatedBackup verifies that deleting an instance removes its automated backup.
func TestDeleteDBInstanceClearsAutomatedBackup(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-del", "postgres", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{BackupRetentionPeriod: 7})
	require.NoError(t, err)
	// Verify backup was created.
	backups := b.DescribeDBInstanceAutomatedBackups("db-del")
	require.Len(t, backups, 1)
	// Delete the instance.
	_, err = b.DeleteDBInstance("db-del")
	require.NoError(t, err)
	// Backup should be removed.
	backups = b.DescribeDBInstanceAutomatedBackups("db-del")
	assert.Empty(t, backups, "automated backup should be removed when instance is deleted")
}

// TestDeleteDBClusterClearsMemberInstances verifies that deleting a cluster clears member DBClusterIdentifier.
func TestDeleteDBClusterClearsMemberInstances(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("cluster-del", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBInstance("member-inst", "aurora-postgresql", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{DBClusterIdentifier: "cluster-del"})
	require.NoError(t, err)
	// Instance should be a member.
	inst, err := b.DescribeDBInstances("member-inst")
	require.NoError(t, err)
	assert.Equal(t, "cluster-del", inst[0].DBClusterIdentifier)
	// Delete cluster.
	_, err = b.DeleteDBCluster("cluster-del")
	require.NoError(t, err)
	// Instance's cluster identifier should be cleared.
	inst, err = b.DescribeDBInstances("member-inst")
	require.NoError(t, err)
	assert.Empty(
		t, inst[0].DBClusterIdentifier, "member instance DBClusterIdentifier should be cleared on cluster delete",
	)
}

// TestRebootDBClusterTransitionsState verifies RebootDBCluster requires available state and transitions to rebooting.
func TestRebootDBClusterTransitionsState(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("cluster-reboot", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	// Rebooting an available cluster should return it in "rebooting" status.
	got, err := b.RebootDBCluster("cluster-reboot")
	require.NoError(t, err)
	assert.Equal(t, "rebooting", got.Status)
}

// TestRebootDBClusterNotFound verifies error when cluster does not exist.
func TestRebootDBClusterNotFound(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.RebootDBCluster("nonexistent")
	require.Error(t, err)
	assert.ErrorIs(t, err, rds.ErrClusterNotFound)
}

// TestEnableHTTPEndpointByID verifies EnableHTTPEndpoint works with cluster identifier.
func TestEnableHTTPEndpointByID(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBCluster("http-cluster", "aurora-postgresql", "admin", "", "", 0, nil, rds.DBClusterOptions{})
	require.NoError(t, err)
	// Enable by cluster ID directly.
	err = b.EnableHTTPEndpoint("http-cluster")
	require.NoError(t, err)
	got, err := b.DescribeDBClusters("http-cluster")
	require.NoError(t, err)
	assert.True(t, got[0].HTTPEndpointEnabled)
	// Disable by ARN.
	err = b.DisableHTTPEndpoint("arn:aws:rds:us-east-1:123456789012:cluster:http-cluster")
	require.NoError(t, err)
	got, err = b.DescribeDBClusters("http-cluster")
	require.NoError(t, err)
	assert.False(t, got[0].HTTPEndpointEnabled)
}

// TestEventSubscriptionEventCategories verifies EventCategories field in CreateEventSubscription.
func TestEventSubscriptionEventCategories(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	cats := []string{"backup", "availability"}
	sub, err := b.CreateEventSubscription(
		"cat-sub", "arn:aws:sns:us-east-1:123456789012:topic", "db-instance", nil, cats,
	)
	require.NoError(t, err)
	assert.Equal(t, cats, sub.EventCategories)
	// Modify to update categories.
	newCats := []string{"failover"}
	sub, err = b.ModifyEventSubscription("cat-sub", "", "", nil, newCats, nil)
	require.NoError(t, err)
	assert.Equal(t, newCats, sub.EventCategories)
}

// TestDeletionProtectionCanBeDisabled verifies that ModifyDBInstance can disable DeletionProtection.
func TestDeletionProtectionCanBeDisabled(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-dp", "postgres", "db.t3.micro", "", "admin", "", 20,
		rds.DBInstanceOptions{DeletionProtection: true})
	require.NoError(t, err)
	inst, err := b.DescribeDBInstances("db-dp")
	require.NoError(t, err)
	require.True(t, inst[0].DeletionProtection)
	// Disable deletion protection via ModifyDBInstance with DeletionProtectionSet.
	_, err = b.ModifyDBInstance("db-dp", "", 0, rds.DBInstanceOptions{
		DeletionProtection:    false,
		DeletionProtectionSet: true,
	})
	require.NoError(t, err)
	inst, err = b.DescribeDBInstances("db-dp")
	require.NoError(t, err)
	assert.False(t, inst[0].DeletionProtection, "DeletionProtection should be disabled after modify")
}

// TestCopyDBSnapshotSetsTimestamp verifies CopyDBSnapshot sets SnapshotCreateTime and PercentProgress.
func TestCopyDBSnapshotSetsTimestamp(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBInstance("db-copy", "postgres", "db.t3.micro", "", "admin", "", 20, rds.DBInstanceOptions{})
	require.NoError(t, err)
	_, err = b.CreateDBSnapshot("snap-src", "db-copy")
	require.NoError(t, err)
	before := time.Now().UTC()
	copied, err := b.CopyDBSnapshot("snap-src", "snap-copy", rds.CopyDBSnapshotOptions{})
	require.NoError(t, err)
	after := time.Now().UTC()
	assert.False(t, copied.SnapshotCreateTime.IsZero())
	assert.False(t, copied.SnapshotCreateTime.Before(before))
	assert.False(t, copied.SnapshotCreateTime.After(after))
	assert.Equal(t, 100, copied.PercentProgress)
}

// TestDescribeDBParameterGroupsSorted verifies sorted order.
func TestDescribeDBParameterGroupsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, name := range []string{"pg-z", "pg-a", "pg-m"} {
		_, err := b.CreateDBParameterGroup(name, "postgres15", "desc")
		require.NoError(t, err)
	}
	got, err := b.DescribeDBParameterGroups("")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 3)
	names := make([]string, len(got))
	for i, pg := range got {
		names[i] = pg.DBParameterGroupName
	}
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "parameter groups should be sorted alphabetically")
	}
}

// TestDescribeOptionGroupsSorted verifies sorted order.
func TestDescribeOptionGroupsSorted(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	for _, name := range []string{"og-z", "og-a", "og-m"} {
		_, err := b.CreateOptionGroup(name, "mysql", "8.0", "desc")
		require.NoError(t, err)
	}
	got, err := b.DescribeOptionGroups("")
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(got), 3)
	names := make([]string, len(got))
	for i, og := range got {
		names[i] = og.OptionGroupName
	}
	for i := 1; i < len(names); i++ {
		assert.LessOrEqual(t, names[i-1], names[i], "option groups should be sorted alphabetically")
	}
}

// TestModifyDBProxyCopied verifies ModifyDBProxy returns an independent copy.
func TestModifyDBProxyCopied(t *testing.T) {
	t.Parallel()
	b := newTestBackend(t)
	_, err := b.CreateDBProxy("my-proxy", "POSTGRESQL", "arn:aws:iam::123456789012:role/proxy-role",
		[]rds.UserAuthConfig{{SecretARN: "arn:aws:secretsmanager:us-east-1:123456789012:secret:s1"}})
	require.NoError(t, err)
	requireTLS := true
	proxy1, err := b.ModifyDBProxy("my-proxy", &requireTLS, nil, nil)
	require.NoError(t, err)
	// Verify the returned value is a copy (not a pointer to the stored proxy).
	// Modify a subsequent call and verify proxy1 is unaffected.
	requireTLS2 := false
	timeout := 900
	_, err = b.ModifyDBProxy("my-proxy", &requireTLS2, &timeout, nil)
	require.NoError(t, err)
	assert.True(t, proxy1.RequireTLS, "first ModifyDBProxy result should be independent copy")
	assert.Equal(t, 1800, proxy1.IdleClientTimeout, "first result should retain original timeout")
}
