package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestRefinement2_ClusterHasReaderEndpoint verifies CreateDBCluster response includes ReaderEndpoint.
func TestRefinement2_ClusterHasReaderEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"reader-ep-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<ReaderEndpoint>")
	assert.Contains(t, rr.Body.String(), "cluster-ro.")
}

// TestRefinement2_ClusterHasArn verifies CreateDBCluster response includes DBClusterArn.
func TestRefinement2_ClusterHasArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"arn-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<DBClusterArn>")
	assert.Contains(t, rr.Body.String(), "arn:aws:neptune:")
}

// TestRefinement2_InstanceHasArn verifies CreateDBInstance response includes DBInstanceArn.
func TestRefinement2_InstanceHasArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "inst-arn-cluster")
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-arn-instance"},
		"DBClusterIdentifier":  {"inst-arn-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<DBInstanceArn>")
}

// TestRefinement2_SnapshotHasArn verifies CreateDBClusterSnapshot response includes DBClusterSnapshotArn.
func TestRefinement2_SnapshotHasArn(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-arn-cluster")
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-arn-snap"},
		"DBClusterIdentifier":         {"snap-arn-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<DBClusterSnapshotArn>")
}

// TestRefinement2_SnapshotHasEngineVersion verifies CreateDBClusterSnapshot response includes EngineVersion.
func TestRefinement2_SnapshotHasEngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-ev-cluster")
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-ev-snap"},
		"DBClusterIdentifier":         {"snap-ev-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<EngineVersion>")
}

// TestRefinement2_SnapshotHasSnapshotType verifies SnapshotType is returned.
func TestRefinement2_SnapshotHasSnapshotType(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-type-cluster")
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-type-snap"},
		"DBClusterIdentifier":         {"snap-type-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "manual")
}

// TestRefinement2_CreateDBInstance_ClusterNotFound verifies error when cluster not found.
func TestRefinement2_CreateDBInstance_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-nocluster"},
		"DBClusterIdentifier":  {"nonexistent-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// TestRefinement2_CreateDBSnapshot_ClusterNotFound verifies error when cluster not found.
func TestRefinement2_CreateDBSnapshot_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-nocluster"},
		"DBClusterIdentifier":         {"nonexistent-cluster"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// TestRefinement2_DescribeDBClusterSnapshots_ClusterFilter verifies cluster-filter works.
func TestRefinement2_DescribeDBClusterSnapshots_ClusterFilter(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("cluster-a")
	backend.AddClusterInternal("cluster-b")
	backend.AddSnapshotInternal("snap-a", "cluster-a")
	backend.AddSnapshotInternal("snap-b", "cluster-b")

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterSnapshots"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"cluster-a"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "snap-a")
	assert.NotContains(t, body, "snap-b")
}

// TestRefinement2_DescribeDBClusterParameters_MissingGroup returns error.
func TestRefinement2_DescribeDBClusterParameters_MissingGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"nonexistent-cpg"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupNotFound")
}

// TestRefinement2_DescribeDBClusterParameters_Empty verifies empty group name succeeds.
func TestRefinement2_DescribeDBClusterParameters_Empty(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusterParameters"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_DescribeDBParameters_MissingGroup returns error.
func TestRefinement2_DescribeDBParameters_MissingGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"nonexistent-pg"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupNotFound")
}

// TestRefinement2_DescribeDBClusterSnapshotAttributes_MissingSnapshot returns error.
func TestRefinement2_DescribeDBClusterSnapshotAttributes_MissingSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterSnapshotAttributes"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"nonexistent-snap"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterSnapshotNotFoundFault")
}

// TestRefinement2_ModifyDBClusterSnapshotAttribute_MissingSnapshot returns error.
func TestRefinement2_ModifyDBClusterSnapshotAttribute_MissingSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterSnapshotAttribute"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"nonexistent-snap"},
		"AttributeName":               {"restore"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterSnapshotNotFoundFault")
}

// TestRefinement2_DescribeEventCategories_ReturnsCategories verifies non-empty event categories.
func TestRefinement2_DescribeEventCategories_ReturnsCategories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventCategories"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db-cluster")
	assert.Contains(t, body, "db-instance")
	assert.Contains(t, body, "failover")
	assert.Contains(t, body, "maintenance")
}

// TestRefinement2_DescribeDBEngineVersions_MoreVersions verifies more engine versions returned.
func TestRefinement2_DescribeDBEngineVersions_MoreVersions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBEngineVersions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "1.2.0.0")
	assert.Contains(t, body, "1.3.0.0")
	assert.Contains(t, body, "1.3.1.0")
	assert.Contains(t, body, "1.4.0.0")
	assert.Contains(t, body, "neptune1.3")
}

// TestRefinement2_DescribeOrderableDBInstanceOptions_MoreOptions verifies comprehensive instance classes.
func TestRefinement2_DescribeOrderableDBInstanceOptions_MoreOptions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeOrderableDBInstanceOptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db.r5.large")
	assert.Contains(t, body, "db.r6g.large")
	assert.Contains(t, body, "db.t3.medium")
	assert.Contains(t, body, "db.r5.4xlarge")
}

// TestRefinement2_DescribeEngineDefaultClusterParameters_Family verifies requested family is reflected.
func TestRefinement2_DescribeEngineDefaultClusterParameters_Family(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                 {"DescribeEngineDefaultClusterParameters"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupFamily": {"neptune1.2"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "neptune1.2")
}

// TestRefinement2_DescribeEngineDefaultParameters_Family verifies requested family is reflected.
func TestRefinement2_DescribeEngineDefaultParameters_Family(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                 {"DescribeEngineDefaultParameters"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupFamily": {"neptune1.4"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "neptune1.4")
}

// TestRefinement2_DescribeValidDBInstanceModifications_HasClasses verifies valid instance classes returned.
func TestRefinement2_DescribeValidDBInstanceModifications_HasClasses(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeValidDBInstanceModifications"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"some-instance"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "db.r5.large")
	assert.Contains(t, body, "db.r6g.large")
}

// TestRefinement2_RestoreDBClusterFromSnapshot_CopiesEngineVersion verifies EngineVersion from snapshot.
func TestRefinement2_RestoreDBClusterFromSnapshot_CopiesEngineVersion(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("src-cluster")
	backend.AddSnapshotInternal("src-snap", "src-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"RestoreDBClusterFromSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"src-snap"},
		"DBClusterIdentifier":         {"restored-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "restored-cluster")
	assert.Contains(t, rr.Body.String(), "<ReaderEndpoint>")
	assert.Contains(t, rr.Body.String(), "<DBClusterArn>")
}

// TestRefinement2_InstanceEngineVersionInheritsFromCluster verifies instance engine version from cluster.
func TestRefinement2_InstanceEngineVersionInheritsFromCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ev-inherit-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"ev-inherit-inst"},
		"DBClusterIdentifier":  {"ev-inherit-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<EngineVersion>")
	assert.Contains(t, rr.Body.String(), "1.3.0.0")
}

// TestRefinement2_InstanceHasMaintenanceWindow verifies PreferredMaintenanceWindow is returned.
func TestRefinement2_InstanceHasMaintenanceWindow(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "mw-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"mw-inst"},
		"DBClusterIdentifier":  {"mw-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<PreferredMaintenanceWindow>")
}

// TestRefinement2_CopyDBClusterSnapshot_PropagatesEngineVersion verifies engine version is copied.
func TestRefinement2_CopyDBClusterSnapshot_PropagatesEngineVersion(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("copy-src-cluster")
	backend.AddSnapshotInternal("copy-src-snap", "copy-src-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"copy-src-snap"},
		"TargetDBClusterSnapshotIdentifier": {"copy-tgt-snap"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<EngineVersion>")
}

// TestRefinement2_ClusterBackupRetentionPeriod verifies BackupRetentionPeriod is returned.
func TestRefinement2_ClusterBackupRetentionPeriod(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"backup-rp-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<BackupRetentionPeriod>")
}

// TestRefinement2_DescribeDBClusterSnapshotAttributes_ExistingSnapshot succeeds.
func TestRefinement2_DescribeDBClusterSnapshotAttributes_ExistingSnapshot(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("attr-cluster")
	backend.AddSnapshotInternal("attr-snap", "attr-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterSnapshotAttributes"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"attr-snap"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "attr-snap")
}

// TestRefinement2_RestoreDBClusterToPointInTime_HasReaderEndpoint verifies reader endpoint.
func TestRefinement2_RestoreDBClusterToPointInTime_HasReaderEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "pitr-src-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"RestoreDBClusterToPointInTime"},
		"Version":                   {"2014-10-31"},
		"SourceDBClusterIdentifier": {"pitr-src-cluster"},
		"DBClusterIdentifier":       {"pitr-tgt-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<ReaderEndpoint>")
	assert.Contains(t, rr.Body.String(), "<DBClusterArn>")
}

// TestRefinement2_DescribeDBParameters_ExistingGroup succeeds.
func TestRefinement2_DescribeDBParameters_ExistingGroup(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddParameterGroupInternal("my-pg", "neptune1.3")

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"my-pg"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}
