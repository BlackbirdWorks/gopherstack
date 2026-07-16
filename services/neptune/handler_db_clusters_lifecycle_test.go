package neptune_test

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDeleteDBCluster_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		protection   string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "deletion_protection_enabled",
			protection:   "true",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name:         "deletion_protection_disabled",
			protection:   "false",
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-prot-cluster"},
				"DeletionProtection":  {tt.protection},
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-prot-cluster"},
				"SkipFinalSnapshot":   {"true"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- StopDBCluster / StartDBCluster state validation ---

func TestStopDBCluster_AlreadyStopped(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "state-cluster")

	// First stop succeeds.
	rr := doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"state-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Second stop on already-stopped cluster must fail.
	rr = doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"state-cluster"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
}

func TestStartDBCluster_AlreadyAvailable(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "start-avail-cluster")

	// Cluster is available; starting it must fail.
	rr := doRequest(t, h, url.Values{
		"Action":              {"StartDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"start-avail-cluster"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
}

func TestStartDBCluster_AfterStop_Succeeds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "stop-start-cluster")

	doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"stop-start-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"StartDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"stop-start-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "StartDBClusterResponse")
}

// ---- FailoverDBCluster comprehensive coverage ----

func TestFailoverDBCluster_ReturnsCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "failover-cluster")
	createInstance(t, h, "failover-inst-w", "failover-cluster")
	createInstance(t, h, "failover-inst-r", "failover-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"failover-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "failover-cluster")
	assert.Contains(t, body, "available")
}

func TestFailoverDBCluster_WithTargetInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "fo-target-cluster")
	createInstance(t, h, "fo-writer", "fo-target-cluster")
	createInstance(t, h, "fo-reader", "fo-target-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                     {"FailoverDBCluster"},
		"Version":                    {"2014-10-31"},
		"DBClusterIdentifier":        {"fo-target-cluster"},
		"TargetDBInstanceIdentifier": {"fo-reader"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "fo-target-cluster")
}

func TestFailoverDBCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nonexistent-cluster"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// ---- IAM Role operations comprehensive coverage ----

func TestAddRemoveRole_Lifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "role-cluster")

	roleARN := "arn:aws:iam::000000000000:role/neptune-role"

	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Add same role again — idempotent
	rr = doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Remove role
	rr = doRequest(t, h, url.Values{
		"Action":              {"RemoveRoleFromDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestAddRole_MissingRoleARN(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "role-missing-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-missing-cluster"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

func TestAddRole_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nonexistent-cluster"},
		"RoleArn":             {"arn:aws:iam::000000000000:role/test"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

func TestRoles_ClearedOnClusterDelete(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(
		context.Background(),
		"role-del-cluster",
		"",
		0,
		neptune.DBClusterCreateOptions{},
	)
	require.NoError(t, err)
	err = b.AddRoleToDBCluster(
		context.Background(),
		"role-del-cluster",
		"arn:aws:iam::000000000000:role/r1",
	)
	require.NoError(t, err)
	err = b.AddRoleToDBCluster(
		context.Background(),
		"role-del-cluster",
		"arn:aws:iam::000000000000:role/r2",
	)
	require.NoError(t, err)

	_, err = b.DeleteDBCluster(
		context.Background(),
		"role-del-cluster",
		neptune.DBClusterDeleteOptions{SkipFinalSnapshot: true},
	)
	require.NoError(t, err)

	// Verify roles gone
	require.Equal(t, 0, neptune.ClusterRoleCount(b, "role-del-cluster"))
}

func TestPromoteReadReplicaDBCluster_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"PromoteReadReplicaDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"nonexistent-cluster"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// ---- XML response structure verification ----

func TestXML_CreateDBCluster_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"xml-struct-cluster"},
		"EngineVersion":       {"1.3.0.0"},
		"Port":                {"8182"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Verify it's valid XML
	var result struct {
		XMLName xml.Name `xml:"CreateDBClusterResponse"`
	}
	err := xml.Unmarshal(rr.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "CreateDBClusterResponse", result.XMLName.Local)
}

func TestXML_DescribeDBClusters_Structure(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "xml-desc-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	var result struct {
		XMLName xml.Name `xml:"DescribeDBClustersResponse"`
	}
	err := xml.Unmarshal(rr.Body.Bytes(), &result)
	require.NoError(t, err)
	assert.Equal(t, "DescribeDBClustersResponse", result.XMLName.Local)
}

// ---- Cluster ARN format ----

func TestClusterARN_Format(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"arn-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "arn:aws:neptune:us-east-1:000000000000:cluster:arn-cluster")
}

// TestDeleteDBCluster_CascadesClearRoles verifies IAM roles are removed when cluster is deleted.
func TestDeleteDBCluster_CascadesClearRoles(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	hb := neptune.NewHandler(backend)

	backend.AddClusterInternal("cascade-cluster")

	doRequest(t, hb, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"cascade-cluster"},
		"RoleArn":             {"arn:aws:iam::000000000000:role/test-role"},
	})
	require.Equal(t, 1, neptune.ClusterRoleCount(backend, "cascade-cluster"))

	doRequest(t, hb, url.Values{
		"Action":              {"DeleteDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"cascade-cluster"},
		"SkipFinalSnapshot":   {"true"},
	})

	assert.Equal(t, 0, neptune.ClusterRoleCount(backend, "cascade-cluster"))
	assert.Equal(t, 0, neptune.ClusterCount(backend))
}

// TestEngineVersion_InCreateResponse verifies EngineVersion is returned.
func TestEngineVersion_InCreateResponse(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ev-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<EngineVersion>")
	assert.Contains(t, rr.Body.String(), "1.3.0.0")
}

// TestPortParsing verifies custom port is honored.
func TestPortParsing(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"port-cluster"},
		"Port":                {"9999"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "9999")
}

// TestAddRoleToDBCluster_MissingRole verifies error on empty RoleArn.
func TestAddRoleToDBCluster_MissingRole(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "role-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-cluster"},
		"RoleArn":             {""},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// TestAddRoleToDBCluster_Idempotent verifies adding same role twice is idempotent.
func TestAddRoleToDBCluster_Idempotent(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("role-idem-cluster")

	roleARN := "arn:aws:iam::000000000000:role/neptune-role"
	doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-idem-cluster"},
		"RoleArn":             {roleARN},
	})
	doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-idem-cluster"},
		"RoleArn":             {roleARN},
	})

	// Role should only be listed once (idempotent)
	assert.Equal(t, 1, neptune.ClusterRoleCount(backend, "role-idem-cluster"))
}

// TestCreateDBCluster_MissingID verifies validation error on empty ID.
func TestCreateDBCluster_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"CreateDBCluster"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// TestClusterHasReaderEndpoint verifies CreateDBCluster response includes ReaderEndpoint.
func TestClusterHasReaderEndpoint(t *testing.T) {
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

// TestClusterHasArn verifies CreateDBCluster response includes DBClusterArn.
func TestClusterHasArn(t *testing.T) {
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

// TestClusterBackupRetentionPeriod verifies BackupRetentionPeriod is returned.
func TestClusterBackupRetentionPeriod(t *testing.T) {
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

// TestRestoreDBClusterToPointInTime_HasReaderEndpoint verifies reader endpoint.
func TestRestoreDBClusterToPointInTime_HasReaderEndpoint(t *testing.T) {
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

// --- Role operations ---

func TestRemoveRoleFromDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterID    string
		roleARN      string
		wantContains string
		wantStatus   int
		setupCluster bool
	}{
		{
			name:         "cluster_not_found",
			clusterID:    "no-such-cluster",
			roleARN:      "arn:aws:iam::000000000000:role/MyRole",
			setupCluster: false,
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name:         "missing_cluster_id",
			clusterID:    "",
			roleARN:      "arn:aws:iam::000000000000:role/MyRole",
			setupCluster: false,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "missing_role_arn",
			clusterID:    "role-cluster",
			roleARN:      "",
			setupCluster: true,
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setupCluster {
				createCluster(t, h, tt.clusterID)
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"RemoveRoleFromDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {tt.clusterID},
				"RoleArn":             {tt.roleARN},
			})
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRemoveRoleFromDBCluster_RoundTrip(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "role-rt-cluster")

	roleARN := "arn:aws:iam::000000000000:role/NeptuneRole"

	// add role
	rr := doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-rt-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// remove role
	rr = doRequest(t, h, url.Values{
		"Action":              {"RemoveRoleFromDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-rt-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

func TestRestoreDBClusterToPointInTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		srcClusterID string
		targetID     string
		wantContains string
		wantStatus   int
		setupSrc     bool
	}{
		{
			name:         "success",
			srcClusterID: "src-pitr",
			targetID:     "pitr-restored",
			setupSrc:     true,
			wantStatus:   http.StatusOK,
			wantContains: "pitr-restored",
		},
		{
			name:         "source_not_found",
			srcClusterID: "no-such-cluster",
			targetID:     "pitr-new",
			setupSrc:     false,
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setupSrc {
				createCluster(t, h, tt.srcClusterID)
			}
			rr := doRequest(t, h, url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {tt.srcClusterID},
				"DBClusterIdentifier":       {tt.targetID},
			})
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- PromoteReadReplicaDBCluster ---

func TestPromoteReadReplicaDBCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterID    string
		wantContains string
		wantStatus   int
		setupCluster bool
	}{
		{
			name:         "success",
			setupCluster: true,
			clusterID:    "promote-cluster",
			wantStatus:   http.StatusOK,
			wantContains: "promote-cluster",
		},
		{
			name:         "not_found",
			setupCluster: false,
			clusterID:    "no-such-cluster",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setupCluster {
				createCluster(t, h, tt.clusterID)
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"PromoteReadReplicaDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {tt.clusterID},
			})
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- IAM Role operations ---

// TestRemoveRoleFromDBCluster_ActuallyRemovesRole tests removing role from cluster.
func TestRemoveRoleFromDBCluster_ActuallyRemovesRole(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("role-rm-cluster")

	roleARN := "arn:aws:iam::000000000000:role/neptune-role"
	doRequest(t, h, url.Values{
		"Action":              {"AddRoleToDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-rm-cluster"},
		"RoleArn":             {roleARN},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"RemoveRoleFromDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"role-rm-cluster"},
		"RoleArn":             {roleARN},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Equal(t, 0, neptune.ClusterRoleCount(backend, "role-rm-cluster"))
}

// TestStopStartDBCluster tests stop/start lifecycle.
func TestStopStartDBCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "stopstart-cluster")

	// Stop
	rr := doRequest(t, h, url.Values{
		"Action":              {"StopDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"stopstart-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "stopstart-cluster")

	// Start
	rr = doRequest(t, h, url.Values{
		"Action":              {"StartDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"stopstart-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "stopstart-cluster")
}

// TestFailoverDBCluster tests FailoverDBCluster.
func TestFailoverDBCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "failover-cluster")
	createInstance(t, h, "failover-writer", "failover-cluster")
	createInstance(t, h, "failover-reader", "failover-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"failover-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<IsClusterWriter>true</IsClusterWriter>")
}

// TestModifyDBCluster tests cluster modification.
func TestModifyDBCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "modify-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"ModifyDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"modify-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "modify-cluster")
}

// TestDescribeDBClusters_ByID tests single cluster lookup.
func TestDescribeDBClusters_ByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "byid-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"byid-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "byid-cluster")
}

// TestCreateDBCluster_VpcSecurityGroupIds verifies VpcSecurityGroupIds are parsed and returned.
func TestCreateDBCluster_VpcSecurityGroupIds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains []string
	}{
		{
			name: "single_sg",
			vals: url.Values{
				"Action":                       {"CreateDBCluster"},
				"Version":                      {"2014-10-31"},
				"DBClusterIdentifier":          {"sg-cluster"},
				"VpcSecurityGroupIds.member.1": {"sg-11111111"},
			},
			wantContains: []string{"sg-11111111", "VpcSecurityGroupMembership"},
		},
		{
			name: "multiple_sgs",
			vals: url.Values{
				"Action":                       {"CreateDBCluster"},
				"Version":                      {"2014-10-31"},
				"DBClusterIdentifier":          {"sg-cluster-multi"},
				"VpcSecurityGroupIds.member.1": {"sg-aaaa"},
				"VpcSecurityGroupIds.member.2": {"sg-bbbb"},
			},
			wantContains: []string{"sg-aaaa", "sg-bbbb"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestCreateDBCluster_AvailabilityZones verifies AvailabilityZones are parsed and stored.
func TestCreateDBCluster_AvailabilityZones(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains []string
	}{
		{
			name: "single_az",
			vals: url.Values{
				"Action":                     {"CreateDBCluster"},
				"Version":                    {"2014-10-31"},
				"DBClusterIdentifier":        {"az-cluster"},
				"AvailabilityZones.member.1": {"us-east-1a"},
			},
			wantContains: []string{"az-cluster"},
		},
		{
			name: "no_azs",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"no-az-cluster"},
			},
			wantContains: []string{"no-az-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestCreateDBCluster_MasterUsername verifies MasterUsername is parsed and returned.
func TestCreateDBCluster_MasterUsername(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains []string
	}{
		{
			name: "with_master_username",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"mu-cluster"},
				"MasterUsername":      {"neptune-admin"},
			},
			wantContains: []string{"neptune-admin", "MasterUsername"},
		},
		{
			name: "without_master_username",
			vals: url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"no-mu-cluster"},
			},
			wantContains: []string{"no-mu-cluster"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestAssociatedRoles_PersistedOnCluster verifies that AddRoleToDBCluster persists
// roles in the cluster's AssociatedRoles field (not just the separate roles store).
func TestAssociatedRoles_PersistedOnCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		roleArns     []string
		wantContains []string
	}{
		{
			name:         "single_role_in_associated_roles",
			roleArns:     []string{"arn:aws:iam::000000000000:role/MyRole"},
			wantContains: []string{"arn:aws:iam::000000000000:role/MyRole", "AssociatedRoles", "ACTIVE"},
		},
		{
			name:         "duplicate_role_not_added_twice",
			roleArns:     []string{"arn:aws:iam::000000000000:role/DupRole", "arn:aws:iam::000000000000:role/DupRole"},
			wantContains: []string{"DupRole"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
			ctx := context.Background()
			backend.AddClusterInternal("role-cluster")
			for _, roleArn := range tt.roleArns {
				err := backend.AddRoleToDBCluster(ctx, "role-cluster", roleArn)
				require.NoError(t, err)
			}
			clusters, err := backend.DescribeDBClusters(ctx, "role-cluster", neptune.DBClusterFilters{})
			require.NoError(t, err)
			require.Len(t, clusters, 1)
			cl := clusters[0]
			if tt.name == "duplicate_role_not_added_twice" {
				assert.Len(t, cl.AssociatedRoles, 1, "duplicate role should not be added twice")
			} else {
				assert.NotEmpty(t, cl.AssociatedRoles)
			}
			// Also verify via HTTP handler
			h := neptune.NewHandler(backend)
			rr := doRequest(t, h, url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"role-cluster"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestDescribeDBClusters_SortedDeterministically verifies clusters are returned sorted by identifier.
func TestDescribeDBClusters_SortedDeterministically(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		clusterIDs    []string
		wantOrderedIn []string
	}{
		{
			name:          "sorted_alphabetically",
			clusterIDs:    []string{"cluster-z", "cluster-a", "cluster-m"},
			wantOrderedIn: []string{"cluster-a", "cluster-m", "cluster-z"},
		},
		{
			name:          "already_sorted",
			clusterIDs:    []string{"alpha", "beta", "gamma"},
			wantOrderedIn: []string{"alpha", "beta", "gamma"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			for _, id := range tt.clusterIDs {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {id},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			// Verify order by checking positions in the XML body
			positions := make([]int, len(tt.wantOrderedIn))
			for i, want := range tt.wantOrderedIn {
				positions[i] = strings.Index(body, want)
			}

			for i := 1; i < len(positions); i++ {
				assert.Less(t, positions[i-1], positions[i],
					"cluster %q should appear before %q in response",
					tt.wantOrderedIn[i-1], tt.wantOrderedIn[i])
			}
		})
	}
}

// TestDeleteDBCluster_RequiresFinalSnapshotOrSkip verifies that deleting without
// SkipFinalSnapshot=true and without FinalDBSnapshotIdentifier returns an error.
func TestDeleteDBCluster_RequiresFinalSnapshotOrSkip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "skip_final_snapshot_true_succeeds",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster-skip"},
				"SkipFinalSnapshot":   {"true"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
		{
			name: "with_final_snapshot_identifier_succeeds",
			vals: url.Values{
				"Action":                    {"DeleteDBCluster"},
				"Version":                   {"2014-10-31"},
				"DBClusterIdentifier":       {"del-cluster-snap"},
				"SkipFinalSnapshot":         {"false"},
				"FinalDBSnapshotIdentifier": {"my-final-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterResponse",
		},
		{
			name: "no_skip_no_identifier_fails",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster-fail"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterCombination",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			// Create a cluster for each case that needs one
			if tt.wantStatus == http.StatusOK {
				clusterID := tt.vals.Get("DBClusterIdentifier")
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {clusterID},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestDBCluster_HasResourceIdAndCreateTime verifies DBCluster includes DbClusterResourceId
// and ClusterCreateTime fields.
func TestDBCluster_HasResourceIdAndCreateTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterID    string
		wantContains []string
	}{
		{
			name:      "cluster_has_resource_id",
			clusterID: "res-cluster",
			wantContains: []string{
				"DbClusterResourceId",
				"cluster-res-cluster",
				"ClusterCreateTime",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {tt.clusterID},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// Test_FailoverDBCluster verifies FailoverDBCluster performs a real writer
// promotion (not a disguised no-op): a cluster with fewer than two members
// has no reader to fail over to and must error, a two-member cluster must
// swap which member is the writer, and an explicit TargetDBInstanceIdentifier
// must be honored (or rejected when invalid).
func Test_FailoverDBCluster(t *testing.T) {
	t.Parallel()

	t.Run("no reader available errors", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "solo-cluster")
		createInstance(t, h, "solo-inst", "solo-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"solo-cluster"},
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
	})

	t.Run("promotes a reader to writer", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "fo-cluster")
		createInstance(t, h, "fo-writer", "fo-cluster")
		createInstance(t, h, "fo-reader", "fo-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"fo-cluster"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
		body := rr.Body.String()
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-reader</DBInstanceIdentifier><IsClusterWriter>true</IsClusterWriter>",
		)
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-writer</DBInstanceIdentifier><IsClusterWriter>false</IsClusterWriter>",
		)
	})

	t.Run("honors explicit target instance", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "fo-target-cluster")
		createInstance(t, h, "fo-target-writer", "fo-target-cluster")
		createInstance(t, h, "fo-target-a", "fo-target-cluster")
		createInstance(t, h, "fo-target-b", "fo-target-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":                     {"FailoverDBCluster"},
			"Version":                    {"2014-10-31"},
			"DBClusterIdentifier":        {"fo-target-cluster"},
			"TargetDBInstanceIdentifier": {"fo-target-b"},
		})
		require.Equal(t, http.StatusOK, rr.Code)
		body := rr.Body.String()
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-target-b</DBInstanceIdentifier><IsClusterWriter>true</IsClusterWriter>",
		)
		assert.Contains(
			t, body,
			"<DBInstanceIdentifier>fo-target-a</DBInstanceIdentifier><IsClusterWriter>false</IsClusterWriter>",
		)
	})

	t.Run("invalid target instance errors", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)
		createCluster(t, h, "fo-badtarget-cluster")
		createInstance(t, h, "fo-badtarget-writer", "fo-badtarget-cluster")
		createInstance(t, h, "fo-badtarget-reader", "fo-badtarget-cluster")

		rr := doRequest(t, h, url.Values{
			"Action":                     {"FailoverDBCluster"},
			"Version":                    {"2014-10-31"},
			"DBClusterIdentifier":        {"fo-badtarget-cluster"},
			"TargetDBInstanceIdentifier": {"does-not-exist"},
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "InvalidDBInstanceState")
	})

	t.Run("unknown cluster errors", func(t *testing.T) {
		t.Parallel()
		h := newTestHandler(t)

		rr := doRequest(t, h, url.Values{
			"Action":              {"FailoverDBCluster"},
			"Version":             {"2014-10-31"},
			"DBClusterIdentifier": {"missing-cluster"},
		})
		require.Equal(t, http.StatusBadRequest, rr.Code)
		assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
	})
}
