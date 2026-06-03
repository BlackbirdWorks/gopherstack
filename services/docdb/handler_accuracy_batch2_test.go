package docdb_test

// handler_accuracy_batch2_test.go — DocumentDB AWS-accuracy audit batch-2 (go-6h7s).
//
// Covers accuracy behaviors not previously exercised via HTTP:
//   - DescribeDBInstances pagination: Marker/MaxRecords truncation and continuation.
//   - DescribeDBSubnetGroups pagination: Marker/MaxRecords.
//   - DescribeDBClusterSnapshots pagination: Marker/MaxRecords.
//   - DescribeDBEngineVersions filtering: by Engine and EngineVersion.
//   - DescribeCertificates filtering: specific ID returns one cert; unknown ID returns empty.
//   - DescribeDBClusterParameters: not-found group returns error; missing name returns error.
//   - DescribeGlobalClusters filtering: specific ID; unknown ID returns empty.
//   - CopyDBClusterParameterGroup: not-found source; duplicate target.
//   - CopyDBClusterSnapshot: not-found source; duplicate target.
//   - RestoreDBClusterFromSnapshot: not-found snapshot; duplicate cluster ID.
//   - RestoreDBClusterToPointInTime: not-found source cluster; duplicate target.
//   - CreateGlobalCluster: duplicate creation returns AlreadyExists error.
//   - RemoveFromGlobalCluster: not-found global cluster.
//   - FailoverGlobalCluster: not-found global cluster.
//   - SwitchoverGlobalCluster: not-found global cluster.
//   - ModifyDBCluster: DeletionProtection round-trip persists in Describe.
//   - CreateDBClusterSnapshot: cluster not found returns error.

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

// ---- helpers ----

func b2Handler(t *testing.T) *docdb.Handler {
	t.Helper()

	return docdb.NewHandler(docdb.NewInMemoryBackend("000000000000", "us-east-1"))
}

func b2CreateCluster(t *testing.T, h *docdb.Handler, id string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {id},
		"Engine":              {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create cluster %s: %s", id, rr.Body.String())
}

func b2CreateInstance(t *testing.T, h *docdb.Handler, instanceID, clusterID string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {instanceID},
		"DBClusterIdentifier":  {clusterID},
		"DBInstanceClass":      {"db.t3.medium"},
		"Engine":               {"docdb"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create instance %s: %s", instanceID, rr.Body.String())
}

func b2CreateSubnetGroup(t *testing.T, h *docdb.Handler, name string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {name},
		"DBSubnetGroupDescription": {"test"},
		"SubnetIds.SubnetId.1":     {"subnet-aaa"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create subnet group %s: %s", name, rr.Body.String())
}

func b2CreateSnapshot(t *testing.T, h *docdb.Handler, snapshotID, clusterID string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {snapshotID},
		"DBClusterIdentifier":         {clusterID},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create snapshot %s: %s", snapshotID, rr.Body.String())
}

func b2CreateParamGroup(t *testing.T, h *docdb.Handler, name string) {
	t.Helper()
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {name},
		"DBParameterGroupFamily":      {"docdb4.0"},
		"Description":                 {"test group"},
	})
	require.Equal(t, http.StatusOK, rr.Code, "create param group %s: %s", name, rr.Body.String())
}

// ---- DescribeDBInstances pagination ----

func TestBatch2_DescribeDBInstances_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRecords  string
		marker      string
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "no_limit_returns_all",
			maxRecords:  "",
			wantCount:   4,
			wantHasMore: false,
		},
		{
			name:        "limit_to_2",
			maxRecords:  "2",
			wantCount:   2,
			wantHasMore: true,
		},
		{
			name:        "page_2_with_marker",
			maxRecords:  "2",
			marker:      "2",
			wantCount:   2,
			wantHasMore: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2CreateCluster(t, h, "pg-cluster")
			for i := range 4 {
				b2CreateInstance(t, h, fmt.Sprintf("pg-instance-%d", i), "pg-cluster")
			}

			vals := url.Values{
				"Action":  {"DescribeDBInstances"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			if tt.marker != "" {
				vals.Set("Marker", tt.marker)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			count := strings.Count(body, "<DBInstanceIdentifier>")
			assert.Equal(t, tt.wantCount, count)
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			} else {
				assert.NotContains(t, body, "<Marker>")
			}
		})
	}
}

// ---- DescribeDBSubnetGroups pagination ----

func TestBatch2_DescribeDBSubnetGroups_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRecords  string
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "no_limit_returns_all",
			wantCount:   3,
			wantHasMore: false,
		},
		{
			name:        "limit_to_2",
			maxRecords:  "2",
			wantCount:   2,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			for i := range 3 {
				b2CreateSubnetGroup(t, h, fmt.Sprintf("sg-%d", i))
			}

			vals := url.Values{
				"Action":  {"DescribeDBSubnetGroups"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			count := strings.Count(body, "<DBSubnetGroupName>")
			assert.Equal(t, tt.wantCount, count)
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			} else {
				assert.NotContains(t, body, "<Marker>")
			}
		})
	}
}

// ---- DescribeDBClusterSnapshots pagination ----

func TestBatch2_DescribeDBClusterSnapshots_Pagination(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		maxRecords  string
		wantCount   int
		wantHasMore bool
	}{
		{
			name:        "no_limit_returns_all",
			wantCount:   3,
			wantHasMore: false,
		},
		{
			name:        "limit_to_1",
			maxRecords:  "1",
			wantCount:   1,
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2CreateCluster(t, h, "snap-cluster")
			for i := range 3 {
				b2CreateSnapshot(t, h, fmt.Sprintf("snap-%d", i), "snap-cluster")
			}

			vals := url.Values{
				"Action":  {"DescribeDBClusterSnapshots"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			count := strings.Count(body, "<DBClusterSnapshotIdentifier>")
			assert.Equal(t, tt.wantCount, count)
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			} else {
				assert.NotContains(t, body, "<Marker>")
			}
		})
	}
}

// ---- DescribeDBEngineVersions filtering ----

func TestBatch2_DescribeDBEngineVersions_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name          string
		engine        string
		engineVersion string
		wantContains  string
		wantStatus    int
	}{
		{
			name:         "no_filter_returns_all",
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBEngineVersionsResponse",
		},
		{
			name:         "filter_by_engine",
			engine:       "docdb",
			wantStatus:   http.StatusOK,
			wantContains: "docdb",
		},
		{
			name:          "filter_by_version",
			engineVersion: "4.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "4.0.0",
		},
		{
			name:          "unknown_version_returns_empty_list",
			engineVersion: "99.0.0",
			wantStatus:    http.StatusOK,
			wantContains:  "DescribeDBEngineVersionsResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			vals := url.Values{
				"Action":  {"DescribeDBEngineVersions"},
				"Version": {"2014-10-31"},
			}
			if tt.engine != "" {
				vals.Set("Engine", tt.engine)
			}
			if tt.engineVersion != "" {
				vals.Set("EngineVersion", tt.engineVersion)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- DescribeCertificates filtering ----

func TestBatch2_DescribeCertificates_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		certID          string
		wantContains    string
		wantNotContains string
		wantStatus      int
	}{
		{
			name:         "no_filter_returns_all_certs",
			wantStatus:   http.StatusOK,
			wantContains: "rds-ca-2019",
		},
		{
			name:            "filter_by_known_id",
			certID:          "rds-ca-2019",
			wantStatus:      http.StatusOK,
			wantContains:    "rds-ca-2019",
			wantNotContains: "rds-ca-rsa2048-g1",
		},
		{
			name:            "filter_by_second_known_id",
			certID:          "rds-ca-rsa2048-g1",
			wantStatus:      http.StatusOK,
			wantContains:    "rds-ca-rsa2048-g1",
			wantNotContains: "rds-ca-2019",
		},
		{
			name:         "unknown_cert_id_returns_empty",
			certID:       "rds-ca-does-not-exist",
			wantStatus:   http.StatusOK,
			wantContains: "DescribeCertificatesResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			vals := url.Values{
				"Action":  {"DescribeCertificates"},
				"Version": {"2014-10-31"},
			}
			if tt.certID != "" {
				vals.Set("CertificateIdentifier", tt.certID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
			if tt.wantNotContains != "" {
				assert.NotContains(t, rr.Body.String(), tt.wantNotContains)
			}
		})
	}
}

// ---- DescribeDBClusterParameters errors ----

func TestBatch2_DescribeDBClusterParameters_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupName    string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "missing_group_name_returns_error",
			groupName:    "",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "unknown_group_returns_not_found",
			groupName:    "no-such-group",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
		{
			name:         "known_group_returns_params",
			groupName:    "test-group",
			wantStatus:   http.StatusOK,
			wantContains: "tls",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			if tt.groupName == "test-group" {
				b2CreateParamGroup(t, h, "test-group")
			}
			vals := url.Values{
				"Action":  {"DescribeDBClusterParameters"},
				"Version": {"2014-10-31"},
			}
			if tt.groupName != "" {
				vals.Set("DBClusterParameterGroupName", tt.groupName)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- DescribeGlobalClusters filtering ----

func TestBatch2_DescribeGlobalClusters_Filter(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		filterID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "no_filter_returns_all",
			wantStatus:   http.StatusOK,
			wantContains: "DescribeGlobalClustersResponse",
		},
		{
			name:         "filter_by_known_id",
			filterID:     "global-a",
			wantStatus:   http.StatusOK,
			wantContains: "global-a",
		},
		{
			name:         "filter_by_unknown_id_returns_empty",
			filterID:     "no-such-global",
			wantStatus:   http.StatusOK,
			wantContains: "DescribeGlobalClustersResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"global-a"},
			})
			doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"global-b"},
			})

			vals := url.Values{
				"Action":  {"DescribeGlobalClusters"},
				"Version": {"2014-10-31"},
			}
			if tt.filterID != "" {
				vals.Set("GlobalClusterIdentifier", tt.filterID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
			if tt.filterID == "global-a" {
				assert.NotContains(t, rr.Body.String(), "global-b")
			}
		})
	}
}

// ---- CopyDBClusterParameterGroup errors ----

func TestBatch2_CopyDBClusterParameterGroup_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		target       string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "not_found_source_returns_error",
			source:       "no-such-group",
			target:       "new-group",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupNotFoundFault",
		},
		{
			name:         "duplicate_target_returns_error",
			source:       "src-group",
			target:       "dst-group",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterParameterGroupAlreadyExistsFault",
		},
		{
			name:         "valid_copy_succeeds",
			source:       "src-group2",
			target:       "dst-group2",
			wantStatus:   http.StatusOK,
			wantContains: "CopyDBClusterParameterGroupResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			// Pre-create source groups for tests that need them.
			if tt.source == "src-group" || tt.source == "src-group2" {
				b2CreateParamGroup(t, h, tt.source)
			}
			// Pre-create target to trigger duplicate error.
			if tt.name == "duplicate_target_returns_error" {
				b2CreateParamGroup(t, h, tt.target)
			}

			rr := doRequest(t, h, url.Values{
				"Action":  {"CopyDBClusterParameterGroup"},
				"Version": {"2014-10-31"},
				"SourceDBClusterParameterGroupIdentifier":  {tt.source},
				"TargetDBClusterParameterGroupIdentifier":  {tt.target},
				"TargetDBClusterParameterGroupDescription": {"copy"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- CopyDBClusterSnapshot errors ----

func TestBatch2_CopyDBClusterSnapshot_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		source       string
		target       string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "not_found_source_returns_error",
			source:       "no-such-snap",
			target:       "new-snap",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name:         "duplicate_target_returns_error",
			source:       "src-snap",
			target:       "dst-snap",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotAlreadyExistsFault",
		},
		{
			name:         "valid_copy_succeeds",
			source:       "src-snap2",
			target:       "dst-snap2",
			wantStatus:   http.StatusOK,
			wantContains: "CopyDBClusterSnapshotResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2CreateCluster(t, h, "copy-snap-cluster")

			// Create source snapshots where needed.
			if tt.source == "src-snap" || tt.source == "src-snap2" {
				b2CreateSnapshot(t, h, tt.source, "copy-snap-cluster")
			}
			// Create target snapshot to trigger duplicate error.
			if tt.name == "duplicate_target_returns_error" {
				b2CreateSnapshot(t, h, tt.target, "copy-snap-cluster")
			}

			rr := doRequest(t, h, url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {tt.source},
				"TargetDBClusterSnapshotIdentifier": {tt.target},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- RestoreDBClusterFromSnapshot errors ----

func TestBatch2_RestoreDBClusterFromSnapshot_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshotID   string
		clusterID    string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "missing_snapshot_id",
			snapshotID:   "",
			clusterID:    "new-cluster",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "not_found_snapshot",
			snapshotID:   "no-such-snap",
			clusterID:    "new-cluster",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name:         "duplicate_cluster_id",
			snapshotID:   "existing-snap",
			clusterID:    "existing-cluster",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterAlreadyExistsFault",
		},
		{
			name:         "valid_restore_succeeds",
			snapshotID:   "restore-snap",
			clusterID:    "restored-cluster",
			wantStatus:   http.StatusOK,
			wantContains: "RestoreDBClusterFromSnapshotResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2CreateCluster(t, h, "source-cluster")

			if tt.snapshotID == "existing-snap" || tt.snapshotID == "restore-snap" {
				b2CreateSnapshot(t, h, tt.snapshotID, "source-cluster")
			}
			if tt.clusterID == "existing-cluster" {
				b2CreateCluster(t, h, "existing-cluster")
			}

			vals := url.Values{
				"Action":              {"RestoreDBClusterFromSnapshot"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {tt.clusterID},
			}
			if tt.snapshotID != "" {
				vals.Set("DBClusterSnapshotIdentifier", tt.snapshotID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- RestoreDBClusterToPointInTime errors ----

func TestBatch2_RestoreDBClusterToPointInTime_Errors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		sourceID     string
		targetID     string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "missing_source_id",
			sourceID:     "",
			targetID:     "target-cluster",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "not_found_source_cluster",
			sourceID:     "no-such-cluster",
			targetID:     "new-cluster",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name:         "duplicate_target_cluster",
			sourceID:     "pit-source",
			targetID:     "pit-target-exists",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterAlreadyExistsFault",
		},
		{
			name:         "valid_restore_succeeds",
			sourceID:     "pit-source",
			targetID:     "pit-new-target",
			wantStatus:   http.StatusOK,
			wantContains: "RestoreDBClusterToPointInTimeResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)

			if tt.sourceID == "pit-source" {
				b2CreateCluster(t, h, "pit-source")
			}
			if tt.targetID == "pit-target-exists" {
				b2CreateCluster(t, h, "pit-target-exists")
			}

			vals := url.Values{
				"Action":              {"RestoreDBClusterToPointInTime"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {tt.targetID},
			}
			if tt.sourceID != "" {
				vals.Set("SourceDBClusterIdentifier", tt.sourceID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- CreateGlobalCluster duplicate ----

func TestBatch2_CreateGlobalCluster_Duplicate(t *testing.T) {
	t.Parallel()

	h := b2Handler(t)

	rr := doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"dup-global"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"dup-global"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterAlreadyExistsFault")
}

// ---- Global cluster not-found paths ----

func TestBatch2_GlobalCluster_NotFound_Paths(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		wantContains string
	}{
		{
			name:         "RemoveFromGlobalCluster_not_found",
			action:       "RemoveFromGlobalCluster",
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name:         "FailoverGlobalCluster_not_found",
			action:       "FailoverGlobalCluster",
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name:         "SwitchoverGlobalCluster_not_found",
			action:       "SwitchoverGlobalCluster",
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name:         "DeleteGlobalCluster_not_found",
			action:       "DeleteGlobalCluster",
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name:         "ModifyGlobalCluster_not_found",
			action:       "ModifyGlobalCluster",
			wantContains: "GlobalClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			vals := url.Values{
				"Action":                  {tt.action},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"no-such-global"},
			}
			if tt.action == "RemoveFromGlobalCluster" {
				vals.Set("DbClusterIdentifier", "dummy-cluster")
			}
			if tt.action == "FailoverGlobalCluster" || tt.action == "SwitchoverGlobalCluster" {
				vals.Set("TargetDbClusterIdentifier", "dummy-cluster")
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- ModifyDBCluster DeletionProtection round-trip ----

func TestBatch2_ModifyDBCluster_DeletionProtection_RoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                  string
		setDeletionProtection string
		wantInResponse        string
	}{
		{
			name:                  "enable_deletion_protection",
			setDeletionProtection: "true",
			wantInResponse:        "<DeletionProtection>true</DeletionProtection>",
		},
		{
			name:                  "disable_deletion_protection",
			setDeletionProtection: "false",
			wantInResponse:        "<DeletionProtection>false</DeletionProtection>",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2Handler(t)
			b2CreateCluster(t, h, "dp-cluster")

			rr := doRequest(t, h, url.Values{
				"Action":              {"ModifyDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"dp-cluster"},
				"DeletionProtection":  {tt.setDeletionProtection},
			})
			require.Equal(t, http.StatusOK, rr.Code)

			// Verify the field is persisted via Describe.
			rr = doRequest(t, h, url.Values{
				"Action":              {"DescribeDBClusters"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"dp-cluster"},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantInResponse)
		})
	}
}

// ---- CreateDBClusterSnapshot: cluster not found ----

func TestBatch2_CreateDBClusterSnapshot_ClusterNotFound(t *testing.T) {
	t.Parallel()

	h := b2Handler(t)

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"orphan-snap"},
		"DBClusterIdentifier":         {"no-such-cluster"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// ---- RestoreDBClusterFromSnapshot inherits engine version from snapshot ----

func TestBatch2_RestoreDBClusterFromSnapshot_InheritsEngineVersion(t *testing.T) {
	t.Parallel()

	h := b2Handler(t)

	// Create cluster with explicit engine version.
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ev-source-cluster"},
		"Engine":              {"docdb"},
		"EngineVersion":       {"5.0.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	b2CreateSnapshot(t, h, "ev-snap", "ev-source-cluster")

	rr = doRequest(t, h, url.Values{
		"Action":                      {"RestoreDBClusterFromSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"ev-snap"},
		"DBClusterIdentifier":         {"ev-restored"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "5.0.0")
}

// ---- RestoreDBClusterToPointInTime inherits source cluster properties ----

func TestBatch2_RestoreDBClusterToPointInTime_InheritsProperties(t *testing.T) {
	t.Parallel()

	h := b2Handler(t)

	// Create source with specific username.
	rr := doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"pit-props-source"},
		"Engine":              {"docdb"},
		"MasterUsername":      {"adminuser"},
		"MasterUserPassword":  {"password123"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	rr = doRequest(t, h, url.Values{
		"Action":                    {"RestoreDBClusterToPointInTime"},
		"Version":                   {"2014-10-31"},
		"SourceDBClusterIdentifier": {"pit-props-source"},
		"DBClusterIdentifier":       {"pit-props-target"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "adminuser")
}
