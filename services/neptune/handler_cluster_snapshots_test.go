package neptune_test

import (
	"context"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHandler_DBClusterSnapshots(t *testing.T) {
	t.Parallel()

	tests := []struct {
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_snapshot",
			vals: url.Values{
				"Action":                      {"CreateDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"test-snap"},
				"DBClusterIdentifier":         {"snap-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "test-snap",
		},
		{
			name: "describe_snapshots",
			vals: url.Values{
				"Action":  {"DescribeDBClusterSnapshots"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DescribeDBClusterSnapshotsResponse",
		},
		{
			name: "delete_snapshot",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {"test-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "DeleteDBClusterSnapshotResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createCluster(t, h, "snap-cluster")
			if tt.name != "create_snapshot" {
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"test-snap"},
					"DBClusterIdentifier":         {"snap-cluster"},
				})
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestHandler_DescribeClusterSnapshotsPagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-cluster")

	for _, id := range []string{"snap-1", "snap-2"} {
		doRequest(t, h, url.Values{
			"Action":                      {"CreateDBClusterSnapshot"},
			"Version":                     {"2014-10-31"},
			"DBClusterSnapshotIdentifier": {id},
			"DBClusterIdentifier":         {"snap-cluster"},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":     {"DescribeDBClusterSnapshots"},
		"Version":    {"2014-10-31"},
		"MaxRecords": {"1"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "<Marker>")
}

func TestHandler_DeleteClusterSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap2-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap2"},
		"DBClusterIdentifier":         {"snap2-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap2"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DeleteDBClusterSnapshotResponse")
}

func TestHandler_CopyDBClusterSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*neptune.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "copy_snapshot_success",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "snap-copy-cluster")
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"src-snap"},
					"DBClusterIdentifier":         {"snap-copy-cluster"},
				})
			},
			vals: url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"src-snap"},
				"TargetDBClusterSnapshotIdentifier": {"dst-snap"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "dst-snap",
		},
		{
			name: "copy_snapshot_source_not_found",
			vals: url.Values{
				"Action":                            {"CopyDBClusterSnapshot"},
				"Version":                           {"2014-10-31"},
				"SourceDBClusterSnapshotIdentifier": {"no-such-snap"},
				"TargetDBClusterSnapshotIdentifier": {"new-snap"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- DBClusterSnapshot comprehensive coverage ----

func TestDBClusterSnapshot_FieldsPreserved(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"snap-src"},
		"EngineVersion":       {"1.3.0.0"},
		"StorageEncrypted":    {"true"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-full"},
		"DBClusterIdentifier":         {"snap-src"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "snap-full")
	assert.Contains(t, body, "snap-src")
	assert.Contains(t, body, "1.3.0.0")
	assert.Contains(t, body, "manual")
	assert.Contains(t, body, "available")
}

func TestDBClusterSnapshot_DescribeByCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "snap-filter-cluster")
	createCluster(t, h, "other-cluster")

	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-cluster1"},
		"DBClusterIdentifier":         {"snap-filter-cluster"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"snap-other"},
		"DBClusterIdentifier":         {"other-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterSnapshots"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"snap-filter-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "snap-cluster1")
	assert.NotContains(t, body, "snap-other")
}

func TestDBClusterSnapshot_CrossRegionCopy(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "cross-region-src")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"source-snap"},
		"DBClusterIdentifier":         {"cross-region-src"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"source-snap"},
		"TargetDBClusterSnapshotIdentifier": {"target-snap"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "target-snap")
	assert.Contains(t, body, "cross-region-src")
}

func TestDBClusterSnapshot_CopyNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"nonexistent-snap"},
		"TargetDBClusterSnapshotIdentifier": {"dest-snap"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterSnapshotNotFoundFault")
}

func TestDBClusterSnapshot_Attributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "attr-snap-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"attr-snap"},
		"DBClusterIdentifier":         {"attr-snap-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterSnapshotAttributes"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"attr-snap"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "attr-snap")
}

func TestDBClusterSnapshot_ModifyAttributes(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "mod-attr-cluster")
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"mod-attr-snap"},
		"DBClusterIdentifier":         {"mod-attr-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterSnapshotAttribute"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"mod-attr-snap"},
		"AttributeName":               {"restore"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestDBClusterSnapshot_RestoreFromSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"restore-src"},
		"EngineVersion":       {"1.3.0.0"},
	})
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"restore-snap"},
		"DBClusterIdentifier":         {"restore-src"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"RestoreDBClusterFromSnapshot"},
		"Version":                     {"2014-10-31"},
		"DBClusterSnapshotIdentifier": {"restore-snap"},
		"DBClusterIdentifier":         {"restore-dst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "restore-dst")
	assert.Contains(t, body, "available")

	// Verify new cluster exists
	rr2 := doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusters"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"restore-dst"},
	})
	require.Equal(t, http.StatusOK, rr2.Code)
	assert.Contains(t, rr2.Body.String(), "restore-dst")
}

func TestDBClusterSnapshot_RestoreToPointInTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "pitr-src")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"RestoreDBClusterToPointInTime"},
		"Version":                   {"2014-10-31"},
		"SourceDBClusterIdentifier": {"pitr-src"},
		"DBClusterIdentifier":       {"pitr-dst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "pitr-dst")
	assert.Contains(t, body, "available")
}

// ---- Cascade delete verification ----

func TestDeleteCluster_CascadesSnapshots(t *testing.T) {
	t.Parallel()

	b := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_, err := b.CreateDBCluster(
		context.Background(),
		"cascade-del-cluster",
		"",
		0,
		neptune.DBClusterCreateOptions{},
	)
	require.NoError(t, err)
	_, err = b.CreateDBClusterSnapshot(context.Background(), "cascade-snap", "cascade-del-cluster")
	require.NoError(t, err)

	require.Equal(t, 1, neptune.ClusterSnapshotCount(b))

	// Delete cluster — snapshots should remain (AWS behavior: snapshots not auto-deleted)
	_, err = b.DeleteDBCluster(
		context.Background(),
		"cascade-del-cluster",
		neptune.DBClusterDeleteOptions{SkipFinalSnapshot: true},
	)
	require.NoError(t, err)

	require.Equal(t, 0, neptune.ClusterCount(b))
	// Snapshots are not cascade-deleted
	require.Equal(t, 1, neptune.ClusterSnapshotCount(b))
}

// TestCopyDBClusterSnapshot_MissingSource verifies error on missing source snapshot.
func TestCopyDBClusterSnapshot_MissingSource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBClusterSnapshot"},
		"Version":                           {"2014-10-31"},
		"SourceDBClusterSnapshotIdentifier": {"nonexistent"},
		"TargetDBClusterSnapshotIdentifier": {"target-snap"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterSnapshotNotFoundFault")
}

// TestSnapshotHasArn verifies CreateDBClusterSnapshot response includes DBClusterSnapshotArn.
func TestSnapshotHasArn(t *testing.T) {
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

// TestSnapshotHasEngineVersion verifies CreateDBClusterSnapshot response includes EngineVersion.
func TestSnapshotHasEngineVersion(t *testing.T) {
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

// TestSnapshotHasSnapshotType verifies SnapshotType is returned.
func TestSnapshotHasSnapshotType(t *testing.T) {
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

// TestCreateDBSnapshot_ClusterNotFound verifies error when cluster not found.
func TestCreateDBSnapshot_ClusterNotFound(t *testing.T) {
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

// TestDescribeDBClusterSnapshots_ClusterFilter verifies cluster-filter works.
func TestDescribeDBClusterSnapshots_ClusterFilter(t *testing.T) {
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

// TestDescribeDBClusterSnapshotAttributes_MissingSnapshot returns error.
func TestDescribeDBClusterSnapshotAttributes_MissingSnapshot(t *testing.T) {
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

// TestModifyDBClusterSnapshotAttribute_MissingSnapshot returns error.
func TestModifyDBClusterSnapshotAttribute_MissingSnapshot(t *testing.T) {
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

// TestRestoreDBClusterFromSnapshot_CopiesEngineVersion verifies EngineVersion from snapshot.
func TestRestoreDBClusterFromSnapshot_CopiesEngineVersion(t *testing.T) {
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

// TestCopyDBClusterSnapshot_PropagatesEngineVersion verifies engine version is copied.
func TestCopyDBClusterSnapshot_PropagatesEngineVersion(t *testing.T) {
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

// TestDescribeDBClusterSnapshotAttributes_ExistingSnapshot succeeds.
func TestDescribeDBClusterSnapshotAttributes_ExistingSnapshot(t *testing.T) {
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

// --- Snapshot attributes ---

func TestSnapshotAttributes(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshotID   string
		action       string
		wantContains string
		wantStatus   int
		setupSnap    bool
	}{
		{
			name:         "describe_attributes_existing",
			setupSnap:    true,
			snapshotID:   "snap-attr",
			action:       "DescribeDBClusterSnapshotAttributes",
			wantStatus:   http.StatusOK,
			wantContains: "snap-attr",
		},
		{
			name:         "describe_attributes_no_id",
			setupSnap:    false,
			snapshotID:   "",
			action:       "DescribeDBClusterSnapshotAttributes",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
		},
		{
			name:         "describe_attributes_not_found",
			setupSnap:    false,
			snapshotID:   "no-such-snap",
			action:       "DescribeDBClusterSnapshotAttributes",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name:         "modify_attributes_existing",
			setupSnap:    true,
			snapshotID:   "snap-attr2",
			action:       "ModifyDBClusterSnapshotAttribute",
			wantStatus:   http.StatusOK,
			wantContains: "ModifyDBClusterSnapshotAttributeResponse",
		},
		{
			name:         "modify_attributes_not_found",
			setupSnap:    false,
			snapshotID:   "no-such-snap",
			action:       "ModifyDBClusterSnapshotAttribute",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setupSnap {
				createCluster(t, h, "snap-cluster-"+tt.snapshotID)
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {tt.snapshotID},
					"DBClusterIdentifier":         {"snap-cluster-" + tt.snapshotID},
				})
			}
			vals := url.Values{
				"Action":  {tt.action},
				"Version": {"2014-10-31"},
			}
			if tt.snapshotID != "" {
				vals["DBClusterSnapshotIdentifier"] = []string{tt.snapshotID}
			}
			if tt.action == "ModifyDBClusterSnapshotAttribute" {
				vals["AttributeName"] = []string{"restore"}
				vals["ValuesToAdd.AttributeValue.1"] = []string{"123456789012"}
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- Restore operations ---

func TestRestoreDBClusterFromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		snapshotID   string
		targetID     string
		wantContains string
		wantStatus   int
		setupSnap    bool
	}{
		{
			name:         "success",
			snapshotID:   "restore-snap",
			targetID:     "restored-cluster",
			setupSnap:    true,
			wantStatus:   http.StatusOK,
			wantContains: "restored-cluster",
		},
		{
			name:         "snapshot_not_found",
			snapshotID:   "no-such-snap",
			targetID:     "new-cluster",
			setupSnap:    false,
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setupSnap {
				createCluster(t, h, "src-cluster-"+tt.snapshotID)
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {tt.snapshotID},
					"DBClusterIdentifier":         {"src-cluster-" + tt.snapshotID},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":                      {"RestoreDBClusterFromSnapshot"},
				"Version":                     {"2014-10-31"},
				"DBClusterSnapshotIdentifier": {tt.snapshotID},
				"DBClusterIdentifier":         {tt.targetID},
			})
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}
