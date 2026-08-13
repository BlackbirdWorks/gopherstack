package docdb_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestHandler_RestoreClusterOperations(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "restore_from_snapshot",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"source-cluster"},
					"Engine":              {"docdb"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"my-snap"},
					"DBClusterIdentifier":         {"source-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"RestoreDBClusterFromSnapshot"},
				"Version":             {"2014-10-31"},
				"SnapshotIdentifier":  {"my-snap"},
				"DBClusterIdentifier": {"restored-cluster"},
				"Engine":              {"docdb"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "restored-cluster",
		},
		{
			name: "restore_from_snapshot_not_found",
			vals: url.Values{
				"Action":              {"RestoreDBClusterFromSnapshot"},
				"Version":             {"2014-10-31"},
				"SnapshotIdentifier":  {"nonexistent"},
				"DBClusterIdentifier": {"restored-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterSnapshotNotFoundFault",
		},
		{
			name: "restore_to_point_in_time",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"source-cluster"},
					"Engine":              {"docdb"},
				})
			},
			vals: url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {"source-cluster"},
				"DBClusterIdentifier":       {"restored-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "restored-cluster",
		},
		{
			name: "restore_to_point_in_time_not_found",
			vals: url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {"nonexistent"},
				"DBClusterIdentifier":       {"restored-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRestoreCluster_FromSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		wantContains   string
		wantStatus     int
		snapshotExists bool
		targetExists   bool
	}{
		{
			name:           "restore_from_valid_snapshot",
			snapshotExists: true,
			targetExists:   false,
			wantStatus:     200,
			wantContains:   "RestoreDBClusterFromSnapshotResponse",
		},
		{
			name:           "restore_with_nonexistent_snapshot_fails",
			snapshotExists: false,
			targetExists:   false,
			wantStatus:     400,
			wantContains:   "DBClusterSnapshotNotFoundFault",
		},
		{
			name:           "restore_when_target_exists_fails",
			snapshotExists: true,
			targetExists:   true,
			wantStatus:     400,
			wantContains:   "DBClusterAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.snapshotExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"orig-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"restore-snap"},
					"DBClusterIdentifier":         {"orig-cluster"},
				})
			}
			if tt.targetExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"restored-cluster"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"RestoreDBClusterFromSnapshot"},
				"Version":             {"2014-10-31"},
				"SnapshotIdentifier":  {"restore-snap"},
				"DBClusterIdentifier": {"restored-cluster"},
				"Engine":              {"docdb"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRestoreCluster_ToPointInTime(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
		sourceExists bool
		targetExists bool
	}{
		{
			name:         "restore_from_source",
			sourceExists: true,
			targetExists: false,
			wantStatus:   200,
			wantContains: "RestoreDBClusterToPointInTimeResponse",
		},
		{
			name:         "source_not_found_fails",
			sourceExists: false,
			targetExists: false,
			wantStatus:   400,
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name:         "target_already_exists_fails",
			sourceExists: true,
			targetExists: true,
			wantStatus:   400,
			wantContains: "DBClusterAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.sourceExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"pitr-source"},
				})
			}
			if tt.targetExists {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"pitr-target"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":                    {"RestoreDBClusterToPointInTime"},
				"Version":                   {"2014-10-31"},
				"SourceDBClusterIdentifier": {"pitr-source"},
				"DBClusterIdentifier":       {"pitr-target"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestRestoreDBClusterFromSnapshot_Errors(t *testing.T) {
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

			h := newTestHandler(t)
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
				vals.Set("SnapshotIdentifier", tt.snapshotID)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// ---- RestoreDBClusterToPointInTime errors ----

func TestRestoreDBClusterToPointInTime_Errors(t *testing.T) {
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

			h := newTestHandler(t)

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

func TestRestoreDBClusterFromSnapshot_InheritsEngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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
		"Action":              {"RestoreDBClusterFromSnapshot"},
		"Version":             {"2014-10-31"},
		"SnapshotIdentifier":  {"ev-snap"},
		"DBClusterIdentifier": {"ev-restored"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "5.0.0")
}

// ---- RestoreDBClusterToPointInTime inherits source cluster properties ----

func TestRestoreDBClusterToPointInTime_InheritsProperties(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

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
