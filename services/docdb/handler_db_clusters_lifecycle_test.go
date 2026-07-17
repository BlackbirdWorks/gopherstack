package docdb_test

import (
	"context"
	"fmt"
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/docdb"
)

func TestStopStartClusterStateValidation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*testing.T, *docdb.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "stop_available_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "stopped",
		},
		{
			name: "stop_already_stopped_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":              {"StopDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name: "start_stopped_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
				doRequest(t, h, url.Values{
					"Action":              {"StopDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "available",
		},
		{
			name: "start_already_available_cluster",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"my-cluster"},
				})
			},
			vals: url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"my-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
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

func TestDeleteClusterProtections(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(h *docdb.Handler)
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "deletion_protection_enabled",
			setup: func(h *docdb.Handler) {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"protected-cluster"},
					"DeletionProtection":  {"true"},
				})
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name: "active_instance_blocks_delete",
			setup: func(h *docdb.Handler) {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {"cluster-with-inst"},
				})
				doRequest(t, h, url.Values{
					"Action":               {"CreateDBInstance"},
					"Version":              {"2014-10-31"},
					"DBInstanceIdentifier": {"inst1"},
					"DBClusterIdentifier":  {"cluster-with-inst"},
					"DBInstanceClass":      {"db.r5.large"},
					"Engine":               {"docdb"},
				})
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			tt.setup(h)

			clusterID := "protected-cluster"
			if tt.name == "active_instance_blocks_delete" {
				clusterID = "cluster-with-inst"
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {clusterID},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestFailoverStoppedCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "failover_stopped_cluster_rejected",
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			})
			doRequest(t, h, url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"test-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDeleteCluster_FinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup              func(*testing.T, *docdb.Handler)
		vals               url.Values
		name               string
		wantContains       string
		wantStatus         int
		wantSnapshotExists bool
	}{
		{
			name: "skip_final_snapshot",
			vals: url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
				"SkipFinalSnapshot":   {"true"},
			},
			wantStatus:         200,
			wantContains:       "DeleteDBClusterResponse",
			wantSnapshotExists: false,
		},
		{
			name: "create_final_snapshot",
			vals: url.Values{
				"Action":                           {"DeleteDBCluster"},
				"Version":                          {"2014-10-31"},
				"DBClusterIdentifier":              {"del-cluster"},
				"SkipFinalSnapshot":                {"false"},
				"FinalDBClusterSnapshotIdentifier": {"final-snap"},
			},
			wantStatus:         200,
			wantContains:       "DeleteDBClusterResponse",
			wantSnapshotExists: true,
		},
		{
			name: "final_snapshot_already_exists_fails",
			setup: func(t *testing.T, h *docdb.Handler) {
				t.Helper()
				doRequest(t, h, url.Values{
					"Action":                      {"CreateDBClusterSnapshot"},
					"Version":                     {"2014-10-31"},
					"DBClusterSnapshotIdentifier": {"final-snap"},
					"DBClusterIdentifier":         {"del-cluster"},
				})
			},
			vals: url.Values{
				"Action":                           {"DeleteDBCluster"},
				"Version":                          {"2014-10-31"},
				"DBClusterIdentifier":              {"del-cluster"},
				"SkipFinalSnapshot":                {"false"},
				"FinalDBClusterSnapshotIdentifier": {"final-snap"},
			},
			wantStatus:   400,
			wantContains: "DBClusterSnapshotAlreadyExistsFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
			})
			if tt.setup != nil {
				tt.setup(t, h)
			}
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)

			if tt.wantSnapshotExists {
				snaps, err := h.Backend.DescribeDBClusterSnapshots(context.Background(), "final-snap", "", "")
				require.NoError(t, err)
				assert.Len(t, snaps, 1)
			}
		})
	}
}

func TestDeleteCluster_DeletionProtection(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name               string
		wantContains       string
		wantStatus         int
		deletionProtection bool
	}{
		{
			name:               "deletion_protection_blocks_delete",
			deletionProtection: true,
			wantStatus:         400,
			wantContains:       "InvalidDBClusterStateFault",
		},
		{
			name:               "no_deletion_protection_allows_delete",
			deletionProtection: false,
			wantStatus:         200,
			wantContains:       "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "prot-cluster",
				Status:              "available",
				DeletionProtection:  tt.deletionProtection,
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"prot-cluster"},
				"SkipFinalSnapshot":   {"true"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestDeleteCluster_WithInstances(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
		addInstance  bool
	}{
		{
			name:         "fails_with_instances",
			addInstance:  true,
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name:         "succeeds_without_instances",
			addInstance:  false,
			wantStatus:   200,
			wantContains: "DeleteDBClusterResponse",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "inst-cluster",
				Status:              "available",
			})
			if tt.addInstance {
				b.AddDBInstanceInternal(&docdb.DBInstance{
					DBInstanceIdentifier: "inst-to-block",
					DBClusterIdentifier:  "inst-cluster",
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"inst-cluster"},
				"SkipFinalSnapshot":   {"true"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestStopCluster_AlreadyStopped(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "stop_already_stopped_cluster_fails",
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "stopped-cluster",
				Status:              "stopped",
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"StopDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stopped-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestStartCluster_AlreadyAvailable(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "start_already_available_cluster_fails",
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":              {"CreateDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"avail-cluster"},
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"StartDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"avail-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestFailoverCluster_StoppedFails(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name:         "failover_stopped_cluster_fails",
			wantStatus:   400,
			wantContains: "InvalidDBClusterStateFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			b := h.Backend
			b.AddDBClusterInternal(&docdb.DBCluster{
				DBClusterIdentifier: "stopped-fo-cluster",
				Status:              "stopped",
			})
			rr := doRequest(t, h, url.Values{
				"Action":              {"FailoverDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"stopped-fo-cluster"},
			})
			assert.Equal(t, tt.wantStatus, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestPagination_Clusters(t *testing.T) {
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
			wantCount:   5,
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
			wantHasMore: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			for i := range 5 {
				doRequest(t, h, url.Values{
					"Action":              {"CreateDBCluster"},
					"Version":             {"2014-10-31"},
					"DBClusterIdentifier": {fmt.Sprintf("cluster-%d", i)},
				})
			}
			vals := url.Values{
				"Action":  {"DescribeDBClusters"},
				"Version": {"2014-10-31"},
			}
			if tt.maxRecords != "" {
				vals.Set("MaxRecords", tt.maxRecords)
			}
			if tt.marker != "" {
				vals.Set("Marker", tt.marker)
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, 200, rr.Code)
			body := rr.Body.String()
			if tt.wantHasMore {
				assert.Contains(t, body, "<Marker>")
			}
		})
	}
}

func TestModifyDBCluster_DeletionProtection_RoundTrip(t *testing.T) {
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

			h := newTestHandler(t)
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

// TestParity_DeleteDBCluster_SkipFinalSnapshot verifies SkipFinalSnapshot validation.
func TestDeleteDBCluster_SkipFinalSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		extraVals  url.Values
		name       string
		wantCode   string
		wantStatus int
	}{
		{
			name:       "missing_skip_and_identifier_rejected",
			extraVals:  url.Values{},
			wantStatus: http.StatusBadRequest,
			wantCode:   "InvalidParameterValue",
		},
		{
			name:       "skip_final_snapshot_true_ok",
			extraVals:  url.Values{"SkipFinalSnapshot": {"true"}},
			wantStatus: http.StatusOK,
		},
		{
			name: "final_snapshot_identifier_ok",
			extraVals: url.Values{
				"FinalDBClusterSnapshotIdentifier": {"my-final-snap"},
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			pbCreateCluster(t, h, "del-cluster", nil)
			vals := url.Values{
				"Action":              {"DeleteDBCluster"},
				"Version":             {"2014-10-31"},
				"DBClusterIdentifier": {"del-cluster"},
			}
			maps.Copy(vals, tc.extraVals)
			rr := doRequest(t, h, vals)
			assert.Equal(t, tc.wantStatus, rr.Code)
			if tc.wantCode != "" {
				assert.Equal(t, tc.wantCode, pbExtractErrorCode(t, rr.Body.String()))
			}
		})
	}
}
