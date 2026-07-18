package memorydb_test

import (
	"encoding/json"
	"net/http"
	"strings"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/memorydb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWireEpoch_Snapshot_CreationTimeIsNumber(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "snap-epoch-cluster",
		"NodeType":    "db.t4g.small",
	})

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"ClusterName":  "snap-epoch-cluster",
		"SnapshotName": "snap-epoch-1",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	snap, _ := resp["Snapshot"].(map[string]any)
	require.NotNil(t, snap)

	_, isNumber := snap["SnapshotCreationTime"].(float64)
	assert.True(t, isNumber,
		"Snapshot.SnapshotCreationTime must be a JSON number, got %T: %v",
		snap["SnapshotCreationTime"], snap["SnapshotCreationTime"])
}

// -- User Authentication.Type wire-shape regression ------------------------
//
// Real AWS's Authentication.Type output enum only defines "password",
// "no-password", and "iam" (aws-sdk-go-v2/service/memorydb/types.AuthenticationType).
// "no-password-required" is not a valid output value.

func TestRace_CreateSnapshotVsTagResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "race-cluster",
		"NodeType":    "db.t4g.small",
	})

	const n = 20

	statuses := make(chan int, n)

	var wg sync.WaitGroup

	for i := range n {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			rec, err := doRequestAsync(h, "CreateSnapshot", map[string]any{
				"ClusterName":  "race-cluster",
				"SnapshotName": snapshotNameForIndex(i),
			})
			if err != nil {
				statuses <- -1

				return
			}

			statuses <- rec.Code
		}(i)
	}

	wg.Wait()
	close(statuses)

	for status := range statuses {
		assert.Equal(t, http.StatusOK, status)
	}

	arns := collectSnapshotARNs(t, h)

	var tagWg sync.WaitGroup

	for _, snapshotARN := range arns {
		tagWg.Add(1)

		go func(snapshotARN string) {
			defer tagWg.Done()

			_, _ = doRequestAsync(h, "TagResource", map[string]any{
				"ResourceArn": snapshotARN,
				"Tags":        []map[string]string{{"Key": "k", "Value": "v"}},
			})
		}(snapshotARN)
	}

	tagWg.Wait()
}

// TestParity_ExportSnapshot verifies ExportSnapshot returns the named snapshot.
func TestHandler_ExportSnapshot(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	createRec := doRequest(t, h, "CreateCluster", map[string]any{
		"ClusterName": "export-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})
	require.Equal(t, http.StatusOK, createRec.Code, "create cluster: %s", createRec.Body)

	snapRec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"ClusterName":  "export-cluster",
		"SnapshotName": "export-snap",
	})
	require.Equal(t, http.StatusOK, snapRec.Code, "create snapshot: %s", snapRec.Body)

	rec := doRequest(t, h, "ExportSnapshot", map[string]any{
		"SnapshotName": "export-snap",
		"S3BucketName": "my-bucket",
	})
	require.Equal(t, http.StatusOK, rec.Code, "export snapshot: %s", rec.Body)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snap, _ := resp["Snapshot"].(map[string]any)
	assert.Equal(t, "export-snap", snap["Name"], "Snapshot.Name must match")
	assert.NotEmpty(t, snap["ARN"], "Snapshot.ARN must be present")
}

// TestParity_ExportSnapshot_NotFound verifies ExportSnapshot returns an error for missing snapshot.
func TestHandler_ExportSnapshot_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ExportSnapshot", map[string]any{
		"SnapshotName": "no-such-snap",
	})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

// TestParity_ExportSnapshot_MissingSnapshotName verifies validation.
func TestHandler_ExportSnapshot_MissingSnapshotName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rec := doRequest(t, h, "ExportSnapshot", map[string]any{})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestHandler_CreateSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body         map[string]any
		name         string
		wantStatus   int
		wantSnapshot bool
	}{
		{
			name: "creates snapshot",
			body: map[string]any{
				"SnapshotName": "my-snap",
				"ClusterName":  "my-cluster",
			},
			wantStatus:   http.StatusOK,
			wantSnapshot: true,
		},
		{
			name:       "missing snapshot name",
			body:       map[string]any{"ClusterName": "my-cluster"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing cluster name",
			body:       map[string]any{"SnapshotName": "my-snap"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "duplicate snapshot",
			body: map[string]any{
				"SnapshotName": "dup-snap",
				"ClusterName":  "my-cluster",
			},
			wantStatus: http.StatusConflict,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			// Pre-create the cluster that the snapshot will reference.
			if clusterName, ok := tt.body["ClusterName"].(string); ok && clusterName != "" {
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": clusterName,
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			}

			if tt.name == "duplicate snapshot" {
				doRequest(t, h, "CreateSnapshot", tt.body)
			}

			rec := doRequest(t, h, "CreateSnapshot", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantSnapshot {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				snap, ok := resp["Snapshot"]
				require.True(t, ok)
				snapMap := snap.(map[string]any)
				assert.Equal(t, tt.body["SnapshotName"], snapMap["Name"])
				assert.NotEmpty(t, snapMap["ARN"])
			}
		})
	}
}

func TestHandler_CopySnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantName   string
		wantStatus int
	}{
		{
			name: "copies snapshot",
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus: http.StatusOK,
			wantName:   "dst-snap",
		},
		{
			name:       "missing source name",
			body:       map[string]any{"TargetSnapshotName": "dst"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "missing target name",
			body:       map[string]any{"SourceSnapshotName": "src"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "source not found",
			body: map[string]any{
				"SourceSnapshotName": "no-such-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus: http.StatusNotFound,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			if tt.name == "copies snapshot" {
				// Pre-create the cluster, then the source snapshot.
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "src-snap",
					"ClusterName":  "my-cluster",
				})
			}

			rec := doRequest(t, h, "CopySnapshot", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantName != "" {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				snap := resp["Snapshot"].(map[string]any)
				assert.Equal(t, tt.wantName, snap["Name"])
			}
		})
	}
}

func TestHandler_DeleteSnapshot(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{
			name:       "deletes existing snapshot",
			wantStatus: http.StatusOK,
		},
		{
			name:       "delete non-existent snapshot",
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "missing snapshot name",
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			switch tt.name {
			case "deletes existing snapshot":
				// Pre-create cluster then snapshot.
				doRequest(t, h, "CreateCluster", map[string]any{
					"ClusterName": "my-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				doRequest(t, h, "CreateSnapshot", map[string]any{
					"SnapshotName": "del-snap",
					"ClusterName":  "my-cluster",
				})
				rec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotName": "del-snap"})
				assert.Equal(t, tt.wantStatus, rec.Code)
			case "delete non-existent snapshot":
				rec := doRequest(t, h, "DeleteSnapshot", map[string]any{"SnapshotName": "no-snap"})
				assert.Equal(t, tt.wantStatus, rec.Code)
			case "missing snapshot name":
				rec := doRequest(t, h, "DeleteSnapshot", map[string]any{})
				assert.Equal(t, tt.wantStatus, rec.Code)
			}
		})
	}
}

func TestHandler_Snapshot_ClusterConfiguration(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		clusterSetup map[string]any
		wantFields   []string
	}{
		{
			name: "snapshot contains engine field",
			clusterSetup: map[string]any{
				"ClusterName": "snap-engine-test",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
				"Engine":      "redis",
			},
			wantFields: []string{"Engine"},
		},
		{
			name: "snapshot contains maintenance window",
			clusterSetup: map[string]any{
				"ClusterName":       "snap-mw-test",
				"NodeType":          "db.r6g.large",
				"ACLName":           "open-access",
				"MaintenanceWindow": "sun:05:00-sun:06:00",
			},
			wantFields: []string{"MaintenanceWindow"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			clusterName := tt.clusterSetup["ClusterName"].(string)
			resp, code := doCreateCluster(t, h, tt.clusterSetup)
			require.Equal(t, http.StatusOK, code, "create cluster failed: %v", resp)

			// Create a snapshot.
			snapshotName := clusterName + "-snap"
			rec := doRequest(t, h, "CreateSnapshot", map[string]any{
				"ClusterName":  clusterName,
				"SnapshotName": snapshotName,
			})
			require.Equal(t, http.StatusOK, rec.Code)

			// Describe the snapshot.
			snaps := doDescribeSnapshots(t, h, snapshotName)
			require.Len(t, snaps, 1)

			snap, _ := snaps[0].(map[string]any)
			config, hasConfig := snap["ClusterConfiguration"].(map[string]any)
			require.True(t, hasConfig, "snapshot should have ClusterConfiguration")

			for _, field := range tt.wantFields {
				val, ok := config[field]
				assert.True(t, ok, "ClusterConfiguration should have field %q", field)
				assert.NotEmpty(t, val, "ClusterConfiguration.%s should not be empty", field)
			}
		})
	}
}

// -- Findings 25/29: Default parameter groups ------------------------------------

func TestHandler_Snapshot_ExpandedClusterConfig_AllFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	clusterBody := map[string]any{
		"ClusterName":            "full-config-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"Engine":                 "redis",
		"EngineVersion":          "7.0",
		"MaintenanceWindow":      "sun:05:00-sun:06:00",
		"SnapshotWindow":         "03:00-04:00",
		"SnapshotRetentionLimit": 3,
	}

	resp, code := doCreateCluster(t, h, clusterBody)
	require.Equal(t, http.StatusOK, code, "create cluster: %v", resp)

	rec := doRequest(t, h, "CreateSnapshot", map[string]any{
		"ClusterName":  "full-config-cluster",
		"SnapshotName": "full-config-snap",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	snaps := doDescribeSnapshots(t, h, "full-config-snap")
	require.Len(t, snaps, 1)

	snap, _ := snaps[0].(map[string]any)
	config, _ := snap["ClusterConfiguration"].(map[string]any)
	require.NotNil(t, config)

	assert.Equal(t, "redis", config["Engine"])
	assert.Equal(t, "sun:05:00-sun:06:00", config["MaintenanceWindow"])
	assert.Equal(t, "03:00-04:00", config["SnapshotWindow"])
	assert.EqualValues(t, 3, config["SnapshotRetentionLimit"])
}

func TestHandler_Snapshot_FilterByType(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		filterType     string
		wantMinResults int
	}{
		{"filter manual returns manual snaps", "manual", 1},
		{"filter automated returns automated snaps", "automated", 1},
		{"no filter returns all", "", 2},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			// Create cluster with retention > 0 to seed automated snapshot.
			doCreateCluster(t, h, map[string]any{
				"ClusterName":            "snap-filter-cluster",
				"NodeType":               "db.r6g.large",
				"ACLName":                "open-access",
				"SnapshotRetentionLimit": 1,
			})

			// Create a manual snapshot.
			manRec := doRequest(t, h, "CreateSnapshot", map[string]any{
				"ClusterName":  "snap-filter-cluster",
				"SnapshotName": "manual-snap",
			})
			require.Equal(t, http.StatusOK, manRec.Code)

			body := map[string]any{}
			if tt.filterType != "" {
				body["SnapshotType"] = tt.filterType
			}

			rec := doRequest(t, h, "DescribeSnapshots", body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			snaps, _ := resp["Snapshots"].([]any)
			assert.GreaterOrEqual(t, len(snaps), tt.wantMinResults,
				"filter=%q: expected >= %d snapshots", tt.filterType, tt.wantMinResults)
		})
	}
}

// -- Engine version validation (finding 3) ----------------------------------------

func TestHandler_CopySnapshot_OptionalTargetOverrides(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name            string
		targetBucket    string
		targetName      string
		wantCopyCreated bool
		wantStatus      int
	}{
		{
			name:            "copy without bucket creates new snapshot",
			targetBucket:    "",
			targetName:      "copy-dest",
			wantCopyCreated: true,
			wantStatus:      http.StatusOK,
		},
		{
			name:            "copy with TargetBucket returns source (S3 export)",
			targetBucket:    "my-s3-bucket",
			targetName:      "export-dest",
			wantCopyCreated: false,
			wantStatus:      http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)

			doCreateCluster(t, h, minimalClusterBody("copy-snap-cluster"))
			createSnapshot(t, h, map[string]any{
				"ClusterName":  "copy-snap-cluster",
				"SnapshotName": "source-snap",
			})

			body := map[string]any{
				"SourceSnapshotName": "source-snap",
				"TargetSnapshotName": tt.targetName,
			}
			if tt.targetBucket != "" {
				body["TargetBucket"] = tt.targetBucket
			}

			rec := doRequest(t, h, "CopySnapshot", body)
			require.Equal(t, tt.wantStatus, rec.Code, "body: %s", rec.Body)

			if tt.wantCopyCreated {
				// Verify the copy exists.
				snaps := doDescribeSnapshots(t, h, tt.targetName)
				assert.Len(t, snaps, 1, "copied snapshot should exist")
			}
		})
	}
}

// -- EnginePatchVersion: all engine families covered (finding 15) ----------------

// TestHandler_DescribeSnapshots_Filtered tests snapshot filtering paths.
func TestHandler_DescribeSnapshots_Filtered(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body      map[string]any
		name      string
		wantCount int
	}{
		{name: "filter by cluster name", body: map[string]any{"ClusterName": "snap-cl"}, wantCount: 1},
		{name: "filter by type manual", body: map[string]any{"SnapshotType": "manual"}, wantCount: 1},
		{name: "filter by source manual", body: map[string]any{"Source": "manual"}, wantCount: 1},
		{name: "filter by non-match", body: map[string]any{"ClusterName": "no-such"}, wantCount: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "snap-cl",
				"NodeType":    "db.r6g.large",
			})

			doRequest(t, h, "CreateSnapshot", map[string]any{
				"SnapshotName": "snap-001",
				"ClusterName":  "snap-cl",
			})

			rec := doRequest(t, h, "DescribeSnapshots", tt.body)
			assert.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

			snapshots := resp["Snapshots"].([]any)
			assert.Len(t, snapshots, tt.wantCount)
		})
	}
}

// TestHandler_CopySnapshot_ToS3Bucket tests CopySnapshot with TargetBucket.
func TestHandler_CopySnapshot_ToS3Bucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		wantStatus int
	}{
		{name: "copy to S3 bucket returns source snapshot", wantStatus: http.StatusOK},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			doRequest(t, h, "CreateCluster", map[string]any{
				"ClusterName": "src-cl",
				"NodeType":    "db.r6g.large",
			})

			doRequest(t, h, "CreateSnapshot", map[string]any{
				"SnapshotName": "src-snap",
				"ClusterName":  "src-cl",
			})

			rec := doRequest(t, h, "CopySnapshot", map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetBucket":       "my-s3-bucket",
			})

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

func TestHandler_AutomatedSnapshot_OnCreate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name                   string
		snapshotRetentionLimit int
		wantAutoSnap           bool
	}{
		{
			name:                   "retention limit > 0 seeds automated snapshot",
			snapshotRetentionLimit: 3,
			wantAutoSnap:           true,
		},
		{
			name:                   "retention limit 0 no automated snapshot",
			snapshotRetentionLimit: 0,
			wantAutoSnap:           false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, map[string]any{
				"ClusterName":            "snap-cluster",
				"NodeType":               "db.r6g.large",
				"ACLName":                "open-access",
				"SnapshotRetentionLimit": tt.snapshotRetentionLimit,
			})

			rec := doRequest(t, h, "DescribeSnapshots", map[string]any{
				"ClusterName":  "snap-cluster",
				"SnapshotType": "automated",
			})
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			snaps := resp["Snapshots"].([]any)

			if tt.wantAutoSnap {
				assert.NotEmpty(t, snaps, "expected automated snapshot to be created")
				snap := snaps[0].(map[string]any)
				assert.Equal(t, "automated", snap["SnapshotType"])
				assert.True(t, strings.HasPrefix(snap["Name"].(string), "automatic.snap-cluster"))
			} else {
				assert.Empty(t, snaps, "expected no automated snapshot")
			}
		})
	}
}

// -- DescribeSnapshots Source filter (Gap 15) ----------------------------------

func TestHandler_DescribeSnapshots_SourceFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create cluster with retention (seeds automated snapshot).
	createCluster(t, h, map[string]any{
		"ClusterName":            "snap-cluster",
		"NodeType":               "db.r6g.large",
		"ACLName":                "open-access",
		"SnapshotRetentionLimit": 1,
	})

	// Create manual snapshot.
	createSnapshot(t, h, map[string]any{
		"ClusterName":  "snap-cluster",
		"SnapshotName": "manual-snap",
	})

	tests := []struct {
		body             map[string]any
		name             string
		wantSnapshotType string
		wantMinCount     int
	}{
		{
			name:         "no filter returns all",
			body:         map[string]any{},
			wantMinCount: 2,
		},
		{
			name:             "source=manual returns only manual",
			body:             map[string]any{"Source": "manual"},
			wantMinCount:     1,
			wantSnapshotType: "manual",
		},
		{
			name:             "source=automated returns only automated",
			body:             map[string]any{"Source": "automated"},
			wantMinCount:     1,
			wantSnapshotType: "automated",
		},
		{
			name:         "filter by cluster name",
			body:         map[string]any{"ClusterName": "snap-cluster"},
			wantMinCount: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			rec := doRequest(t, h, "DescribeSnapshots", tt.body)
			require.Equal(t, http.StatusOK, rec.Code)

			var resp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
			snaps := resp["Snapshots"].([]any)
			assert.GreaterOrEqual(t, len(snaps), tt.wantMinCount)

			if tt.wantSnapshotType != "" {
				for _, s := range snaps {
					snap := s.(map[string]any)
					assert.Equal(t, tt.wantSnapshotType, snap["SnapshotType"])
				}
			}
		})
	}
}

// -- RestoreCluster from snapshot (Gap 17) -------------------------------------

func TestHandler_CopySnapshot_TargetBucket(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body        map[string]any
		name        string
		wantStatus  int
		wantNewSnap bool
	}{
		{
			name: "target bucket copies to S3 without creating new snapshot",
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "",
				"TargetBucket":       "my-s3-bucket",
			},
			wantStatus:  http.StatusOK,
			wantNewSnap: false,
		},
		{
			name: "normal copy creates new snapshot",
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus:  http.StatusOK,
			wantNewSnap: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createCluster(t, h, map[string]any{
				"ClusterName": "snap-cluster",
				"NodeType":    "db.r6g.large",
				"ACLName":     "open-access",
			})
			createSnapshot(t, h, map[string]any{
				"ClusterName":  "snap-cluster",
				"SnapshotName": "src-snap",
			})

			initialSnaps := memorydb.SnapshotCount(h.Backend.(*memorydb.InMemoryBackend))

			rec := doRequest(t, h, "CopySnapshot", tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				afterSnaps := memorydb.SnapshotCount(h.Backend.(*memorydb.InMemoryBackend))
				if tt.wantNewSnap {
					assert.Equal(t, initialSnaps+1, afterSnaps, "expected new snapshot to be created")
				} else {
					assert.Equal(t, initialSnaps, afterSnaps, "expected no new snapshot for S3 export")
				}
			}
		})
	}
}

// -- FailoverShard (Gap 10) ----------------------------------------------------

func TestHandler_SnapshotCRUD(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup      func(*memorydb.Handler)
		body       map[string]any
		name       string
		op         string
		wantStatus int
	}{
		{
			name: "create snapshot",
			op:   "CreateSnapshot",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
			},
			body: map[string]any{
				"ClusterName":  "test-cluster",
				"SnapshotName": "my-snap",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "create snapshot missing cluster name",
			op:         "CreateSnapshot",
			body:       map[string]any{"SnapshotName": "my-snap"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name:       "create snapshot missing snapshot name",
			op:         "CreateSnapshot",
			body:       map[string]any{"ClusterName": "test-cluster"},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "describe all snapshots",
			op:   "DescribeSnapshots",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				createSnapshot(t, h, map[string]any{
					"ClusterName":  "test-cluster",
					"SnapshotName": "my-snap",
				})
			},
			body:       map[string]any{},
			wantStatus: http.StatusOK,
		},
		{
			name:       "describe snapshot not found",
			op:         "DescribeSnapshots",
			body:       map[string]any{"SnapshotName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name:       "delete snapshot not found",
			op:         "DeleteSnapshot",
			body:       map[string]any{"SnapshotName": "no-such"},
			wantStatus: http.StatusNotFound,
		},
		{
			name: "copy snapshot",
			op:   "CopySnapshot",
			setup: func(h *memorydb.Handler) {
				createCluster(t, h, map[string]any{
					"ClusterName": "test-cluster",
					"NodeType":    "db.r6g.large",
					"ACLName":     "open-access",
				})
				createSnapshot(t, h, map[string]any{
					"ClusterName":  "test-cluster",
					"SnapshotName": "src-snap",
				})
			},
			body: map[string]any{
				"SourceSnapshotName": "src-snap",
				"TargetSnapshotName": "dst-snap",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "copy snapshot missing source",
			op:         "CopySnapshot",
			body:       map[string]any{"TargetSnapshotName": "dst-snap"},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			if tt.setup != nil {
				tt.setup(h)
			}

			rec := doRequest(t, h, tt.op, tt.body)
			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// -- SubnetGroup CRUD ----------------------------------------------------------

func TestHandler_DescribeSnapshots_Pagination(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, map[string]any{
		"ClusterName": "test-cluster",
		"NodeType":    "db.r6g.large",
		"ACLName":     "open-access",
	})

	for _, name := range []string{"snap-a", "snap-b", "snap-c"} {
		createSnapshot(t, h, map[string]any{
			"ClusterName":  "test-cluster",
			"SnapshotName": name,
		})
	}

	rec := doRequest(t, h, "DescribeSnapshots", map[string]any{"MaxResults": 2})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	snaps := resp["Snapshots"].([]any)
	assert.Len(t, snaps, 2)
	assert.NotEmpty(t, resp["NextToken"])
}
