package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

func TestHandler_DescribeGlobalClusters(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "DescribeGlobalClustersResponse")
}

func TestHandler_CreateGlobalCluster(t *testing.T) {
	t.Parallel()

	tests := []struct {
		setup        func(*neptune.Handler)
		vals         url.Values
		name         string
		wantContains string
		wantStatus   int
	}{
		{
			name: "create_global_cluster_success",
			vals: url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "my-global-cluster",
		},
		{
			name: "create_global_cluster_with_source",
			setup: func(h *neptune.Handler) {
				createCluster(t, h, "source-cluster")
			},
			vals: url.Values{
				"Action":                    {"CreateGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"global-with-source"},
				"SourceDBClusterIdentifier": {"source-cluster"},
			},
			wantStatus:   http.StatusOK,
			wantContains: "global-with-source",
		},
		{
			name: "create_global_cluster_duplicate",
			setup: func(h *neptune.Handler) {
				doRequest(t, h, url.Values{
					"Action":                  {"CreateGlobalCluster"},
					"Version":                 {"2014-10-31"},
					"GlobalClusterIdentifier": {"my-global-cluster"},
				})
			},
			vals: url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"my-global-cluster"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterAlreadyExistsFault",
		},
		{
			name: "create_global_cluster_missing_id",
			vals: url.Values{
				"Action":  {"CreateGlobalCluster"},
				"Version": {"2014-10-31"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "InvalidParameterValue",
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

// ---- GlobalCluster comprehensive coverage ----

func TestGlobalCluster_CreateWithSourceCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-src-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-with-src"},
		"SourceDBClusterIdentifier": {"gc-src-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "gc-with-src")
	assert.Contains(t, body, "available")
}

func TestGlobalCluster_DescribeMultiple(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	for _, id := range []string{"gc-multi-1", "gc-multi-2", "gc-multi-3"} {
		doRequest(t, h, url.Values{
			"Action":                  {"CreateGlobalCluster"},
			"Version":                 {"2014-10-31"},
			"GlobalClusterIdentifier": {id},
		})
	}

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "gc-multi-1")
	assert.Contains(t, body, "gc-multi-2")
	assert.Contains(t, body, "gc-multi-3")
}

func TestGlobalCluster_ModifyGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-modify"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                  {"ModifyGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-modify"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-modify")
}

func TestGlobalCluster_FailoverGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-fo-primary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-failover"},
		"SourceDBClusterIdentifier": {"gc-fo-primary"},
	})
	createClusterInGlobalCluster(t, h, "gc-fo-secondary", "gc-failover")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-failover"},
		"TargetDbClusterIdentifier": {"gc-fo-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "gc-failover")
}

func TestGlobalCluster_SwitchoverGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-sw-primary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-switchover"},
		"SourceDBClusterIdentifier": {"gc-sw-primary"},
	})
	createClusterInGlobalCluster(t, h, "gc-sw-secondary", "gc-switchover")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"SwitchoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-switchover"},
		"TargetDbClusterIdentifier": {"gc-sw-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "gc-switchover")
}

func TestGlobalCluster_RemoveFromGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-rm-primary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-remove"},
		"SourceDBClusterIdentifier": {"gc-rm-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                  {"RemoveFromGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-remove"},
		"DbClusterIdentifier":     {"arn:aws:rds:us-east-1:000000000000:cluster:gc-rm-primary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

func TestGlobalCluster_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-dup"},
	})
	rr := doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-dup"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterAlreadyExistsFault")
}

func TestGlobalCluster_DeleteNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"nonexistent-gc"},
	})
	require.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterNotFoundFault")
}

// TestDescribeGlobalClusters_WithData verifies DescribeGlobalClusters returns stored clusters.
func TestDescribeGlobalClusters_WithData(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"my-global-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "my-global-cluster")
}

// TestGlobalClusterAlreadyExists verifies proper error on duplicate.
func TestGlobalClusterAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"dup-gc"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"dup-gc"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterAlreadyExistsFault")
}

// TestCreateGlobalCluster_WithSourceCluster verifies member is populated.
func TestCreateGlobalCluster_WithSourceCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "source-for-gc")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-with-source"},
		"SourceDBClusterIdentifier": {"source-for-gc"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-with-source")
	assert.Contains(t, rr.Body.String(), "IsWriter")
}

// TestCreateGlobalCluster_MissingID verifies validation error on empty global cluster ID.
func TestCreateGlobalCluster_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"CreateGlobalCluster"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// --- Global Cluster lifecycle ---

func TestGlobalCluster_DeleteModifyFailoverSwitchover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
		wantStatus   int
	}{
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":                  {"DeleteGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"no-such-gc"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name: "modify_not_found",
			vals: url.Values{
				"Action":                  {"ModifyGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"no-such-gc"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name: "failover_not_found",
			vals: url.Values{
				"Action":                    {"FailoverGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"no-such-gc"},
				"TargetDbClusterIdentifier": {"target"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name: "switchover_not_found",
			vals: url.Values{
				"Action":                    {"SwitchoverGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"no-such-gc"},
				"TargetDbClusterIdentifier": {"target"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
		{
			name: "remove_from_not_found",
			vals: url.Values{
				"Action":                  {"RemoveFromGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"no-such-gc"},
				"DbClusterIdentifier":     {"some-arn"},
			},
			wantStatus:   http.StatusBadRequest,
			wantContains: "GlobalClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestGlobalCluster_DeleteLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-del"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "gc-del")

	rr = doRequest(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-del"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "gc-del")

	// second delete must fail
	rr = doRequest(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-del"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "GlobalClusterNotFoundFault")
}

func TestGlobalCluster_ModifyFailoverSwitchover(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		wantContains string
	}{
		{name: "modify", action: "ModifyGlobalCluster", wantContains: "gc-ops"},
		{name: "failover", action: "FailoverGlobalCluster", wantContains: "gc-ops-secondary"},
		{name: "switchover", action: "SwitchoverGlobalCluster", wantContains: "gc-ops-secondary"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			createCluster(t, h, "gc-ops-primary")
			doRequest(t, h, url.Values{
				"Action":                    {"CreateGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"gc-ops"},
				"SourceDBClusterIdentifier": {"gc-ops-primary"},
			})
			vals := url.Values{
				"Action":                  {tt.action},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"gc-ops"},
			}
			if tt.action != "ModifyGlobalCluster" {
				createClusterInGlobalCluster(t, h, "gc-ops-secondary", "gc-ops")
				vals.Set("TargetDbClusterIdentifier", "gc-ops-secondary")
			}
			rr := doRequest(t, h, vals)
			assert.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestGlobalCluster_FailoverGlobalCluster_UnknownTargetRejected verifies the
// real-AWS behavior this backend used to get wrong: FailoverGlobalCluster/
// SwitchoverGlobalCluster to a TargetDbClusterIdentifier this backend
// tracks no DB cluster for at all must reject with DBClusterNotFoundFault
// (previously a silent no-op), and to a real cluster that is not a member of
// the named global cluster must reject with InvalidDBClusterStateFault.
func TestGlobalCluster_FailoverGlobalCluster_UnknownTargetRejected(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		action       string
		setup        func(t *testing.T, h *neptune.Handler)
		target       string
		wantContains string
	}{
		{
			name:         "failover to nonexistent cluster",
			action:       "FailoverGlobalCluster",
			target:       "no-such-cluster",
			wantContains: "DBClusterNotFoundFault",
		},
		{
			name:   "failover to a real cluster that is not a member",
			action: "FailoverGlobalCluster",
			setup: func(t *testing.T, h *neptune.Handler) {
				t.Helper()
				createCluster(t, h, "gc-unrelated-cluster")
			},
			target:       "gc-unrelated-cluster",
			wantContains: "InvalidDBClusterStateFault",
		},
		{
			name:         "switchover to nonexistent cluster",
			action:       "SwitchoverGlobalCluster",
			target:       "no-such-cluster",
			wantContains: "DBClusterNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "gc-target-primary")
			doRequest(t, h, url.Values{
				"Action":                    {"CreateGlobalCluster"},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"gc-target-checks"},
				"SourceDBClusterIdentifier": {"gc-target-primary"},
			})
			if tt.setup != nil {
				tt.setup(t, h)
			}

			rr := doRequest(t, h, url.Values{
				"Action":                    {tt.action},
				"Version":                   {"2014-10-31"},
				"GlobalClusterIdentifier":   {"gc-target-checks"},
				"TargetDbClusterIdentifier": {tt.target},
			})
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// TestGlobalCluster_FailoverGlobalCluster_AlreadyPrimaryRejected verifies
// failing over to the current writer (nothing to promote) is rejected as
// InvalidDBClusterStateFault rather than silently succeeding.
func TestGlobalCluster_FailoverGlobalCluster_AlreadyPrimaryRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-already-primary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-already-writer"},
		"SourceDBClusterIdentifier": {"gc-already-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-already-writer"},
		"TargetDbClusterIdentifier": {"gc-already-primary"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "InvalidDBClusterStateFault")
}

func TestGlobalCluster_RemoveFrom(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-member-cluster")

	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-remove"},
		"SourceDBClusterIdentifier": {"gc-member-cluster"},
	})

	// DescribeGlobalClusters shows the global cluster
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "gc-remove")

	// RemoveFromGlobalCluster succeeds (even with arbitrary ARN — backend only filters by ARN)
	rr = doRequest(t, h, url.Values{
		"Action":                  {"RemoveFromGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-remove"},
		"DbClusterIdentifier":     {"arn:aws:rds:us-east-1:000000000000:cluster:gc-member-cluster"},
	})
	assert.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "gc-remove")
}

// --- Global Cluster operations ---

// TestCreateDescribeDeleteGlobalCluster tests full global cluster lifecycle.
func TestCreateDescribeDeleteGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-01-primary")

	// Create global cluster, with a real primary so Failover/Switchover below
	// have a genuine member to promote (an arbitrary identifier is now
	// correctly rejected -- see TestGlobalCluster_FailoverGlobalCluster_UnknownTargetRejected).
	rr := doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-01"},
		"SourceDBClusterIdentifier": {"gc-01-primary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-01")
	createClusterInGlobalCluster(t, h, "gc-01-secondary", "gc-01")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-01")

	// Modify
	rr = doRequest(t, h, url.Values{
		"Action":                  {"ModifyGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Failover promotes the secondary
	rr = doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-01"},
		"TargetDbClusterIdentifier": {"gc-01-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// Switchover promotes the (now-demoted) original primary back
	rr = doRequest(t, h, url.Values{
		"Action":                    {"SwitchoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-01"},
		"TargetDbClusterIdentifier": {"gc-01-primary"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// Remove both members so DeleteGlobalCluster's no-attached-members
	// precondition is satisfied.
	for _, id := range []string{"gc-01-primary", "gc-01-secondary"} {
		rr = doRequest(t, h, url.Values{
			"Action":                  {"RemoveFromGlobalCluster"},
			"Version":                 {"2014-10-31"},
			"GlobalClusterIdentifier": {"gc-01"},
			"DbClusterIdentifier":     {"arn:aws:neptune:us-east-1:000000000000:cluster:" + id},
		})
		require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	}

	// Delete
	rr = doRequest(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
}

// TestGlobalCluster_HasArnResourceIdEngine verifies GlobalCluster includes ARN/ResourceId/Engine fields.
func TestGlobalCluster_HasArnResourceIdEngine(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		globalID     string
		wantContains []string
	}{
		{
			name:     "global_cluster_has_arn_and_engine",
			globalID: "my-global",
			wantContains: []string{
				"GlobalClusterArn",
				"arn:",
				"GlobalClusterResourceId",
				"cluster-my-global",
				"Engine",
				"neptune",
				"EngineVersion",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {tt.globalID},
			})
			require.Equal(t, http.StatusOK, rr.Code)
			body := rr.Body.String()
			for _, want := range tt.wantContains {
				assert.Contains(t, body, want)
			}
		})
	}
}

// TestGlobalCluster_ModifyGlobalCluster_PersistsDeletionProtectionAndEngineVersion
// locks the core fix: ModifyGlobalCluster used to accept no new values at all
// (a disguised no-op by construction, not just by behavior), so nothing a
// caller sent could ever change anything. It must now genuinely mutate
// DeletionProtection/EngineVersion.
func TestGlobalCluster_ModifyGlobalCluster_PersistsDeletionProtectionAndEngineVersion(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-modify-real"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                  {"ModifyGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-modify-real"},
		"DeletionProtection":      {"true"},
		"EngineVersion":           {"1.4.0.0"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "<DeletionProtection>true</DeletionProtection>")
	assert.Contains(t, body, "<EngineVersion>1.4.0.0</EngineVersion>")

	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body = rr.Body.String()
	assert.Contains(t, body, "<DeletionProtection>true</DeletionProtection>")
	assert.Contains(t, body, "<EngineVersion>1.4.0.0</EngineVersion>")
}

// TestGlobalCluster_ModifyGlobalCluster_Rename verifies
// NewGlobalClusterIdentifier renames the global cluster in place, including
// its ARN, and that the old identifier no longer resolves as the
// GlobalClusterIdentifier (GlobalClusterResourceId, like AWS's other
// resource IDs, is immutable and intentionally does NOT change on rename --
// it is derived from the original creation-time identifier).
func TestGlobalCluster_ModifyGlobalCluster_Rename(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-old-name"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                     {"ModifyGlobalCluster"},
		"Version":                    {"2014-10-31"},
		"GlobalClusterIdentifier":    {"gc-old-name"},
		"NewGlobalClusterIdentifier": {"gc-new-name"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-new-name")

	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	body := rr.Body.String()
	assert.Contains(t, body, "<GlobalClusterIdentifier>gc-new-name</GlobalClusterIdentifier>")
	assert.NotContains(t, body, "<GlobalClusterIdentifier>gc-old-name</GlobalClusterIdentifier>")
}

// TestGlobalCluster_FailoverGlobalCluster_PromotesRealTarget locks the core
// fix: FailoverGlobalCluster used to validate the global cluster and then
// return an unchanged clone -- GlobalClusterMembers.IsWriter never flipped
// regardless of TargetDbClusterIdentifier. It must now genuinely promote a
// target that resolves to a real DB cluster.
func TestGlobalCluster_FailoverGlobalCluster_PromotesRealTarget(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-real-primary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-real-failover"},
		"SourceDBClusterIdentifier": {"gc-real-primary"},
	})
	createClusterInGlobalCluster(t, h, "gc-real-secondary", "gc-real-failover")

	rr := doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-real-failover"},
		"TargetDbClusterIdentifier": {"gc-real-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	body := rr.Body.String()
	assert.Contains(t, body, "gc-real-secondary")
	assert.Contains(t, body, "<IsWriter>true</IsWriter>")

	// The old writer (gc-real-primary) must now be demoted.
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeGlobalClusters"},
		"Version": {"2014-10-31"},
	})
	assert.Contains(t, rr.Body.String(), "<IsWriter>false</IsWriter>")
}
