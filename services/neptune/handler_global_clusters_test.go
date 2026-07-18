package neptune_test

import (
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/neptune"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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
	createCluster(t, h, "gc-fo-secondary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-failover"},
		"SourceDBClusterIdentifier": {"gc-fo-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-failover"},
		"TargetDbClusterIdentifier": {"gc-fo-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-failover")
}

func TestGlobalCluster_SwitchoverGlobalCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "gc-sw-primary")
	createCluster(t, h, "gc-sw-secondary")
	doRequest(t, h, url.Values{
		"Action":                    {"CreateGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-switchover"},
		"SourceDBClusterIdentifier": {"gc-sw-primary"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                    {"SwitchoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-switchover"},
		"TargetDbClusterIdentifier": {"gc-sw-secondary"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
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
		extraVals    url.Values
		wantContains string
	}{
		{
			name:         "modify",
			action:       "ModifyGlobalCluster",
			extraVals:    url.Values{},
			wantContains: "gc-ops",
		},
		{
			name:         "failover",
			action:       "FailoverGlobalCluster",
			extraVals:    url.Values{"TargetDbClusterIdentifier": {"some-target"}},
			wantContains: "gc-ops",
		},
		{
			name:         "switchover",
			action:       "SwitchoverGlobalCluster",
			extraVals:    url.Values{"TargetDbClusterIdentifier": {"some-target"}},
			wantContains: "gc-ops",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			doRequest(t, h, url.Values{
				"Action":                  {"CreateGlobalCluster"},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"gc-ops"},
			})
			vals := url.Values{
				"Action":                  {tt.action},
				"Version":                 {"2014-10-31"},
				"GlobalClusterIdentifier": {"gc-ops"},
			}
			maps.Copy(vals, tt.extraVals)
			rr := doRequest(t, h, vals)
			assert.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
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

	// Create global cluster
	rr := doRequest(t, h, url.Values{
		"Action":                  {"CreateGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "gc-01")

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

	// Failover
	rr = doRequest(t, h, url.Values{
		"Action":                    {"FailoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-01"},
		"TargetDbClusterIdentifier": {"some-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Switchover
	rr = doRequest(t, h, url.Values{
		"Action":                    {"SwitchoverGlobalCluster"},
		"Version":                   {"2014-10-31"},
		"GlobalClusterIdentifier":   {"gc-01"},
		"TargetDbClusterIdentifier": {"some-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Remove from global cluster
	rr = doRequest(t, h, url.Values{
		"Action":                  {"RemoveFromGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-01"},
		"DbClusterIdentifier":     {"some-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Delete
	rr = doRequest(t, h, url.Values{
		"Action":                  {"DeleteGlobalCluster"},
		"Version":                 {"2014-10-31"},
		"GlobalClusterIdentifier": {"gc-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
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
