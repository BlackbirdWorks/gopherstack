package neptune_test

// handler_accuracy_batch2_test.go — Neptune AWS-accuracy audit batch-2 (go-1kjc).
//
// Covers HTTP-level ops not previously exercised:
//   - Global cluster lifecycle: Delete, Modify, Failover, Switchover, RemoveFrom + not-found paths
//   - Event subscription lifecycle: Describe, Modify, Delete, RemoveSourceIdentifier +
//     DescribeEventCategories, DescribeEvents
//   - Cluster endpoint lifecycle: Describe (by ID, by cluster), Modify, Delete + not-found
//   - DB parameter group lifecycle: Describe, Modify, Reset, Delete, DescribeDBParameters
//   - Cluster parameter group extended: Modify, Reset, DescribeDBClusterParameters
//   - Snapshot attributes: DescribeDBClusterSnapshotAttributes, ModifyDBClusterSnapshotAttribute
//   - Role operations: RemoveRoleFromDBCluster round-trip + not-found
//   - Restore operations: RestoreDBClusterFromSnapshot, RestoreDBClusterToPointInTime
//   - ModifyDBSubnetGroup
//   - PromoteReadReplicaDBCluster
//   - DescribePendingMaintenanceActions
//   - DescribeValidDBInstanceModifications
//   - DescribeEngineDefaultClusterParameters, DescribeEngineDefaultParameters

import (
	"maps"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Global Cluster lifecycle ---

func TestBatch2Ops_GlobalCluster_DeleteModifyFailoverSwitchover(t *testing.T) {
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

func TestBatch2Ops_GlobalCluster_DeleteLifecycle(t *testing.T) {
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

func TestBatch2Ops_GlobalCluster_ModifyFailoverSwitchover(t *testing.T) {
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

func TestBatch2Ops_GlobalCluster_RemoveFrom(t *testing.T) {
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

// --- Event subscription lifecycle ---

func TestBatch2Ops_EventSubscription_DescribeModifyDeleteLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// create subscription
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:my-topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// describe all
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeEventSubscriptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")
	assert.Contains(t, rr.Body.String(), "my-topic")

	// describe by name
	rr = doRequest(t, h, url.Values{
		"Action":           {"DescribeEventSubscriptions"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// modify
	rr = doRequest(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:new-topic"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// delete
	rr = doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-1")

	// second delete must fail
	rr = doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-1"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionNotFound")
}

func TestBatch2Ops_EventSubscription_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
	}{
		{
			name: "describe_not_found",
			vals: url.Values{
				"Action":           {"DescribeEventSubscriptions"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such"},
			},
			wantContains: "SubscriptionNotFound",
		},
		{
			name: "modify_not_found",
			vals: url.Values{
				"Action":           {"ModifyEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such"},
				"SnsTopicArn":      {"arn:x"},
			},
			wantContains: "SubscriptionNotFound",
		},
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":           {"DeleteEventSubscription"},
				"Version":          {"2014-10-31"},
				"SubscriptionName": {"no-such"},
			},
			wantContains: "SubscriptionNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

func TestBatch2Ops_EventSubscription_RemoveSourceIdentifier(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-src"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:topic"},
	})
	doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-src"},
		"SourceIdentifier": {"my-cluster"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"RemoveSourceIdentifierFromSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-src"},
		"SourceIdentifier": {"my-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "sub-src")
}

func TestBatch2Ops_DescribeEventCategories(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEventCategories"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "db-cluster")
	assert.Contains(t, rr.Body.String(), "db-instance")
	assert.Contains(t, rr.Body.String(), "failover")
}

func TestBatch2Ops_DescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "DescribeEventsResponse")
}

// --- Cluster endpoint lifecycle ---

func TestBatch2Ops_ClusterEndpoint_DescribeModifyDelete(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-lifecycle-cluster")

	// create
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
		"DBClusterIdentifier":         {"ep-lifecycle-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// describe by endpoint ID
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterEndpoints"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")
	assert.Contains(t, rr.Body.String(), "READER")

	// describe by cluster ID
	rr = doRequest(t, h, url.Values{
		"Action":              {"DescribeDBClusterEndpoints"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ep-lifecycle-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// describe all (no filter)
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusterEndpoints"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// modify
	rr = doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
		"EndpointType":                {"WRITER"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "ep-lifecycle")

	// delete
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// delete again must fail
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-lifecycle"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterEndpointNotFoundFault")
}

func TestBatch2Ops_ClusterEndpoint_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
	}{
		{
			name: "modify_not_found",
			vals: url.Values{
				"Action":                      {"ModifyDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"no-such"},
				"EndpointType":                {"READER"},
			},
			wantContains: "DBClusterEndpointNotFoundFault",
		},
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":                      {"DeleteDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {"no-such"},
			},
			wantContains: "DBClusterEndpointNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- DB parameter group lifecycle ---

func TestBatch2Ops_DBParameterGroup_FullLifecycle(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// create
	rr := doRequest(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"pg-full"},
		"DBParameterGroupFamily": {"neptune1.3"},
		"Description":            {"test group"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "pg-full")

	// describe all
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeDBParameterGroups"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "pg-full")

	// describe by name
	rr = doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameterGroups"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-full"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "pg-full")

	// describe parameters
	rr = doRequest(t, h, url.Values{
		"Action":               {"DescribeDBParameters"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-full"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "DescribeDBParametersResponse")

	// modify
	rr = doRequest(t, h, url.Values{
		"Action":               {"ModifyDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-full"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "pg-full")

	// reset
	rr = doRequest(t, h, url.Values{
		"Action":               {"ResetDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-full"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "pg-full")

	// delete
	rr = doRequest(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-full"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())

	// delete again must fail
	rr = doRequest(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-full"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupNotFound")
}

func TestBatch2Ops_DBParameterGroup_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		vals         url.Values
		wantContains string
	}{
		{
			name: "describe_not_found",
			vals: url.Values{
				"Action":               {"DescribeDBParameterGroups"},
				"Version":              {"2014-10-31"},
				"DBParameterGroupName": {"no-such"},
			},
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name: "describe_params_not_found",
			vals: url.Values{
				"Action":               {"DescribeDBParameters"},
				"Version":              {"2014-10-31"},
				"DBParameterGroupName": {"no-such"},
			},
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name: "modify_not_found",
			vals: url.Values{
				"Action":               {"ModifyDBParameterGroup"},
				"Version":              {"2014-10-31"},
				"DBParameterGroupName": {"no-such"},
			},
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name: "reset_not_found",
			vals: url.Values{
				"Action":               {"ResetDBParameterGroup"},
				"Version":              {"2014-10-31"},
				"DBParameterGroupName": {"no-such"},
			},
			wantContains: "DBParameterGroupNotFound",
		},
		{
			name: "delete_not_found",
			vals: url.Values{
				"Action":               {"DeleteDBParameterGroup"},
				"Version":              {"2014-10-31"},
				"DBParameterGroupName": {"no-such"},
			},
			wantContains: "DBParameterGroupNotFound",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- Cluster parameter group extended ops ---

func TestBatch2Ops_ClusterParameterGroup_ModifyResetDescribeParams(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-ext"},
		"DBParameterGroupFamily":      {"neptune1.3"},
		"Description":                 {"extended test"},
	})

	// modify
	rr := doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-ext"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "cpg-ext")

	// reset
	rr = doRequest(t, h, url.Values{
		"Action":                      {"ResetDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-ext"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "cpg-ext")

	// describe parameters
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-ext"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "DescribeDBClusterParametersResponse")

	// describe parameters with unknown group returns error
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameters"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"no-such-cpg"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupNotFound")
}

// --- Snapshot attributes ---

func TestBatch2Ops_SnapshotAttributes(t *testing.T) {
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

// --- Role operations ---

func TestBatch2Ops_RemoveRoleFromDBCluster(t *testing.T) {
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

func TestBatch2Ops_RemoveRoleFromDBCluster_RoundTrip(t *testing.T) {
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

// --- Restore operations ---

func TestBatch2Ops_RestoreDBClusterFromSnapshot(t *testing.T) {
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

func TestBatch2Ops_RestoreDBClusterToPointInTime(t *testing.T) {
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

// --- ModifyDBSubnetGroup ---

func TestBatch2Ops_ModifyDBSubnetGroup(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		groupName    string
		description  string
		wantContains string
		wantStatus   int
		setupGroup   bool
	}{
		{
			name:         "success",
			setupGroup:   true,
			groupName:    "sg-modify",
			description:  "modified description",
			wantStatus:   http.StatusOK,
			wantContains: "sg-modify",
		},
		{
			name:         "not_found",
			setupGroup:   false,
			groupName:    "no-such-sg",
			description:  "desc",
			wantStatus:   http.StatusBadRequest,
			wantContains: "DBSubnetGroupNotFoundFault",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			if tt.setupGroup {
				doRequest(t, h, url.Values{
					"Action":                       {"CreateDBSubnetGroup"},
					"Version":                      {"2014-10-31"},
					"DBSubnetGroupName":            {tt.groupName},
					"DBSubnetGroupDescription":     {"initial description"},
					"SubnetIds.SubnetIdentifier.1": {"subnet-aaa"},
				})
			}
			rr := doRequest(t, h, url.Values{
				"Action":                   {"ModifyDBSubnetGroup"},
				"Version":                  {"2014-10-31"},
				"DBSubnetGroupName":        {tt.groupName},
				"DBSubnetGroupDescription": {tt.description},
			})
			assert.Equal(t, tt.wantStatus, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
		})
	}
}

// --- PromoteReadReplicaDBCluster ---

func TestBatch2Ops_PromoteReadReplicaDBCluster(t *testing.T) {
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

// --- Stub ops ---

func TestBatch2Ops_DescribePendingMaintenanceActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribePendingMaintenanceActions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "DescribePendingMaintenanceActionsResponse")
}

func TestBatch2Ops_DescribeValidDBInstanceModifications(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeValidDBInstanceModifications"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
	assert.Contains(t, rr.Body.String(), "DescribeValidDBInstanceModificationsResponse")
	assert.Contains(t, rr.Body.String(), "db.r5.large")
	assert.Contains(t, rr.Body.String(), "db.r6g.large")
}

func TestBatch2Ops_DescribeEngineDefaultParameters(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		family       string
		action       string
		wantFamily   string
		wantContains string
	}{
		{
			name:         "cluster_params_default_family",
			action:       "DescribeEngineDefaultClusterParameters",
			family:       "",
			wantContains: "DescribeEngineDefaultClusterParametersResponse",
			wantFamily:   "neptune1.3",
		},
		{
			name:         "cluster_params_explicit_family",
			action:       "DescribeEngineDefaultClusterParameters",
			family:       "neptune1.2",
			wantContains: "neptune1.2",
			wantFamily:   "neptune1.2",
		},
		{
			name:         "instance_params_default_family",
			action:       "DescribeEngineDefaultParameters",
			family:       "",
			wantContains: "DescribeEngineDefaultParametersResponse",
			wantFamily:   "neptune1.3",
		},
		{
			name:         "instance_params_explicit_family",
			action:       "DescribeEngineDefaultParameters",
			family:       "neptune1.1",
			wantContains: "neptune1.1",
			wantFamily:   "neptune1.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			h := newTestHandler(t)
			vals := url.Values{
				"Action":  {tt.action},
				"Version": {"2014-10-31"},
			}
			if tt.family != "" {
				vals["DBParameterGroupFamily"] = []string{tt.family}
			}
			rr := doRequest(t, h, vals)
			require.Equal(t, http.StatusOK, rr.Code, rr.Body.String())
			assert.Contains(t, rr.Body.String(), tt.wantContains)
			assert.Contains(t, rr.Body.String(), tt.wantFamily)
		})
	}
}
