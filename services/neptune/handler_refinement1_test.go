package neptune_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// TestRefinement1_BackendReset verifies that Reset() clears all maps.
func TestRefinement1_BackendReset(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddClusterInternal("c1")
	backend.AddClusterInternal("c2")

	require.Equal(t, 2, neptune.ClusterCount(backend))

	backend.Reset()

	assert.Equal(t, 0, neptune.ClusterCount(backend))
	assert.Equal(t, 0, neptune.InstanceCount(backend))
	assert.Equal(t, 0, neptune.SubnetGroupCount(backend))
	assert.Equal(t, 0, neptune.ClusterParameterGroupCount(backend))
	assert.Equal(t, 0, neptune.ClusterSnapshotCount(backend))
	assert.Equal(t, 0, neptune.ParameterGroupCount(backend))
	assert.Equal(t, 0, neptune.ClusterEndpointCount(backend))
	assert.Equal(t, 0, neptune.EventSubscriptionCount(backend))
	assert.Equal(t, 0, neptune.GlobalClusterCount(backend))
	assert.Equal(t, 0, neptune.TagCount(backend))
}

// TestRefinement1_HandlerReset verifies that Handler.Reset() delegates to the backend.
func TestRefinement1_HandlerReset(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "c1")
	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	_ = neptune.NewHandler(backend)
	// Reset via handler
	createCluster(t, h, "del-me")
	h.Reset()

	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusters"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusOK, rr.Code)
	assert.NotContains(t, rr.Body.String(), "del-me")
}

// TestRefinement1_AccountID verifies AccountID is returned from the backend.
func TestRefinement1_AccountID(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("123456789012", "eu-west-1")
	assert.Equal(t, "123456789012", backend.AccountID())
	assert.Equal(t, "eu-west-1", backend.Region())
}

// TestRefinement1_ProviderInit_NilCtx verifies ErrNilAppContext is returned when nil ctx is passed.
func TestRefinement1_ProviderInit_NilCtx(t *testing.T) {
	t.Parallel()

	p := &neptune.Provider{}
	_, err := p.Init(nil)
	require.ErrorIs(t, err, neptune.ErrNilAppContext)
}

// TestRefinement1_ProviderInit_ValidCtx verifies Init succeeds with a valid context.
func TestRefinement1_ProviderInit_ValidCtx(t *testing.T) {
	t.Parallel()

	p := &neptune.Provider{}
	svc, err := p.Init(&service.AppContext{})
	require.NoError(t, err)
	assert.NotNil(t, svc)
}

// TestRefinement1_GetSupportedOperations_AllOps verifies the expected ops count and sorting.
func TestRefinement1_GetSupportedOperations_AllOps(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()
	require.NotEmpty(t, ops)

	// Verify all 10 new ops are present
	expectedNew := []string{
		"AddRoleToDBCluster",
		"AddSourceIdentifierToSubscription",
		"ApplyPendingMaintenanceAction",
		"CopyDBClusterParameterGroup",
		"CopyDBClusterSnapshot",
		"CopyDBParameterGroup",
		"CreateDBClusterEndpoint",
		"CreateDBParameterGroup",
		"CreateEventSubscription",
		"CreateGlobalCluster",
	}
	for _, op := range expectedNew {
		assert.Contains(t, ops, op, "missing op: %s", op)
	}

	// Verify sorted
	sorted := make([]string, len(ops))
	copy(sorted, ops)
	sort.Strings(sorted)
	assert.Equal(t, sorted, ops, "GetSupportedOperations should return sorted list")

	assert.Len(t, ops, neptune.HandlerOpsLen(h))
}

// TestRefinement1_PersistenceRoundTrip verifies Snapshot/Restore preserves all state.
func TestRefinement1_PersistenceRoundTrip(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddClusterInternal("persist-cluster")
	backend.AddSnapshotInternal("persist-snap", "persist-cluster")
	backend.AddClusterParameterGroupInternal("persist-pg", "neptune1.3")
	backend.AddParameterGroupInternal("persist-dpg", "neptune1.3")
	backend.AddEventSubscriptionInternal("persist-sub", "arn:aws:sns:us-east-1:000000000000:test")

	data := backend.Snapshot()
	require.NotEmpty(t, data)

	// Verify it's valid JSON
	var raw map[string]any
	require.NoError(t, json.Unmarshal(data, &raw))

	restored := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, restored.Restore(data))

	assert.Equal(t, 1, neptune.ClusterCount(restored))
	assert.Equal(t, 1, neptune.ClusterSnapshotCount(restored))
	assert.Equal(t, 1, neptune.ClusterParameterGroupCount(restored))
	assert.Equal(t, 1, neptune.ParameterGroupCount(restored))
	assert.Equal(t, 1, neptune.EventSubscriptionCount(restored))
}

// TestRefinement1_DescribeGlobalClusters_WithData verifies DescribeGlobalClusters returns stored clusters.
func TestRefinement1_DescribeGlobalClusters_WithData(t *testing.T) {
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

// TestRefinement1_DeleteDBCluster_CascadesClearRoles verifies IAM roles are removed when cluster is deleted.
func TestRefinement1_DeleteDBCluster_CascadesClearRoles(t *testing.T) {
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
	})

	assert.Equal(t, 0, neptune.ClusterRoleCount(backend, "cascade-cluster"))
	assert.Equal(t, 0, neptune.ClusterCount(backend))
}

// TestRefinement1_DeleteDBCluster_CascadesClearEndpoints verifies endpoints are removed when cluster is deleted.
func TestRefinement1_DeleteDBCluster_CascadesClearEndpoints(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	hb := neptune.NewHandler(backend)

	backend.AddClusterInternal("ep-cluster")

	doRequest(t, hb, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-1"},
		"DBClusterIdentifier":         {"ep-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, 1, neptune.ClusterEndpointCount(backend))

	doRequest(t, hb, url.Values{
		"Action":              {"DeleteDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"ep-cluster"},
	})

	assert.Equal(t, 0, neptune.ClusterEndpointCount(backend))
}

// TestRefinement1_EndpointType_Validation verifies valid/invalid EndpointType.
func TestRefinement1_EndpointType_Validation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		endpointType string
		wantStatus   int
	}{
		{name: "reader_valid", endpointType: "READER", wantStatus: http.StatusOK},
		{name: "writer_valid", endpointType: "WRITER", wantStatus: http.StatusOK},
		{name: "custom_valid", endpointType: "CUSTOM", wantStatus: http.StatusOK},
		{name: "any_valid", endpointType: "ANY", wantStatus: http.StatusOK},
		{name: "empty_defaults_reader", endpointType: "", wantStatus: http.StatusOK},
		{name: "invalid_type", endpointType: "INVALID", wantStatus: http.StatusBadRequest},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			createCluster(t, h, "ep-type-cluster")

			epID := tt.name + "-ep"
			vals := url.Values{
				"Action":                      {"CreateDBClusterEndpoint"},
				"Version":                     {"2014-10-31"},
				"DBClusterEndpointIdentifier": {epID},
				"DBClusterIdentifier":         {"ep-type-cluster"},
			}
			if tt.endpointType != "" {
				vals.Set("EndpointType", tt.endpointType)
			}

			rr := doRequest(t, h, vals)
			assert.Equal(t, tt.wantStatus, rr.Code)
		})
	}
}

// TestRefinement1_EngineVersion_InCreateResponse verifies EngineVersion is returned.
func TestRefinement1_EngineVersion_InCreateResponse(t *testing.T) {
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

// TestRefinement1_PortParsing verifies custom port is honored.
func TestRefinement1_PortParsing(t *testing.T) {
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

// TestRefinement1_TagsOnCreate verifies tags passed during CreateDBCluster are stored.
func TestRefinement1_TagsOnCreate(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)

	doRequest(t, h, url.Values{
		"Action":              {"CreateDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"tagged-cluster"},
		"Tags.Tag.1.Key":      {"Env"},
		"Tags.Tag.1.Value":    {"production"},
	})

	assert.Equal(t, 1, neptune.TagCount(backend))
}

// TestRefinement1_CloneCluster_NoSharedSlice verifies deep copy of DBClusterMembers.
func TestRefinement1_CloneCluster_NoSharedSlice(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)

	createCluster(t, h, "member-cluster")
	createInstance(t, h, "member-inst", "member-cluster")

	clusters, err := backend.DescribeDBClusters(context.Background(), "member-cluster")
	require.NoError(t, err)
	require.Len(t, clusters, 1)
	require.Len(t, clusters[0].DBClusterMembers, 1)

	// Mutate the returned copy — should not affect stored state.
	clusters[0].DBClusterMembers[0].DBInstanceIdentifier = "mutated"

	clusters2, err := backend.DescribeDBClusters(context.Background(), "member-cluster")
	require.NoError(t, err)
	assert.NotEqual(t, "mutated", clusters2[0].DBClusterMembers[0].DBInstanceIdentifier)
}

// TestRefinement1_SeedHelpers verifies all AddXInternal seed helpers work correctly.
func TestRefinement1_SeedHelpers(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	backend.AddClusterInternal("s-cluster")
	backend.AddSnapshotInternal("s-snap", "s-cluster")
	backend.AddClusterParameterGroupInternal("s-cpg", "neptune1.3")
	backend.AddParameterGroupInternal("s-pg", "neptune1.3")
	backend.AddEventSubscriptionInternal("s-sub", "arn:aws:sns:us-east-1:000000000000:test")

	assert.Equal(t, 1, neptune.ClusterCount(backend))
	assert.Equal(t, 1, neptune.ClusterSnapshotCount(backend))
	assert.Equal(t, 1, neptune.ClusterParameterGroupCount(backend))
	assert.Equal(t, 1, neptune.ParameterGroupCount(backend))
	assert.Equal(t, 1, neptune.EventSubscriptionCount(backend))
}

// TestRefinement1_CopyDBClusterParameterGroup_MissingSource verifies error on missing source.
func TestRefinement1_CopyDBClusterParameterGroup_MissingSource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"CopyDBClusterParameterGroup"},
		"Version": {"2014-10-31"},
		"SourceDBClusterParameterGroupIdentifier":  {"nonexistent"},
		"TargetDBClusterParameterGroupIdentifier":  {"target-pg"},
		"TargetDBClusterParameterGroupDescription": {"test"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterParameterGroupNotFoundFault")
}

// TestRefinement1_CopyDBClusterSnapshot_MissingSource verifies error on missing source snapshot.
func TestRefinement1_CopyDBClusterSnapshot_MissingSource(t *testing.T) {
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

// TestRefinement1_CopyDBParameterGroup_MissingSource verifies error on missing source param group.
func TestRefinement1_CopyDBParameterGroup_MissingSource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                            {"CopyDBParameterGroup"},
		"Version":                           {"2014-10-31"},
		"SourceDBParameterGroupIdentifier":  {"nonexistent"},
		"TargetDBParameterGroupIdentifier":  {"target-dpg"},
		"TargetDBParameterGroupDescription": {"test"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupNotFoundFault")
}

// TestRefinement1_CreateEventSubscription_MissingSNS verifies error on missing SnsTopicArn.
func TestRefinement1_CreateEventSubscription_MissingSNS(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-no-sns"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// TestRefinement1_CreateDBClusterEndpoint_MissingCluster verifies error on missing cluster.
func TestRefinement1_CreateDBClusterEndpoint_MissingCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-missing"},
		"DBClusterIdentifier":         {"nonexistent"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterNotFoundFault")
}

// TestRefinement1_AddRoleToDBCluster_MissingRole verifies error on empty RoleArn.
func TestRefinement1_AddRoleToDBCluster_MissingRole(t *testing.T) {
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

// TestRefinement1_AddSourceIdentifier_SubscriptionNotFound verifies proper error.
func TestRefinement1_AddSourceIdentifier_SubscriptionNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"nonexistent-sub"},
		"SourceIdentifier": {"some-id"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionNotFoundFault")
}

// TestRefinement1_GlobalClusterAlreadyExists verifies proper error on duplicate.
func TestRefinement1_GlobalClusterAlreadyExists(t *testing.T) {
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

// TestRefinement1_ApplyPendingMaintenanceAction_MissingFields verifies validation.
func TestRefinement1_ApplyPendingMaintenanceAction_MissingFields(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		vals        url.Values
		wantContain string
	}{
		{
			name: "missing_resource_id",
			vals: url.Values{
				"Action":      {"ApplyPendingMaintenanceAction"},
				"Version":     {"2014-10-31"},
				"ApplyAction": {"system-update"},
				"OptInType":   {"immediate"},
			},
			wantContain: "InvalidParameterValue",
		},
		{
			name: "missing_apply_action",
			vals: url.Values{
				"Action":             {"ApplyPendingMaintenanceAction"},
				"Version":            {"2014-10-31"},
				"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:test"},
				"OptInType":          {"immediate"},
			},
			wantContain: "InvalidParameterValue",
		},
		{
			name: "missing_opt_in_type",
			vals: url.Values{
				"Action":             {"ApplyPendingMaintenanceAction"},
				"Version":            {"2014-10-31"},
				"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:test"},
				"ApplyAction":        {"system-update"},
			},
			wantContain: "InvalidParameterValue",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			rr := doRequest(t, h, tt.vals)
			assert.Equal(t, http.StatusBadRequest, rr.Code)
			assert.Contains(t, rr.Body.String(), tt.wantContain)
		})
	}
}

// TestRefinement1_CopyDBClusterParameterGroup_TargetAlreadyExists verifies duplicate error.
func TestRefinement1_CopyDBClusterParameterGroup_TargetAlreadyExists(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	hb := neptune.NewHandler(backend)

	backend.AddClusterParameterGroupInternal("src-cpg", "neptune1.3")
	backend.AddClusterParameterGroupInternal("existing-cpg", "neptune1.3")

	rr := doRequest(t, hb, url.Values{
		"Action":  {"CopyDBClusterParameterGroup"},
		"Version": {"2014-10-31"},
		"SourceDBClusterParameterGroupIdentifier":  {"src-cpg"},
		"TargetDBClusterParameterGroupIdentifier":  {"existing-cpg"},
		"TargetDBClusterParameterGroupDescription": {"test"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterParameterGroupAlreadyExistsFault")
}

// TestRefinement1_ClusterEndpointAlreadyExists verifies duplicate endpoint error.
func TestRefinement1_ClusterEndpointAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-dup-cluster")

	doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"dup-ep"},
		"DBClusterIdentifier":         {"ep-dup-cluster"},
		"EndpointType":                {"READER"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"dup-ep"},
		"DBClusterIdentifier":         {"ep-dup-cluster"},
		"EndpointType":                {"READER"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterEndpointAlreadyExistsFault")
}

// TestRefinement1_SubscriptionAlreadyExists verifies duplicate subscription error.
func TestRefinement1_SubscriptionAlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"dup-sub"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:test"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"dup-sub"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:test"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "SubscriptionAlreadyExistFault")
}

// TestRefinement1_AddRoleToDBCluster_Idempotent verifies adding same role twice is idempotent.
func TestRefinement1_AddRoleToDBCluster_Idempotent(t *testing.T) {
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

// TestRefinement1_CreateGlobalCluster_WithSourceCluster verifies member is populated.
func TestRefinement1_CreateGlobalCluster_WithSourceCluster(t *testing.T) {
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

// TestRefinement1_CreateDBParameterGroup_AlreadyExists verifies duplicate error.
func TestRefinement1_CreateDBParameterGroup_AlreadyExists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"dup-dpg"},
		"DBParameterGroupFamily": {"neptune1.3"},
		"Description":            {"test"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"dup-dpg"},
		"DBParameterGroupFamily": {"neptune1.3"},
		"Description":            {"test"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBParameterGroupAlreadyExistsFault")
}

// TestRefinement1_Persistence_EmptyRestore verifies Restore with empty maps is safe.
func TestRefinement1_Persistence_EmptyRestore(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	data := backend.Snapshot()
	require.NotEmpty(t, data)

	fresh := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	require.NoError(t, fresh.Restore(data))
	assert.Equal(t, 0, neptune.ClusterCount(fresh))
}

// TestRefinement1_Persistence_InvalidJSON verifies Restore returns error on bad data.
func TestRefinement1_Persistence_InvalidJSON(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	err := backend.Restore([]byte(`not-json`))
	require.Error(t, err)
}

// TestRefinement1_CreateDBCluster_MissingID verifies validation error on empty ID.
func TestRefinement1_CreateDBCluster_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"CreateDBCluster"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}

// TestRefinement1_CreateGlobalCluster_MissingID verifies validation error on empty global cluster ID.
func TestRefinement1_CreateGlobalCluster_MissingID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"CreateGlobalCluster"},
		"Version": {"2014-10-31"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "InvalidParameterValue")
}
