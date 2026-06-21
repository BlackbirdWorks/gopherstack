package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
)

// --- Cluster Endpoint operations ---

// TestRefinement2_CreateDescribeDeleteDBClusterEndpoint tests CRUD for cluster endpoints.
func TestRefinement2_CreateDescribeDeleteDBClusterEndpoint(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "ep-cluster")

	// Create endpoint
	rr := doRequest(t, h, url.Values{
		"Action":                      {"CreateDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-01"},
		"DBClusterIdentifier":         {"ep-cluster"},
		"EndpointType":                {"READER"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "ep-01")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeDBClusterEndpoints"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "ep-01")

	// Modify endpoint
	rr = doRequest(t, h, url.Values{
		"Action":                      {"ModifyDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-01"},
		"EndpointType":                {"ANY"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Delete endpoint
	rr = doRequest(t, h, url.Values{
		"Action":                      {"DeleteDBClusterEndpoint"},
		"Version":                     {"2014-10-31"},
		"DBClusterEndpointIdentifier": {"ep-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// --- DB Parameter Group operations ---

// TestRefinement2_CreateDescribeDeleteDBParameterGroup tests CRUD for parameter groups.
func TestRefinement2_CreateDescribeDeleteDBParameterGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rr := doRequest(t, h, url.Values{
		"Action":                 {"CreateDBParameterGroup"},
		"Version":                {"2014-10-31"},
		"DBParameterGroupName":   {"pg-01"},
		"DBParameterGroupFamily": {"neptune1.3"},
		"Description":            {"test"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "pg-01")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeDBParameterGroups"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "pg-01")

	// Modify
	rr = doRequest(t, h, url.Values{
		"Action":               {"ModifyDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Reset
	rr = doRequest(t, h, url.Values{
		"Action":               {"ResetDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Delete
	rr = doRequest(t, h, url.Values{
		"Action":               {"DeleteDBParameterGroup"},
		"Version":              {"2014-10-31"},
		"DBParameterGroupName": {"pg-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_ResetDBClusterParameterGroup tests reset cluster parameter group.
func TestRefinement2_ResetDBClusterParameterGroup(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterParameterGroupInternal("cpg-reset", "neptune1.3")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"ResetDBClusterParameterGroup"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"cpg-reset"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "cpg-reset")
}

// --- Event Subscriptions ---

// TestRefinement2_CreateDescribeDeleteEventSubscription tests full event subscription lifecycle.
func TestRefinement2_CreateDescribeDeleteEventSubscription(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create
	rr := doRequest(t, h, url.Values{
		"Action":           {"CreateEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"SnsTopicArn":      {"arn:aws:sns:us-east-1:000000000000:neptune-events"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "sub-01")

	// Describe
	rr = doRequest(t, h, url.Values{
		"Action":  {"DescribeEventSubscriptions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "sub-01")

	// Add source identifier
	rr = doRequest(t, h, url.Values{
		"Action":           {"AddSourceIdentifierToSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"SourceIdentifier": {"my-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Remove source identifier
	rr = doRequest(t, h, url.Values{
		"Action":           {"RemoveSourceIdentifierFromSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"SourceIdentifier": {"my-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Modify subscription
	rr = doRequest(t, h, url.Values{
		"Action":           {"ModifyEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
		"Enabled":          {"true"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Delete
	rr = doRequest(t, h, url.Values{
		"Action":           {"DeleteEventSubscription"},
		"Version":          {"2014-10-31"},
		"SubscriptionName": {"sub-01"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// --- Global Cluster operations ---

// TestRefinement2_CreateDescribeDeleteGlobalCluster tests full global cluster lifecycle.
func TestRefinement2_CreateDescribeDeleteGlobalCluster(t *testing.T) {
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

// --- IAM Role operations ---

// TestRefinement2_RemoveRoleFromDBCluster tests removing role from cluster.
func TestRefinement2_RemoveRoleFromDBCluster(t *testing.T) {
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

// --- Misc operations ---

// TestRefinement2_DescribeEvents tests DescribeEvents.
func TestRefinement2_DescribeEvents(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribeEvents"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_DescribePendingMaintenanceActions tests pending maintenance.
func TestRefinement2_DescribePendingMaintenanceActions(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":  {"DescribePendingMaintenanceActions"},
		"Version": {"2014-10-31"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_PromoteReadReplicaDBCluster tests promote replica.
func TestRefinement2_PromoteReadReplicaDBCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "replica-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"PromoteReadReplicaDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"replica-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_ModifyDBSubnetGroup tests ModifyDBSubnetGroup.
func TestRefinement2_ModifyDBSubnetGroup(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"sg-modify"},
		"DBSubnetGroupDescription": {"test"},
		"SubnetIds.member.1":       {"subnet-1"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":                   {"ModifyDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"sg-modify"},
		"DBSubnetGroupDescription": {"updated desc"},
		"SubnetIds.member.1":       {"subnet-1"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_Persistence_Handler tests handler Snapshot and Restore.
func TestRefinement2_Persistence_Handler(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterInternal("persist-cluster")

	// Snapshot via handler
	data := h.Snapshot(t.Context())
	require.NotEmpty(t, data)

	// Restore into fresh handler
	backend2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h2 := neptune.NewHandler(backend2)
	require.NoError(t, h2.Restore(t.Context(), data))
	assert.Equal(t, 1, neptune.ClusterCount(backend2))
}

// TestRefinement2_DescribeDBInstances_ByID tests single-instance lookup.
func TestRefinement2_DescribeDBInstances_ByID(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "inst-byid-cluster")
	doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-byid"},
		"DBClusterIdentifier":  {"inst-byid-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"inst-byid"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "inst-byid")
}

// TestRefinement2_DescribeDBInstances_NotFound tests not found error.
func TestRefinement2_DescribeDBInstances_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":               {"DescribeDBInstances"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"nonexistent-inst"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBInstanceNotFound")
}

// TestRefinement2_DescribeDBSubnetGroups_ByName tests single subnet group lookup.
func TestRefinement2_DescribeDBSubnetGroups_ByName(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	doRequest(t, h, url.Values{
		"Action":                   {"CreateDBSubnetGroup"},
		"Version":                  {"2014-10-31"},
		"DBSubnetGroupName":        {"sg-byname"},
		"DBSubnetGroupDescription": {"test"},
		"SubnetIds.member.1":       {"subnet-1"},
	})

	rr := doRequest(t, h, url.Values{
		"Action":            {"DescribeDBSubnetGroups"},
		"Version":           {"2014-10-31"},
		"DBSubnetGroupName": {"sg-byname"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "sg-byname")
}

// TestRefinement2_DescribeDBSubnetGroups_NotFound tests not found error.
func TestRefinement2_DescribeDBSubnetGroups_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":            {"DescribeDBSubnetGroups"},
		"Version":           {"2014-10-31"},
		"DBSubnetGroupName": {"nonexistent-sg"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBSubnetGroupNotFoundFault")
}

// TestRefinement2_StopStartDBCluster tests stop/start lifecycle.
func TestRefinement2_StopStartDBCluster(t *testing.T) {
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

// TestRefinement2_FailoverDBCluster tests FailoverDBCluster.
func TestRefinement2_FailoverDBCluster(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "failover-cluster")

	rr := doRequest(t, h, url.Values{
		"Action":              {"FailoverDBCluster"},
		"Version":             {"2014-10-31"},
		"DBClusterIdentifier": {"failover-cluster"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_ModifyRebootDBInstance tests instance modify and reboot.
func TestRefinement2_ModifyRebootDBInstance(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "reboot-cluster")
	doRequest(t, h, url.Values{
		"Action":               {"CreateDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"reboot-inst"},
		"DBClusterIdentifier":  {"reboot-cluster"},
		"DBInstanceClass":      {"db.r5.large"},
	})

	// Modify
	rr := doRequest(t, h, url.Values{
		"Action":               {"ModifyDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"reboot-inst"},
		"DBInstanceClass":      {"db.r5.xlarge"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// Reboot
	rr = doRequest(t, h, url.Values{
		"Action":               {"RebootDBInstance"},
		"Version":              {"2014-10-31"},
		"DBInstanceIdentifier": {"reboot-inst"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_Tags_AddListRemove tests tag operations.
func TestRefinement2_Tags_AddListRemove(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createCluster(t, h, "tag-cluster")

	arn := "arn:aws:neptune:us-east-1:000000000000:cluster:tag-cluster"

	// Add tags
	rr := doRequest(t, h, url.Values{
		"Action":           {"AddTagsToResource"},
		"Version":          {"2014-10-31"},
		"ResourceName":     {arn},
		"Tags.Tag.1.Key":   {"env"},
		"Tags.Tag.1.Value": {"test"},
	})
	require.Equal(t, http.StatusOK, rr.Code)

	// List tags
	rr = doRequest(t, h, url.Values{
		"Action":       {"ListTagsForResource"},
		"Version":      {"2014-10-31"},
		"ResourceName": {arn},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "env")

	// Remove tags
	rr = doRequest(t, h, url.Values{
		"Action":           {"RemoveTagsFromResource"},
		"Version":          {"2014-10-31"},
		"ResourceName":     {arn},
		"TagKeys.member.1": {"env"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_ApplyPendingMaintenanceAction tests apply pending maintenance.
func TestRefinement2_ApplyPendingMaintenanceAction(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":             {"ApplyPendingMaintenanceAction"},
		"Version":            {"2014-10-31"},
		"ResourceIdentifier": {"arn:aws:rds:us-east-1:000000000000:cluster:test"},
		"ApplyAction":        {"system-update"},
		"OptInType":          {"immediate"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
}

// TestRefinement2_DescribeDBClusterParameterGroups_ByName tests describe by name.
func TestRefinement2_DescribeDBClusterParameterGroups_ByName(t *testing.T) {
	t.Parallel()

	backend := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	h := neptune.NewHandler(backend)
	backend.AddClusterParameterGroupInternal("my-cpg", "neptune1.3")

	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"my-cpg"},
	})
	require.Equal(t, http.StatusOK, rr.Code)
	assert.Contains(t, rr.Body.String(), "my-cpg")
}

// TestRefinement2_DescribeDBClusterParameterGroups_NotFound tests not found.
func TestRefinement2_DescribeDBClusterParameterGroups_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	rr := doRequest(t, h, url.Values{
		"Action":                      {"DescribeDBClusterParameterGroups"},
		"Version":                     {"2014-10-31"},
		"DBClusterParameterGroupName": {"nonexistent-cpg"},
	})
	assert.Equal(t, http.StatusBadRequest, rr.Code)
	assert.Contains(t, rr.Body.String(), "DBClusterParameterGroupNotFoundFault")
}

// TestRefinement2_ModifyDBCluster tests cluster modification.
func TestRefinement2_ModifyDBCluster(t *testing.T) {
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

// TestRefinement2_DescribeDBClusters_ByID tests single cluster lookup.
func TestRefinement2_DescribeDBClusters_ByID(t *testing.T) {
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
