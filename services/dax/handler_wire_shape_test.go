package dax_test

import (
	"encoding/json"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestHandlerClusterStatusWireKey verifies the cluster status is emitted
// under the real DAX wire key "Status", not the invented "ClusterStatus".
// The real aws-sdk-go-v2 deserializer (awsAwsjson11_deserializeDocumentCluster)
// only recognizes "Status"; any other key is silently discarded by its
// default case, leaving the client's Cluster.Status nil.
func TestHandlerClusterStatusWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := daxRequest(t, h, "CreateCluster", validClusterBody("status-key-cluster"))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cluster := resp["Cluster"].(map[string]any)
	assert.Equal(t, "available", cluster["Status"], "wire key must be Status")
	_, hasLegacyKey := cluster["ClusterStatus"]
	assert.False(t, hasLegacyKey, "ClusterStatus is not a real DAX wire key")
}

// TestHandlerTimestampsAreEpochSeconds verifies that DAX timestamp fields
// (Node.NodeCreateTime, Event.Date) are emitted as JSON numbers (epoch
// seconds), matching the awsjson1.1 unixTimestamp wire format the real SDK
// deserializer requires (smithytime.ParseEpochSeconds). RFC3339 strings are
// rejected by the client with "expected TStamp to be a JSON Number".
func TestHandlerTimestampsAreEpochSeconds(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	before := time.Now().UTC().Add(-time.Minute).Unix()

	rec := daxRequest(t, h, "CreateCluster", validClusterBody("epoch-cluster"))
	require.Equal(t, http.StatusOK, rec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

	cluster := createResp["Cluster"].(map[string]any)
	nodes := cluster["Nodes"].([]any)
	require.NotEmpty(t, nodes)

	node := nodes[0].(map[string]any)
	createTime, ok := node["NodeCreateTime"].(float64)
	require.True(t, ok, "NodeCreateTime must decode as a JSON number, got %T", node["NodeCreateTime"])
	assert.Greater(t, createTime, float64(before))

	eventsRec := daxRequest(t, h, "DescribeEvents", map[string]any{})
	require.Equal(t, http.StatusOK, eventsRec.Code)

	var eventsResp map[string]any
	require.NoError(t, json.Unmarshal(eventsRec.Body.Bytes(), &eventsResp))

	events := eventsResp["Events"].([]any)
	require.NotEmpty(t, events)

	ev := events[0].(map[string]any)
	date, ok := ev["Date"].(float64)
	require.True(t, ok, "Date must decode as a JSON number, got %T", ev["Date"])
	assert.Greater(t, date, float64(before))
}

// TestHandlerDescribeEventsAcceptsEpochSecondsRequest verifies StartTime and
// EndTime on the DescribeEvents request decode as epoch-seconds JSON numbers
// (the real awsAwsjson11_serializeOpDocumentDescribeEventsInput encodes them
// via smithytime.FormatEpochSeconds), not RFC3339 strings, and that the
// filter actually excludes events outside the window.
func TestHandlerDescribeEventsAcceptsEpochSecondsRequest(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	daxRequest(t, h, "CreateCluster", validClusterBody("evt-epoch-cluster"))

	future := float64(time.Now().UTC().Add(time.Hour).Unix())

	rec := daxRequest(t, h, "DescribeEvents", map[string]any{
		"StartTime": future,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	assert.Empty(t, resp["Events"], "a StartTime in the future should exclude all existing events")

	past := float64(time.Now().UTC().Add(-time.Hour).Unix())

	rec2 := daxRequest(t, h, "DescribeEvents", map[string]any{
		"StartTime": past,
	})
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	assert.NotEmpty(t, resp2["Events"], "a StartTime in the past should include existing events")
}

// TestHandlerDescribeEventsRejectsNonNumericTime verifies that a malformed
// (non-numeric) StartTime is rejected as a client error rather than crashing
// or silently ignored.
func TestHandlerDescribeEventsRejectsNonNumericTime(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := daxRequest(t, h, "DescribeEvents", map[string]any{
		"StartTime": "not-a-number",
	})

	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

// TestHandlerParameterGroupNodeIdsToRebootWireKey verifies the pending-reboot
// node list is surfaced on the wire under "NodeIdsToReboot" (matching
// awsAwsjson11_deserializeDocumentParameterGroupStatus) and that
// toClusterResponse actually copies it from backend state instead of
// silently dropping it.
func TestHandlerParameterGroupNodeIdsToRebootWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler()

	daxRequest(t, h, "CreateParameterGroup", map[string]any{
		"ParameterGroupName": "custom-pg",
		"Description":        "test",
	})

	body := validClusterBody("pg-reboot-cluster")
	body["ParameterGroupName"] = "custom-pg"
	body["ReplicationFactor"] = 2
	daxRequest(t, h, "CreateCluster", body)

	daxRequest(t, h, "UpdateParameterGroup", map[string]any{
		"ParameterGroupName": "custom-pg",
		"ParameterNameValues": []map[string]any{
			{"ParameterName": "query-ttl-millis", "ParameterValue": "600000"},
		},
	})

	rec := daxRequest(t, h, "DescribeClusters", map[string]any{
		"ClusterNames": []string{"pg-reboot-cluster"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	clusters := resp["Clusters"].([]any)
	require.Len(t, clusters, 1)

	cluster := clusters[0].(map[string]any)
	pg := cluster["ParameterGroup"].(map[string]any)

	assert.Equal(t, "pending-reboot", pg["ParameterApplyStatus"])

	nodeIDs, ok := pg["NodeIdsToReboot"].([]any)
	require.True(t, ok, "NodeIdsToReboot must be present under the real wire key")
	assert.Len(t, nodeIDs, 2)

	_, hasLegacyKey := pg["NodeIDsToReboot"]
	assert.False(t, hasLegacyKey, "NodeIDsToReboot is not a real DAX wire key")
}

// TestHandlerClusterNetworkTypeWireKey verifies NetworkType is emitted under
// its real wire key "NetworkType" (types.Cluster.NetworkType /
// awsAwsjson11_deserializeDocumentCluster case "NetworkType") and defaults to
// "ipv4" when the request omits it.
func TestHandlerClusterNetworkTypeWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := daxRequest(t, h, "CreateCluster", validClusterBody("network-type-cluster"))
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cluster := resp["Cluster"].(map[string]any)
	assert.Equal(t, "ipv4", cluster["NetworkType"])
}

// TestHandlerDecreaseReplicationFactorNodeIdsToRemoveWireKey verifies the
// transient node-removal list surfaces under the real wire key
// "NodeIdsToRemove" (types.Cluster.NodeIdsToRemove) while a
// DecreaseReplicationFactor is in flight.
func TestHandlerDecreaseReplicationFactorNodeIdsToRemoveWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	body := validClusterBody("decrease-wire")
	body["ReplicationFactor"] = 3
	daxRequest(t, h, "CreateCluster", body)

	rec := daxRequest(t, h, "DecreaseReplicationFactor", map[string]any{
		"ClusterName":          "decrease-wire",
		"NewReplicationFactor": 1,
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	cluster := resp["Cluster"].(map[string]any)
	nodeIDs, ok := cluster["NodeIdsToRemove"].([]any)
	require.True(t, ok, "NodeIdsToRemove must be present under the real wire key while modifying")
	assert.Len(t, nodeIDs, 2)
}

// TestHandlerSubnetGroupSupportedNetworkTypesWireKey verifies the
// SupportedNetworkTypes field surfaces under its real wire key
// (types.SubnetGroup.SupportedNetworkTypes).
func TestHandlerSubnetGroupSupportedNetworkTypesWireKey(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	rec := daxRequest(t, h, "CreateSubnetGroup", map[string]any{
		"SubnetGroupName": "wire-sg",
		"SubnetIds":       []string{"subnet-11111111"},
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	sg := resp["SubnetGroup"].(map[string]any)
	types, ok := sg["SupportedNetworkTypes"].([]any)
	require.True(t, ok, "SupportedNetworkTypes must be present under the real wire key")
	assert.Equal(t, []any{"ipv4"}, types)
}

// TestHandlerDescribeParametersSourceFilter verifies the request's "Source"
// field (types.DescribeParametersInput.Source) is honored end-to-end through
// the HTTP handler, not just the backend method.
func TestHandlerDescribeParametersSourceFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler()
	daxRequest(t, h, "CreateParameterGroup", map[string]any{"ParameterGroupName": "src-filter-pg"})
	daxRequest(t, h, "UpdateParameterGroup", map[string]any{
		"ParameterGroupName": "src-filter-pg",
		"ParameterNameValues": []map[string]any{
			{"ParameterName": "query-ttl-millis", "ParameterValue": "60000"},
		},
	})

	rec := daxRequest(t, h, "DescribeParameters", map[string]any{
		"ParameterGroupName": "src-filter-pg",
		"Source":             "user",
	})
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	params := resp["Parameters"].([]any)
	require.Len(t, params, 1)
	assert.Equal(t, "query-ttl-millis", params[0].(map[string]any)["ParameterName"])
}
