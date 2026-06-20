package kafka_test

// handler_accuracy_batch2_test.go — MSK AWS-accuracy audit batch-2 (go-s504).
//
// Covers ops not exercised via HTTP in batch-1 / refinement tests:
//   - UpdateBrokerCount, UpdateBrokerStorage, UpdateBrokerType, UpdateClusterKafkaVersion:
//     round-trip via HTTP and verify state persists in DescribeClusterV2.
//   - UpdateConnectivity, UpdateMonitoring, UpdateRebalancing, UpdateSecurity,
//     UpdateStorage: stub ops return 200 + clusterOperationArn; NotFound on missing cluster.
//   - RebootBroker: returns 200 + clusterOperationArn; NotFound on missing cluster.
//   - ClusterOperation tracking: update ops create retrievable operations via
//     ListClusterOperations (v1/v2) and DescribeClusterOperation (v1/v2).
//   - SCRAM lifecycle: BatchAssociate → ListScramSecrets → BatchDisassociate via HTTP.
//   - VPC connection lifecycle: Create → Describe → List → ListClientVpcConnections
//     → Reject → Delete; NotFound paths.
//   - Configuration revision lifecycle: DescribeConfigurationRevision,
//     ListConfigurationRevisions, UpdateConfiguration via HTTP.
//   - Replicator UpdateReplicationInfo via HTTP.
//   - Tag CRUD: TagResource, ListTagsForResource, UntagResource via HTTP.

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

// ----------------------------------------
// helpers
// ----------------------------------------

func b2NewHandler(t *testing.T) *kafka.Handler {
	t.Helper()

	return kafka.NewHandler(kafka.NewInMemoryBackend(testAccountID, testRegion))
}

func b2CreateCluster(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
		"clusterName":         name,
		"kafkaVersion":        "2.8.0",
		"numberOfBrokerNodes": 3,
		"brokerNodeGroupInfo": map[string]any{
			"instanceType":  "kafka.m5.large",
			"clientSubnets": []string{"subnet-1"},
			"storageInfo": map[string]any{
				"ebsStorageInfo": map[string]any{"volumeSize": 100},
			},
		},
	})
	require.Equal(t, http.StatusOK, rec.Code, "create cluster: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["clusterArn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func b2CreateConfig(t *testing.T, h *kafka.Handler, name string) string {
	t.Helper()

	rec := doKafkaRequest(t, h, http.MethodPost, "/v1/configurations", map[string]any{
		"name":             name,
		"kafkaVersions":    []string{"2.8.0"},
		"serverProperties": "auto.create.topics.enable=false",
	})
	require.Equal(t, http.StatusOK, rec.Code, "create config: %s", rec.Body.String())

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	arn, _ := resp["arn"].(string)
	require.NotEmpty(t, arn)

	return arn
}

func b2EncodedPath(arn string) string {
	return url.PathEscape(arn)
}

func b2ParseOp(t *testing.T, h *kafka.Handler, method, path string, body any) (map[string]any, int) {
	t.Helper()

	rec := doKafkaRequest(t, h, method, path, body)

	var resp map[string]any
	if rec.Body.Len() > 0 {
		_ = json.Unmarshal(rec.Body.Bytes(), &resp)
	}

	return resp, rec.Code
}

// ----------------------------------------
// UpdateBrokerCount: persists new broker count
// ----------------------------------------

func TestBatch2_UpdateBrokerCount_Persists(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		initialBrokers int
		targetBrokers  int32
	}{
		{name: "scale_up", initialBrokers: 3, targetBrokers: 6},
		{name: "scale_down", initialBrokers: 6, targetBrokers: 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2NewHandler(t)
			clusterArn := b2CreateCluster(t, h, "update-broker-count-"+tt.name)
			encoded := b2EncodedPath(clusterArn)

			resp, code := b2ParseOp(t, h, http.MethodPut,
				"/api/v2/clusters/"+encoded+"/broker-count",
				map[string]any{
					"currentVersion":            kafka.DefaultClusterVersion,
					"targetNumberOfBrokerNodes": tt.targetBrokers,
				})
			require.Equal(t, http.StatusOK, code)
			assert.NotEmpty(t, resp["clusterOperationArn"])

			// Verify the broker count updated in the cluster.
			descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded, nil)
			require.Equal(t, http.StatusOK, descRec.Code)

			var descResp map[string]any
			require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
			clusterInfo, ok := descResp["clusterInfo"].(map[string]any)
			require.True(t, ok)
			provisioned, ok := clusterInfo["provisioned"].(map[string]any)
			require.True(t, ok)
			assert.InDelta(t, float64(tt.targetBrokers), provisioned["numberOfBrokerNodes"], 0,
				"numberOfBrokerNodes should reflect the update")
		})
	}
}

func TestBatch2_UpdateBrokerCount_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1/broker-count",
		map[string]any{
			"currentVersion":            kafka.DefaultClusterVersion,
			"targetNumberOfBrokerNodes": int32(6),
		})
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// UpdateBrokerStorage: persists new volume size
// ----------------------------------------

func TestBatch2_UpdateBrokerStorage_Persists(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "update-broker-storage")
	encoded := b2EncodedPath(clusterArn)

	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/broker-storage",
		map[string]any{
			"currentVersion": kafka.DefaultClusterVersion,
			"targetBrokerEBSVolumeInfo": []map[string]any{
				{"kafkaBrokerNodeId": "0", "volumeSizeGB": int32(200)},
			},
		})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])

	// Verify storage updated.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	clusterInfo, _ := descResp["clusterInfo"].(map[string]any)
	provisioned, _ := clusterInfo["provisioned"].(map[string]any)
	bng, _ := provisioned["brokerNodeGroupInfo"].(map[string]any)
	storageInfo, _ := bng["storageInfo"].(map[string]any)
	ebsInfo, _ := storageInfo["ebsStorageInfo"].(map[string]any)
	assert.InDelta(t, float64(200), ebsInfo["volumeSize"], 0, "storage should reflect the update")
}

func TestBatch2_UpdateBrokerStorage_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1/broker-storage",
		map[string]any{"currentVersion": kafka.DefaultClusterVersion})
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// UpdateBrokerType: persists new instance type
// ----------------------------------------

func TestBatch2_UpdateBrokerType_Persists(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "update-broker-type")
	encoded := b2EncodedPath(clusterArn)

	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/broker-type",
		map[string]any{
			"currentVersion":     kafka.DefaultClusterVersion,
			"targetInstanceType": "kafka.m5.xlarge",
		})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])

	// Verify instance type updated.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	clusterInfo, _ := descResp["clusterInfo"].(map[string]any)
	provisioned, _ := clusterInfo["provisioned"].(map[string]any)
	bng, _ := provisioned["brokerNodeGroupInfo"].(map[string]any)
	assert.Equal(t, "kafka.m5.xlarge", bng["instanceType"])
}

func TestBatch2_UpdateBrokerType_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1/broker-type",
		map[string]any{
			"currentVersion":     kafka.DefaultClusterVersion,
			"targetInstanceType": "kafka.m5.xlarge",
		})
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// UpdateClusterKafkaVersion: persists new version
// ----------------------------------------

func TestBatch2_UpdateClusterKafkaVersion_Persists(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "update-kafka-version")
	encoded := b2EncodedPath(clusterArn)

	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/kafka-version",
		map[string]any{
			"currentVersion":     kafka.DefaultClusterVersion,
			"targetKafkaVersion": "3.5.1",
		})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])

	// Verify version updated.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	clusterInfo, _ := descResp["clusterInfo"].(map[string]any)
	provisioned, _ := clusterInfo["provisioned"].(map[string]any)
	assert.Equal(t, "3.5.1", provisioned["currentBrokerSoftwareInfo"].(map[string]any)["kafkaVersion"])
}

func TestBatch2_UpdateClusterKafkaVersion_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1/kafka-version",
		map[string]any{
			"currentVersion":     kafka.DefaultClusterVersion,
			"targetKafkaVersion": "3.5.1",
		})
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Stub update ops (Connectivity, Monitoring, Rebalancing, Security, Storage)
// These don't persist state but must return 200 + clusterOperationArn.
// ----------------------------------------

func TestBatch2_StubUpdateOps_HappyPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
	}{
		{name: "connectivity", suffix: "/connectivity"},
		{name: "monitoring", suffix: "/monitoring"},
		{name: "rebalancing", suffix: "/rebalancing"},
		{name: "security", suffix: "/security"},
		{name: "storage", suffix: "/storage"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2NewHandler(t)
			clusterArn := b2CreateCluster(t, h, "stub-update-"+tt.name)
			encoded := b2EncodedPath(clusterArn)

			resp, code := b2ParseOp(t, h, http.MethodPut,
				"/api/v2/clusters/"+encoded+tt.suffix,
				map[string]any{"currentVersion": kafka.DefaultClusterVersion})
			assert.Equal(t, http.StatusOK, code, "suffix=%s", tt.suffix)
			assert.NotEmpty(t, resp["clusterOperationArn"],
				"should return clusterOperationArn for %s", tt.name)
		})
	}
}

func TestBatch2_StubUpdateOps_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
	}{
		{name: "connectivity", suffix: "/connectivity"},
		{name: "monitoring", suffix: "/monitoring"},
		{name: "rebalancing", suffix: "/rebalancing"},
		{name: "security", suffix: "/security"},
		{name: "storage", suffix: "/storage"},
	}

	missingARN := "arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := b2NewHandler(t)
			_, code := b2ParseOp(t, h, http.MethodPut,
				"/api/v2/clusters/"+missingARN+tt.suffix,
				map[string]any{"currentVersion": kafka.DefaultClusterVersion})
			assert.Equal(t, http.StatusNotFound, code, "suffix=%s", tt.suffix)
		})
	}
}

// ----------------------------------------
// RebootBroker
// ----------------------------------------

func TestBatch2_RebootBroker_HappyPath(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "reboot-broker-cluster")
	encoded := b2EncodedPath(clusterArn)

	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/reboot-broker",
		map[string]any{"brokerIds": []string{"0", "1"}})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])
}

func TestBatch2_RebootBroker_EmptyBrokerIds(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "reboot-empty-brokers")
	encoded := b2EncodedPath(clusterArn)

	// Empty brokerIds is accepted — AWS accepts it as a no-op reboot request.
	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/reboot-broker",
		map[string]any{"brokerIds": []string{}})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])
}

func TestBatch2_RebootBroker_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodPut,
		"/v1/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1/reboot-broker",
		map[string]any{"brokerIds": []string{"0"}})
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// ClusterOperation tracking
// Update ops must create operations retrievable via List/Describe (v1 + v2).
// ----------------------------------------

func TestBatch2_ClusterOperationTracking_V1(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "op-tracking-v1")
	encoded := b2EncodedPath(clusterArn)

	// Trigger two update ops.
	resp1, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/broker-count",
		map[string]any{
			"currentVersion":            kafka.DefaultClusterVersion,
			"targetNumberOfBrokerNodes": int32(6),
		})
	require.Equal(t, http.StatusOK, code)
	op1Arn, _ := resp1["clusterOperationArn"].(string)
	require.NotEmpty(t, op1Arn)

	resp2, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/broker-type",
		map[string]any{
			"currentVersion":     kafka.DefaultClusterVersion,
			"targetInstanceType": "kafka.m5.xlarge",
		})
	require.Equal(t, http.StatusOK, code)
	op2Arn, _ := resp2["clusterOperationArn"].(string)
	require.NotEmpty(t, op2Arn)

	// ListClusterOperations (v1).
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded+"/operations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	opList, ok := listResp["clusterOperationInfoList"].([]any)
	require.True(t, ok)
	assert.GreaterOrEqual(t, len(opList), 2, "should have at least 2 operations")

	// Collect operation ARNs from the list.
	opArns := make([]string, 0, len(opList))
	for _, o := range opList {
		om, _ := o.(map[string]any)
		if arn, arnOK := om["clusterOperationArn"].(string); arnOK {
			opArns = append(opArns, arn)
		}
	}
	assert.Contains(t, opArns, op1Arn)
	assert.Contains(t, opArns, op2Arn)

	// DescribeClusterOperation (v1) for both.
	for _, opArn := range []string{op1Arn, op2Arn} {
		descRec := doKafkaRequest(t, h, http.MethodGet, "/v1/operations/"+b2EncodedPath(opArn), nil)
		require.Equal(t, http.StatusOK, descRec.Code)

		var descResp map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
		opInfo, infoOK := descResp["clusterOperationInfo"].(map[string]any)
		require.True(t, infoOK)
		assert.Equal(t, opArn, opInfo["clusterOperationArn"])
		assert.Equal(t, clusterArn, opInfo["clusterArn"])
		assert.NotEmpty(t, opInfo["operationType"])
	}
}

func TestBatch2_ClusterOperationTracking_V2(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "op-tracking-v2")
	encoded := b2EncodedPath(clusterArn)

	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/monitoring",
		map[string]any{"currentVersion": kafka.DefaultClusterVersion})
	require.Equal(t, http.StatusOK, code)
	opArn, _ := resp["clusterOperationArn"].(string)
	require.NotEmpty(t, opArn)

	// ListClusterOperationsV2.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded+"/operations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	opList, ok := listResp["clusterOperationInfoList"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, opList)

	// DescribeClusterOperationV2.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/operations/"+b2EncodedPath(opArn), nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	opInfo, ok := descResp["clusterOperationInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, opArn, opInfo["clusterOperationArn"])
}

func TestBatch2_ClusterOperation_DescribeNotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	missingOpArn := "arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster-operation%2Fnone"
	_, code := b2ParseOp(t, h, http.MethodGet, "/v1/operations/"+missingOpArn, nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// SCRAM secret lifecycle
// ----------------------------------------

func TestBatch2_ScramSecret_Lifecycle(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "scram-lifecycle")
	encoded := b2EncodedPath(clusterArn)
	secretArn := "arn:aws:secretsmanager:us-east-1:000000000000:secret:my-kafka-secret"

	// BatchAssociateScramSecret.
	assocRec := doKafkaRequest(t, h, http.MethodPost,
		"/v1/clusters/"+encoded+"/scram-secrets",
		map[string]any{"secretArnList": []string{secretArn}})
	require.Equal(t, http.StatusOK, assocRec.Code)

	var assocResp map[string]any
	require.NoError(t, json.Unmarshal(assocRec.Body.Bytes(), &assocResp))
	unprocessed, _ := assocResp["unprocessedScramSecrets"].([]any)
	assert.Empty(t, unprocessed, "no secrets should be unprocessed")

	// ListScramSecrets — should include the associated secret.
	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encoded+"/scram-secrets", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	secretList, _ := listResp["secretArnList"].([]any)
	require.Len(t, secretList, 1)
	assert.Equal(t, secretArn, secretList[0])

	// BatchDisassociateScramSecret.
	disassocRec := doKafkaRequest(t, h, http.MethodPatch,
		"/v1/clusters/"+encoded+"/scram-secrets",
		map[string]any{"secretArnList": []string{secretArn}})
	require.Equal(t, http.StatusOK, disassocRec.Code)

	var disassocResp map[string]any
	require.NoError(t, json.Unmarshal(disassocRec.Body.Bytes(), &disassocResp))
	unprocessed2, _ := disassocResp["unprocessedScramSecrets"].([]any)
	assert.Empty(t, unprocessed2, "no secrets should be unprocessed after disassociate")

	// ListScramSecrets — should be empty again.
	listRec2 := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encoded+"/scram-secrets", nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	secretList2, _ := listResp2["secretArnList"].([]any)
	assert.Empty(t, secretList2, "list should be empty after disassociate")
}

func TestBatch2_ScramSecret_AssociateMultiple(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "scram-multi")
	encoded := b2EncodedPath(clusterArn)

	secrets := []string{
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:secret-1",
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:secret-2",
		"arn:aws:secretsmanager:us-east-1:000000000000:secret:secret-3",
	}

	assocRec := doKafkaRequest(t, h, http.MethodPost,
		"/v1/clusters/"+encoded+"/scram-secrets",
		map[string]any{"secretArnList": secrets})
	require.Equal(t, http.StatusOK, assocRec.Code)

	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encoded+"/scram-secrets", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	secretList, _ := listResp["secretArnList"].([]any)
	assert.Len(t, secretList, 3)
}

func TestBatch2_ScramSecret_NotFound(t *testing.T) {
	t.Parallel()

	missingARN := "arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1"
	h := b2NewHandler(t)

	// Associate on missing cluster.
	rec := doKafkaRequest(t, h, http.MethodPost,
		"/v1/clusters/"+missingARN+"/scram-secrets",
		map[string]any{"secretArnList": []string{"arn:aws:secretsmanager:us-east-1:000000000000:secret:s1"}})
	assert.Equal(t, http.StatusNotFound, rec.Code)

	// List on missing cluster.
	rec2 := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+missingARN+"/scram-secrets", nil)
	assert.Equal(t, http.StatusNotFound, rec2.Code)
}

// ----------------------------------------
// VPC connection lifecycle
// ----------------------------------------

func TestBatch2_VpcConnection_Lifecycle(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "vpc-lifecycle-cluster")

	// CreateVpcConnection.
	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
		"targetClusterArn": clusterArn,
		"vpcId":            "vpc-abc123",
		"authentication":   "SASL_IAM",
	})
	require.Equal(t, http.StatusOK, createRec.Code, createRec.Body.String())

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vpcConnArn, _ := createResp["vpcConnectionArn"].(string)
	require.NotEmpty(t, vpcConnArn)
	assert.Equal(t, clusterArn, createResp["targetClusterArn"])
	assert.Equal(t, "vpc-abc123", createResp["vpcId"])
	assert.NotEmpty(t, createResp["state"])

	encodedVpc := b2EncodedPath(vpcConnArn)

	// DescribeVpcConnection.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connection/"+encodedVpc, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.Equal(t, vpcConnArn, descResp["vpcConnectionArn"])
	assert.Equal(t, clusterArn, descResp["targetClusterArn"])

	// ListVpcConnections — our connection is present.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connection", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	conns, _ := listResp["vpcConnections"].([]any)
	require.Len(t, conns, 1)

	// DeleteVpcConnection.
	delRec := doKafkaRequest(t, h, http.MethodDelete, "/v1/vpc-connection/"+encodedVpc, nil)
	assert.Equal(t, http.StatusOK, delRec.Code)

	// DescribeVpcConnection after delete → 404.
	descRec2 := doKafkaRequest(t, h, http.MethodGet, "/v1/vpc-connection/"+encodedVpc, nil)
	assert.Equal(t, http.StatusNotFound, descRec2.Code)
}

func TestBatch2_VpcConnection_ListClientVpcConnections(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "client-vpc-cluster")
	encodedCluster := b2EncodedPath(clusterArn)

	// Create two VPC connections targeting this cluster.
	for i, vpcID := range []string{"vpc-111", "vpc-222"} {
		rec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
			"targetClusterArn": clusterArn,
			"vpcId":            vpcID,
		})
		require.Equal(t, http.StatusOK, rec.Code, "create vpc conn %d", i)
	}

	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/clusters/"+encodedCluster+"/client-vpc-connections", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	conns, _ := listResp["vpcConnections"].([]any)
	assert.Len(t, conns, 2, "should list both VPC connections for this cluster")
}

func TestBatch2_VpcConnection_RejectClientVpcConnection(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "reject-vpc-cluster")

	createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/vpc-connection", map[string]any{
		"targetClusterArn": clusterArn,
		"vpcId":            "vpc-to-reject",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	vpcConnArn, _ := createResp["vpcConnectionArn"].(string)
	encodedCluster := b2EncodedPath(clusterArn)
	encodedVpc := b2EncodedPath(vpcConnArn)

	rejectRec := doKafkaRequest(t, h, http.MethodPut,
		"/v1/clusters/"+encodedCluster+"/reject-client-vpc-connection/"+encodedVpc, nil)
	assert.Equal(t, http.StatusOK, rejectRec.Code)
}

func TestBatch2_VpcConnection_DescribeNotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodGet, "/v1/vpc-connection/arn%3Anone", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Configuration revision lifecycle
// ----------------------------------------

func TestBatch2_ConfigRevision_DescribeAndList(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	configArn := b2CreateConfig(t, h, "cfg-revision-test")
	encoded := b2EncodedPath(configArn)

	// DescribeConfigurationRevision for revision 1.
	descRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/configurations/"+encoded+"/revisions/1", nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	assert.InDelta(t, float64(1), descResp["revision"], 0)

	// ListConfigurationRevisions.
	listRec := doKafkaRequest(t, h, http.MethodGet,
		"/v1/configurations/"+encoded+"/revisions", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	revisions, _ := listResp["revisions"].([]any)
	assert.NotEmpty(t, revisions, "should have at least one revision")
}

func TestBatch2_ConfigRevision_UpdateConfiguration(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	configArn := b2CreateConfig(t, h, "cfg-update-test")
	encoded := b2EncodedPath(configArn)

	// UpdateConfiguration — new description and server properties.
	updateRec := doKafkaRequest(t, h, http.MethodPut, "/v1/configurations/"+encoded, map[string]any{
		"description":      "updated config",
		"serverProperties": "auto.create.topics.enable=true",
	})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, configArn, updateResp["arn"])
	assert.Equal(t, "cfg-update-test", updateResp["name"])
}

func TestBatch2_ConfigRevision_DescribeNotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodGet,
		"/v1/configurations/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Aconfiguration%2Fmissing%2F1/revisions/1",
		nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Replicator UpdateReplicationInfo
// ----------------------------------------

func TestBatch2_Replicator_UpdateReplicationInfo(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)

	// Create a replicator.
	createRec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators", map[string]any{
		"replicatorName":          "my-replicator",
		"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
		"description":             "original description",
	})
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	replicatorArn, _ := createResp["replicatorArn"].(string)
	require.NotEmpty(t, replicatorArn)
	encodedArn := b2EncodedPath(replicatorArn)

	// UpdateReplicationInfo.
	updateRec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+encodedArn,
		map[string]any{"description": "updated description"})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, replicatorArn, updateResp["replicatorArn"])
	assert.NotEmpty(t, updateResp["replicatorState"])
}

func TestBatch2_Replicator_UpdateReplicationInfo_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodPut,
		"/replication/v1/replicators/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Areplicator%2Fmissing",
		map[string]any{"description": "nope"})
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Tag CRUD
// ----------------------------------------

func TestBatch2_Tag_CRUD(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "tag-crud-cluster")
	encoded := b2EncodedPath(clusterArn)

	// TagResource.
	tagRec := doKafkaRequest(t, h, http.MethodPost, "/v1/tags/"+encoded,
		map[string]any{"tags": map[string]string{"env": "test", "team": "infra"}})
	assert.Equal(t, http.StatusOK, tagRec.Code)

	// ListTagsForResource — both tags present.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/tags/"+encoded, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Equal(t, "test", tags["env"])
	assert.Equal(t, "infra", tags["team"])

	// UntagResource — remove "team" tag.
	untagValues := url.Values{"tagKeys": []string{"team"}}
	untagRec := doKafkaRequest(t, h, http.MethodDelete, "/v1/tags/"+encoded+"?"+untagValues.Encode(), nil)
	assert.Equal(t, http.StatusOK, untagRec.Code)

	// ListTagsForResource — only "env" remains.
	listRec2 := doKafkaRequest(t, h, http.MethodGet, "/v1/tags/"+encoded, nil)
	require.Equal(t, http.StatusOK, listRec2.Code)

	var listResp2 map[string]any
	require.NoError(t, json.Unmarshal(listRec2.Body.Bytes(), &listResp2))
	tags2, _ := listResp2["tags"].(map[string]any)
	assert.Equal(t, "test", tags2["env"])
	_, hasTeam := tags2["team"]
	assert.False(t, hasTeam, "team tag should have been removed")
}

func TestBatch2_Tag_ListEmpty(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "tag-empty-cluster")
	encoded := b2EncodedPath(clusterArn)

	// No tags added — ListTagsForResource returns empty map.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/tags/"+encoded, nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	tags, _ := listResp["tags"].(map[string]any)
	assert.Empty(t, tags, "freshly created cluster should have no tags")
}

func TestBatch2_Tag_NotFound(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	_, code := b2ParseOp(t, h, http.MethodGet,
		"/v1/tags/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1", nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// UpdateClusterConfiguration persists via HTTP (v2 path)
// ----------------------------------------

func TestBatch2_UpdateClusterConfiguration_V2Path(t *testing.T) {
	t.Parallel()

	h := b2NewHandler(t)
	clusterArn := b2CreateCluster(t, h, "update-config-v2")
	configArn := b2CreateConfig(t, h, "my-config-v2")
	encoded := b2EncodedPath(clusterArn)

	resp, code := b2ParseOp(t, h, http.MethodPut,
		"/api/v2/clusters/"+encoded+"/configuration",
		map[string]any{
			"currentVersion": kafka.DefaultClusterVersion,
			"configurationInfo": map[string]any{
				"arn":      configArn,
				"revision": int64(1),
			},
		})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])

	// Verify configurationInfo persisted.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	clusterInfo, _ := descResp["clusterInfo"].(map[string]any)
	provisioned, _ := clusterInfo["provisioned"].(map[string]any)
	cfgInfo, _ := provisioned["configurationInfo"].(map[string]any)
	assert.Equal(t, configArn, cfgInfo["arn"])
	assert.InDelta(t, float64(1), cfgInfo["revision"], 0)
}
