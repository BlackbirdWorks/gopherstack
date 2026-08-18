package kafka_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestKafka_DescribeClusterOperation(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		useRealArn bool
		wantStatus int
	}{
		{name: "success", useRealArn: true, wantStatus: http.StatusOK},
		{name: "not_found", useRealArn: false, wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h, backend := newTestHandlerWithBackend(t)

			// Create a cluster and an operation via internal helper
			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "op-cluster",
				"kafkaVersion":        "2.8.0",
				"numberOfBrokerNodes": 3,
				"brokerNodeGroupInfo": map[string]any{
					"instanceType":  "kafka.m5.large",
					"clientSubnets": []string{"subnet-1"},
				},
			})
			require.Equal(t, http.StatusOK, createRec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
			clusterArn := createResp["clusterArn"].(string)

			op := backend.AddClusterOperationInternal(clusterArn, "UPDATE_BROKER_COUNT")

			var operationArn string
			if tt.useRealArn {
				operationArn = op.ClusterOperationArn
			} else {
				operationArn = "arn:aws:kafka:us-east-1:000000000000:cluster-operation/nonexistent/uuid"
			}

			encodedArn := url.PathEscape(operationArn)
			rec := doKafkaRequest(t, h, http.MethodGet, "/v1/operations/"+encodedArn, nil)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

				opInfo, ok := resp["clusterOperationInfo"].(map[string]any)
				require.True(t, ok)
				assert.Equal(t, kafka.ClusterOperationStateUpdateComplete, opInfo["operationState"])
			}
		})
	}
}

// ----------------------------------------
// Path parsing tests for new operations
// ----------------------------------------

func TestClusterOperationsViaBackend(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createTestClusterOneBroker(t, h, "ops-cluster")

	_, err := be.UpdateBrokerCount(context.Background(), clusterArn, 2)
	require.NoError(t, err)

	// ListClusterOperations
	ops, err := be.ListClusterOperations(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, ops)

	// ListClusterOperationsV2
	ops2, err := be.ListClusterOperationsV2(context.Background(), clusterArn)
	require.NoError(t, err)
	assert.NotEmpty(t, ops2)

	if len(ops2) > 0 {
		op, opErr := be.DescribeClusterOperationV2(context.Background(), ops2[0].ClusterOperationArn)
		require.NoError(t, opErr)
		assert.Equal(t, ops2[0].ClusterOperationArn, op.ClusterOperationArn)
	}
}

func TestClusterOperationTracking_V1(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "op-tracking-v1")
	encoded := url.PathEscape(clusterArn)

	// Trigger two update ops. Real MSK bumps CurrentVersion on every successful
	// update, so the second call must fetch the version the first call left
	// behind rather than reusing DefaultClusterVersion (a stale-version reuse
	// is correctly rejected -- see TestUpdateOpsRequireCurrentVersion).
	resp1, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/nodes/count",
		map[string]any{
			"currentVersion":            kafka.DefaultClusterVersion,
			"targetNumberOfBrokerNodes": int32(6),
		})
	require.Equal(t, http.StatusOK, code)
	op1Arn, _ := resp1["clusterOperationArn"].(string)
	require.NotEmpty(t, op1Arn)

	descAfterFirst := decodeJSONResponse(t, doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encoded, nil))
	clusterInfoAfterFirst, ok := descAfterFirst["clusterInfo"].(map[string]any)
	require.True(t, ok)
	versionAfterFirst, _ := clusterInfoAfterFirst["currentVersion"].(string)
	require.NotEmpty(t, versionAfterFirst)

	resp2, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/nodes/type",
		map[string]any{
			"currentVersion":     versionAfterFirst,
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
		if arn, arnOK := om["operationArn"].(string); arnOK {
			opArns = append(opArns, arn)
		}
	}
	assert.Contains(t, opArns, op1Arn)
	assert.Contains(t, opArns, op2Arn)

	// DescribeClusterOperation (v1) for both.
	for _, opArn := range []string{op1Arn, op2Arn} {
		descRec := doKafkaRequest(t, h, http.MethodGet, "/v1/operations/"+url.PathEscape(opArn), nil)
		require.Equal(t, http.StatusOK, descRec.Code)

		var descResp map[string]any
		require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
		opInfo, infoOK := descResp["clusterOperationInfo"].(map[string]any)
		require.True(t, infoOK)
		assert.Equal(t, opArn, opInfo["operationArn"])
		assert.Equal(t, clusterArn, opInfo["clusterArn"])
		assert.NotEmpty(t, opInfo["operationType"])
	}
}

func TestClusterOperationTracking_V2(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "op-tracking-v2")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/monitoring",
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
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/operations/"+url.PathEscape(opArn), nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	opInfo, ok := descResp["clusterOperationInfo"].(map[string]any)
	require.True(t, ok)
	assert.Equal(t, opArn, opInfo["operationArn"])
}

// TestListClusterOperationsV2_OmitsDescribeOnlyFields covers gopherstack-dv4s:
// ListClusterOperationsV2 previously marshaled the same *ClusterOperation
// domain struct DescribeClusterOperationV2 uses, leaking sourceClusterInfo/
// targetClusterInfo, which real types.ClusterOperationV2Summary never
// declares (unlike V1, where List and Describe genuinely share one real
// type, ClusterOperationInfo -- V2 splits them). UPDATE_MONITORING always
// populates both source and target as non-nil *MutableClusterInfo, so even
// with every sub-field left zero-valued they still serialize as
// non-omitted `{}` objects -- enough for this fixture to actually fail
// against the pre-fix code, not pass vacuously.
func TestListClusterOperationsV2_OmitsDescribeOnlyFields(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "op-v2-omit-test")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/monitoring",
		map[string]any{
			"currentVersion":     kafka.DefaultClusterVersion,
			"enhancedMonitoring": "PER_BROKER",
		})
	require.Equal(t, http.StatusOK, code)
	opArn, _ := resp["clusterOperationArn"].(string)
	require.NotEmpty(t, opArn)

	listRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded+"/operations", nil)
	require.Equal(t, http.StatusOK, listRec.Code)

	var listResp map[string]any
	require.NoError(t, json.Unmarshal(listRec.Body.Bytes(), &listResp))
	opList, ok := listResp["clusterOperationInfoList"].([]any)
	require.True(t, ok)
	require.NotEmpty(t, opList)

	op, ok := opList[0].(map[string]any)
	require.True(t, ok)

	forbidden := []string{"sourceClusterInfo", "targetClusterInfo", "clusterOperationArn"}
	for _, key := range forbidden {
		assert.NotContains(t, op, key)
	}

	assert.Equal(t, opArn, op["operationArn"])
	assert.Equal(t, clusterArn, op["clusterArn"])
	assert.Equal(t, "UPDATE_MONITORING", op["operationType"])
	assert.Contains(t, op, "operationState")
}

func TestClusterOperation_DescribeNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	missingOpArn := "arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster-operation%2Fnone"
	_, code := doKafkaRequestJSON(t, h, http.MethodGet, "/v1/operations/"+missingOpArn, nil)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// SCRAM secret lifecycle
// ----------------------------------------
