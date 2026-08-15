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

func TestUpdateOpsViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "update-ops-cluster")
	encoded := url.PathEscape(clusterArn)

	type updateOp struct {
		body map[string]any
		name string
		path string
	}
	updateOps := []updateOp{
		{
			name: "UpdateBrokerStorage", path: "/v1/clusters/" + encoded + "/nodes/storage",
			body: map[string]any{
				"currentVersion":            "1",
				"targetBrokerEBSVolumeInfo": []map[string]any{{"volumeSizeGB": 100}},
			},
		},
		{
			name: "UpdateBrokerType", path: "/v1/clusters/" + encoded + "/nodes/type",
			body: map[string]any{"currentVersion": "1", "targetInstanceType": "kafka.m5.xlarge"},
		},
		{
			name: "UpdateClusterConfiguration", path: "/v1/clusters/" + encoded + "/configuration",
			body: map[string]any{
				"currentVersion":        "1",
				"configurationArn":      "arn:aws:kafka:us-east-1:000:configuration/test/1",
				"configurationRevision": 1,
			},
		},
		{
			name: "UpdateClusterKafkaVersion", path: "/v1/clusters/" + encoded + "/version",
			body: map[string]any{"currentVersion": "1", "targetKafkaVersion": "3.0.0"},
		},
		{
			name: "UpdateConnectivity", path: "/v1/clusters/" + encoded + "/connectivity",
			body: map[string]any{"currentVersion": "1", "connectivityInfo": map[string]any{}},
		},
		{
			name: "UpdateMonitoring", path: "/v1/clusters/" + encoded + "/monitoring",
			body: map[string]any{"currentVersion": "1", "openMonitoring": map[string]any{}},
		},
		{
			name: "UpdateSecurity", path: "/v1/clusters/" + encoded + "/security",
			body: map[string]any{"currentVersion": "1"},
		},
	}

	for _, op := range updateOps {
		t.Run(op.name, func(t *testing.T) {
			t.Parallel()
			rec := doKafkaRequest(t, h, http.MethodPut, op.path, op.body)
			assert.GreaterOrEqual(t, rec.Code, 200, "op %s should not panic", op.name)
		})
	}
}

// TestKafkaCoverage2_UpdateReplicationInfo covers UpdateReplicationInfo.

func TestRebootBrokerViaHTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "reboot-cluster")
	encoded := url.PathEscape(clusterArn)

	rec := doKafkaRequest(t, h, http.MethodPut, "/v1/clusters/"+encoded+"/reboot-broker",
		map[string]any{"brokerIds": []string{"1"}})
	assert.True(t, rec.Code >= 200 && rec.Code < 300)
}

// TestKafkaCoverage2_UpdateOps covers various kafka update operations.

func TestUpdateOpsRequireCurrentVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body     map[string]any
		name     string
		suffix   string
		wantCode int
	}{
		{
			name:     "broker_count_empty_version_rejected",
			suffix:   "/nodes/count",
			body:     map[string]any{"targetNumberOfBrokerNodes": int32(6)},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "broker_count_wrong_version_rejected",
			suffix: "/nodes/count",
			body: map[string]any{
				"currentVersion":            "WRONG_VERSION",
				"targetNumberOfBrokerNodes": int32(6),
			},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "broker_count_correct_version_accepted",
			suffix: "/nodes/count",
			body: map[string]any{
				"currentVersion":            kafka.DefaultClusterVersion,
				"targetNumberOfBrokerNodes": int32(6),
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "broker_type_empty_version_rejected",
			suffix:   "/nodes/type",
			body:     map[string]any{"targetInstanceType": "kafka.m5.xlarge"},
			wantCode: http.StatusBadRequest,
		},
		{
			name:   "broker_type_correct_version_accepted",
			suffix: "/nodes/type",
			body: map[string]any{
				"currentVersion":     kafka.DefaultClusterVersion,
				"targetInstanceType": "kafka.m5.xlarge",
			},
			wantCode: http.StatusOK,
		},
		{
			name:     "connectivity_empty_version_rejected",
			suffix:   "/connectivity",
			body:     map[string]any{},
			wantCode: http.StatusBadRequest,
		},
		{
			name:     "connectivity_correct_version_accepted",
			suffix:   "/connectivity",
			body:     map[string]any{"currentVersion": kafka.DefaultClusterVersion},
			wantCode: http.StatusOK,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clusterArn := createTestCluster(t, h, "parity-version-check")
			encoded := url.PathEscape(clusterArn)

			rec := doKafkaRequest(t, h, http.MethodPut,
				"/v1/clusters/"+encoded+tt.suffix, tt.body)
			assert.Equal(t, tt.wantCode, rec.Code, "suffix=%s body=%v", tt.suffix, tt.body)

			if tt.wantCode == http.StatusBadRequest {
				var errResp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &errResp))
				assert.Equal(t, "BadRequestException", errResp["__type"],
					"wrong version must produce BadRequestException")
			}
		})
	}
}

func TestUpdateClusterConfiguration_PersistsConfig(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		configArn  string
		revision   int64
		wantArnSet bool
	}{
		{
			name:       "with_valid_config",
			configArn:  "arn:aws:kafka:us-east-1:123:configuration/my-cfg/abc-123",
			revision:   5,
			wantArnSet: true,
		},
		{
			name:       "empty_config_arn",
			configArn:  "",
			revision:   1,
			wantArnSet: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			b := kafka.NewInMemoryBackend(testAccountID, testRegion)
			cl := b.AddClusterInternal("cfg-update-cl", "3.5.1")

			op, err := b.UpdateClusterConfiguration(context.Background(), cl.ClusterArn, tt.configArn, tt.revision)
			require.NoError(t, err)
			require.NotNil(t, op)
			assert.Equal(t, "UPDATE_CLUSTER_CONFIGURATION", op.OperationType)

			described, err := b.DescribeCluster(context.Background(), cl.ClusterArn)
			require.NoError(t, err)

			if tt.wantArnSet {
				require.NotNil(t, described.ConfigurationInfo,
					"ConfigurationInfo should be set after UpdateClusterConfiguration")
				assert.Equal(t, tt.configArn, described.ConfigurationInfo.Arn)
				assert.Equal(t, tt.revision, described.ConfigurationInfo.Revision)
			}
		})
	}
}

// TestRefinement2_ListKafkaVersions_IncludesKRaft verifies KRaft variants are in the list.

func TestUpdateClusterConfiguration_HTTP(t *testing.T) {
	t.Parallel()

	h, backend := newTestHandlerWithBackend(t)
	cl := backend.AddClusterInternal("cfg-update-http", "3.5.1")
	encoded := url.PathEscape(cl.ClusterArn)

	configArn := "arn:aws:kafka:us-east-1:123:configuration/my-cfg/abc-123"

	rec := doKafkaRequest(t, h, http.MethodPut, "/v1/clusters/"+encoded+"/configuration",
		map[string]any{
			"currentVersion": cl.CurrentVersion,
			"configurationInfo": map[string]any{
				"arn":      configArn,
				"revision": 5,
			},
		})
	require.Equal(t, http.StatusOK, rec.Code)

	// Verify via DescribeCluster.
	described, err := backend.DescribeCluster(context.Background(), cl.ClusterArn)
	require.NoError(t, err)
	require.NotNil(t, described.ConfigurationInfo)
	assert.Equal(t, configArn, described.ConfigurationInfo.Arn)
	assert.Equal(t, int64(5), described.ConfigurationInfo.Revision)
}

// TestRefinement2_GetBootstrapBrokers_ScramePublic verifies public SCRAM broker string.

func TestUpdateBrokerCount_Persists(t *testing.T) {
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

			h := newTestHandler(t)
			clusterArn := createTestClusterWithStorage(t, h, "update-broker-count-"+tt.name)
			encoded := url.PathEscape(clusterArn)

			resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
				"/v1/clusters/"+encoded+"/nodes/count",
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

// ----------------------------------------
// UpdateBrokerStorage: persists new volume size
// ----------------------------------------

func TestUpdateBrokerStorage_Persists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "update-broker-storage")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/nodes/storage",
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

// ----------------------------------------
// UpdateBrokerType: persists new instance type
// ----------------------------------------

func TestUpdateBrokerType_Persists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "update-broker-type")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/nodes/type",
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

// ----------------------------------------
// UpdateClusterKafkaVersion: persists new version
// ----------------------------------------

func TestUpdateClusterKafkaVersion_Persists(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "update-kafka-version")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/version",
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
	assert.Equal(
		t,
		"3.5.1",
		provisioned["currentBrokerSoftwareInfo"].(map[string]any)["kafkaVersion"],
	)
}

// TestUpdateBrokerOpsNotFound covers the NotFound path for every broker/cluster
// update op that targets a missing cluster ARN (UpdateBrokerCount,
// UpdateBrokerStorage, UpdateBrokerType, UpdateClusterKafkaVersion).
func TestUpdateBrokerOpsNotFound(t *testing.T) {
	t.Parallel()

	const missingClusterPath = "/v1/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1"

	tests := []struct {
		body       map[string]any
		name       string
		pathSuffix string
	}{
		{
			name:       "update_broker_count",
			pathSuffix: "/nodes/count",
			body: map[string]any{
				"currentVersion":            kafka.DefaultClusterVersion,
				"targetNumberOfBrokerNodes": int32(6),
			},
		},
		{
			name:       "update_broker_storage",
			pathSuffix: "/nodes/storage",
			body:       map[string]any{"currentVersion": kafka.DefaultClusterVersion},
		},
		{
			name:       "update_broker_type",
			pathSuffix: "/nodes/type",
			body: map[string]any{
				"currentVersion":     kafka.DefaultClusterVersion,
				"targetInstanceType": "kafka.m5.xlarge",
			},
		},
		{
			name:       "update_cluster_kafka_version",
			pathSuffix: "/version",
			body: map[string]any{
				"currentVersion":     kafka.DefaultClusterVersion,
				"targetKafkaVersion": "3.5.1",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, code := doKafkaRequestJSON(t, h, http.MethodPut, missingClusterPath+tt.pathSuffix, tt.body)
			assert.Equal(t, http.StatusNotFound, code)
		})
	}
}

// ----------------------------------------
// Stub update ops (Connectivity, Monitoring, Rebalancing, Security, Storage)
// These don't persist state but must return 200 + clusterOperationArn.
// ----------------------------------------

func TestStubUpdateOps_HappyPath(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
		method string
	}{
		{name: "connectivity", suffix: "/connectivity", method: http.MethodPut},
		{name: "monitoring", suffix: "/monitoring", method: http.MethodPut},
		{name: "rebalancing", suffix: "/rebalancing", method: http.MethodPut},
		{name: "security", suffix: "/security", method: http.MethodPatch},
		{name: "storage", suffix: "/storage", method: http.MethodPut},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clusterArn := createTestClusterWithStorage(t, h, "stub-update-"+tt.name)
			encoded := url.PathEscape(clusterArn)

			resp, code := doKafkaRequestJSON(t, h, tt.method,
				"/v1/clusters/"+encoded+tt.suffix,
				map[string]any{"currentVersion": kafka.DefaultClusterVersion})
			assert.Equal(t, http.StatusOK, code, "suffix=%s", tt.suffix)
			assert.NotEmpty(t, resp["clusterOperationArn"],
				"should return clusterOperationArn for %s", tt.name)
		})
	}
}

func TestStubUpdateOps_NotFound(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		suffix string
		method string
	}{
		{name: "connectivity", suffix: "/connectivity", method: http.MethodPut},
		{name: "monitoring", suffix: "/monitoring", method: http.MethodPut},
		{name: "rebalancing", suffix: "/rebalancing", method: http.MethodPut},
		{name: "security", suffix: "/security", method: http.MethodPatch},
		{name: "storage", suffix: "/storage", method: http.MethodPut},
	}

	missingARN := "arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1"

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			_, code := doKafkaRequestJSON(t, h, tt.method,
				"/v1/clusters/"+missingARN+tt.suffix,
				map[string]any{"currentVersion": kafka.DefaultClusterVersion})
			assert.Equal(t, http.StatusNotFound, code, "suffix=%s", tt.suffix)
		})
	}
}

// ----------------------------------------
// RebootBroker
// ----------------------------------------

func TestRebootBroker_HappyPath(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "reboot-broker-cluster")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/reboot-broker",
		map[string]any{"brokerIds": []string{"0", "1"}})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])
}

func TestRebootBroker_EmptyBrokerIds(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "reboot-empty-brokers")
	encoded := url.PathEscape(clusterArn)

	// Empty brokerIds is accepted — AWS accepts it as a no-op reboot request.
	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/reboot-broker",
		map[string]any{"brokerIds": []string{}})
	assert.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])
}

func TestRebootBroker_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, code := doKafkaRequestJSON(
		t,
		h,
		http.MethodPut,
		"/v1/clusters/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Acluster%2Fmissing%2F1/reboot-broker",
		map[string]any{"brokerIds": []string{"0"}},
	)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// ClusterOperation tracking
// Update ops must create operations retrievable via List/Describe (v1 + v2).
// ----------------------------------------

func TestUpdateClusterConfiguration_V2Path(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestClusterWithStorage(t, h, "update-config-v2")
	configArn := createTestConfig(t, h, "my-config-v2")
	encoded := url.PathEscape(clusterArn)

	resp, code := doKafkaRequestJSON(t, h, http.MethodPut,
		"/v1/clusters/"+encoded+"/configuration",
		map[string]any{
			"currentVersion": kafka.DefaultClusterVersion,
			"configurationInfo": map[string]any{
				"arn":      configArn,
				"revision": int64(1),
			},
		})
	require.Equal(t, http.StatusOK, code)
	assert.NotEmpty(t, resp["clusterOperationArn"])

	// The real DescribeClusterV2Output.Provisioned (types.Provisioned) has no
	// configurationInfo member -- field-diffed against kafka@v1.57.2
	// deserializers.go's awsRestjson1_deserializeDocumentProvisioned, which
	// has no such case. AWS only surfaces the active/target configuration via
	// the cluster operation record (DescribeClusterOperation's
	// sourceClusterInfo/targetClusterInfo), not via DescribeCluster(V2).
	// gopherstack previously fabricated this field here; this test used to
	// assert that wrong shape as correct. The persisted configuration itself
	// is covered at the domain level by TestUpdateClusterConfiguration_HTTP/
	// TestUpdateClusterConfiguration_PersistsConfig.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/api/v2/clusters/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)

	var descResp map[string]any
	require.NoError(t, json.Unmarshal(descRec.Body.Bytes(), &descResp))
	clusterInfo, _ := descResp["clusterInfo"].(map[string]any)
	provisioned, _ := clusterInfo["provisioned"].(map[string]any)
	assert.NotContains(t, provisioned, "configurationInfo",
		"configurationInfo is not a real Provisioned member; must not be on the wire")
}
