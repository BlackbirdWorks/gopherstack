package kafka_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/kafka"
)

func TestKafka_CreateReplicator(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"replicatorName":          "my-replicator",
				"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
			},
			wantStatus: http.StatusOK,
		},
		{
			name:       "invalid_body",
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(http.MethodPost, "/replication/v1/replicators", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-replicator", resp["replicatorName"])
				assert.NotEmpty(t, resp["replicatorArn"])
				assert.Equal(t, kafka.ReplicatorStateRunning, resp["replicatorState"])
			}
		})
	}
}

func TestKafka_DeleteReplicator(t *testing.T) {
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

			h := newTestHandler(t)

			e := echo.New()
			bodyBytes, _ := json.Marshal(map[string]any{
				"replicatorName":          "my-replicator",
				"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
			})
			req := httptest.NewRequest(http.MethodPost, "/replication/v1/replicators", bytes.NewReader(bodyBytes))
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)
			require.Equal(t, http.StatusOK, rec.Code)

			var createResp map[string]any
			require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &createResp))

			var replicatorArn string
			if tt.useRealArn {
				replicatorArn = createResp["replicatorArn"].(string)
			} else {
				replicatorArn = "arn:aws:kafka:us-east-1:000000000000:replicator/nonexistent/uuid"
			}

			encodedArn := url.PathEscape(replicatorArn)
			req2 := httptest.NewRequest(http.MethodDelete, "/replication/v1/replicators/"+encodedArn, http.NoBody)
			rec2 := httptest.NewRecorder()
			c2 := e.NewContext(req2, rec2)
			err2 := h.Handler()(c2)
			require.NoError(t, err2)

			assert.Equal(t, tt.wantStatus, rec2.Code)
		})
	}
}

// ----------------------------------------
// Topic handler tests
// ----------------------------------------

func TestDescribeReplicator(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	arn := createTestReplicator(t, h, "desc-repl")

	rec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators/"+arn, nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := decodeJSONResponse(t, rec)
	assert.Equal(t, arn, resp["replicatorArn"])
}

func TestListReplicators(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	createTestReplicator(t, h, "list-repl-1")
	createTestReplicator(t, h, "list-repl-2")

	rec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators", nil)
	assert.Equal(t, http.StatusOK, rec.Code)
	resp := decodeJSONResponse(t, rec)
	repls, ok := resp["replicators"].([]any)
	assert.True(t, ok)
	assert.Len(t, repls, 2)
}

func TestListReplicatorsPagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	for i := range 3 {
		b.AddReplicatorInternal(fmt.Sprintf("rep-%02d", i))
	}

	rec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators?maxResults=2", nil)
	require.Equal(t, http.StatusOK, rec.Code)

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))

	reps := resp["replicators"].([]any)
	assert.Len(t, reps, 2)

	nextToken, _ := resp["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2.
	path2 := "/replication/v1/replicators?maxResults=2&nextToken=" + url.QueryEscape(nextToken)
	rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	reps2 := resp2["replicators"].([]any)
	assert.Len(t, reps2, 1)
	assert.Empty(t, resp2["nextToken"])
}

// ----------------------------------------
// Pagination: ListTopics
// ----------------------------------------

// createTestReplicatorWithTopology creates a replicator with one kafkaClusters
// entry and one source->target replicationInfoList entry via the HTTP API,
// returning its ARN, currentVersion, source ARN, and target ARN so callers
// can drive a real UpdateReplicationInfo call against it.
func createTestReplicatorWithTopology(
	t *testing.T, h *kafka.Handler, name string,
) (string, string, string, string) {
	t.Helper()

	sourceArn := "arn:aws:kafka:us-east-1:000000000000:cluster/source/abc"
	targetArn := "arn:aws:kafka:us-east-1:000000000000:cluster/target/def"

	rec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators",
		map[string]any{
			"replicatorName":          name,
			"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
			"kafkaClusters": []map[string]any{
				{
					"amazonMskCluster": map[string]any{"mskClusterArn": sourceArn},
					"vpcConfig": map[string]any{
						"subnetIds":        []string{"subnet-1"},
						"securityGroupIds": []string{"sg-1"},
					},
				},
				{
					"amazonMskCluster": map[string]any{"mskClusterArn": targetArn},
					"vpcConfig": map[string]any{
						"subnetIds":        []string{"subnet-2"},
						"securityGroupIds": []string{"sg-2"},
					},
				},
			},
			"replicationInfoList": []map[string]any{
				{
					"sourceKafkaClusterArn": sourceArn,
					"targetKafkaClusterArn": targetArn,
					"targetCompressionType": "NONE",
					"topicReplication": map[string]any{
						"topicsToReplicate":               []string{".*"},
						"copyAccessControlListsForTopics": true,
						"copyTopicConfigurations":         true,
						"detectAndCopyNewTopics":          true,
					},
					"consumerGroupReplication": map[string]any{
						"consumerGroupsToReplicate":       []string{".*"},
						"detectAndCopyNewConsumerGroups":  true,
						"synchroniseConsumerGroupOffsets": true,
					},
				},
			},
		})
	require.Equal(t, http.StatusOK, rec.Code, "create replicator: %s", rec.Body.String())

	resp := decodeJSONResponse(t, rec)
	replicatorArn, _ := resp["replicatorArn"].(string)
	require.NotEmpty(t, replicatorArn)

	descRec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators/"+url.PathEscape(replicatorArn), nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	descResp := decodeJSONResponse(t, descRec)
	currentVersion, _ := descResp["currentVersion"].(string)
	require.NotEmpty(t, currentVersion)

	return replicatorArn, currentVersion, sourceArn, targetArn
}

func TestUpdateReplicationInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	replicatorArn, currentVersion, sourceArn, targetArn := createTestReplicatorWithTopology(
		t, h, "test-replicator-update",
	)
	encoded := url.PathEscape(replicatorArn)

	rec := doKafkaRequest(t, h, http.MethodPut, "/replication/v1/replicators/"+encoded+"/replication-info",
		map[string]any{
			"currentVersion":        currentVersion,
			"sourceKafkaClusterArn": sourceArn,
			"targetKafkaClusterArn": targetArn,
			"topicReplication": map[string]any{
				"topicsToReplicate":               []string{"orders.*"},
				"copyAccessControlListsForTopics": false,
				"copyTopicConfigurations":         false,
				"detectAndCopyNewTopics":          false,
			},
			"consumerGroupReplication": map[string]any{
				"consumerGroupsToReplicate":       []string{"orders-.*"},
				"detectAndCopyNewConsumerGroups":  false,
				"synchroniseConsumerGroupOffsets": false,
			},
		})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	resp := decodeJSONResponse(t, rec)
	assert.Equal(t, replicatorArn, resp["replicatorArn"])
	assert.NotEmpty(t, resp["replicatorState"])

	// The update must actually persist: DescribeReplicator reflects the new
	// topic/consumer-group replication settings on the matching flow.
	descRec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	descResp := decodeJSONResponse(t, descRec)
	flows := descResp["replicationInfoList"].([]any)
	require.Len(t, flows, 1)
	flow := flows[0].(map[string]any)
	topicReplication := flow["topicReplication"].(map[string]any)
	topicsToReplicate := topicReplication["topicsToReplicate"].([]any)
	require.Len(t, topicsToReplicate, 1)
	assert.Equal(t, "orders.*", topicsToReplicate[0])
}

func TestReplicator_UpdateReplicationInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	replicatorArn, currentVersion, sourceArn, targetArn := createTestReplicatorWithTopology(
		t, h, "my-replicator",
	)
	encodedArn := url.PathEscape(replicatorArn)

	// UpdateReplicationInfo. Real path suffixes the replicator ARN with
	// /replication-info; a bare PUT to the ARN itself is DescribeReplicator's
	// GET sibling and is not a valid PUT target.
	updateRec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+encodedArn+"/replication-info",
		map[string]any{
			"currentVersion":        currentVersion,
			"sourceKafkaClusterArn": sourceArn,
			"targetKafkaClusterArn": targetArn,
			"topicReplication": map[string]any{
				"topicsToReplicate":               []string{".*"},
				"copyAccessControlListsForTopics": true,
				"copyTopicConfigurations":         true,
				"detectAndCopyNewTopics":          true,
			},
			"consumerGroupReplication": map[string]any{
				"consumerGroupsToReplicate":       []string{".*"},
				"detectAndCopyNewConsumerGroups":  true,
				"synchroniseConsumerGroupOffsets": true,
			},
		})
	require.Equal(t, http.StatusOK, updateRec.Code, updateRec.Body.String())

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, replicatorArn, updateResp["replicatorArn"])
	assert.NotEmpty(t, updateResp["replicatorState"])
}

func TestReplicator_UpdateReplicationInfo_WrongVersionRejected(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	replicatorArn, _, sourceArn, targetArn := createTestReplicatorWithTopology(t, h, "wrong-version-repl")
	encodedArn := url.PathEscape(replicatorArn)

	rec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+encodedArn+"/replication-info",
		map[string]any{
			"currentVersion":        "STALE_VERSION",
			"sourceKafkaClusterArn": sourceArn,
			"targetKafkaClusterArn": targetArn,
		})
	assert.Equal(t, http.StatusBadRequest, rec.Code)
}

func TestReplicator_UpdateReplicationInfo_UnknownFlowNotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	replicatorArn, currentVersion, _, _ := createTestReplicatorWithTopology(t, h, "unknown-flow-repl")
	encodedArn := url.PathEscape(replicatorArn)

	rec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+encodedArn+"/replication-info",
		map[string]any{
			"currentVersion":        currentVersion,
			"sourceKafkaClusterArn": "arn:aws:kafka:us-east-1:000000000000:cluster/no-such-source/abc",
			"targetKafkaClusterArn": "arn:aws:kafka:us-east-1:000000000000:cluster/no-such-target/def",
		})
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestReplicator_UpdateReplicationInfo_NotFound(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	_, code := doKafkaRequestJSON(
		t,
		h,
		http.MethodPut,
		"/replication/v1/replicators/arn%3Aaws%3Akafka%3Aus-east-1%3A000000000000%3Areplicator%2Fmissing/replication-info",
		map[string]any{"description": "nope"},
	)
	assert.Equal(t, http.StatusNotFound, code)
}

// ----------------------------------------
// Tag CRUD
// ----------------------------------------

// TestUpdateReplicationInfo_PreservesStartingPositionAndTopicNameConfig
// guards against gopherstack-1vv2: UpdateReplicationInfoInput.TopicReplication
// is types.TopicReplicationUpdate, which declares no startingPosition or
// topicNameConfiguration at all -- both are immutable after Create, so a
// real client's Update payload can never carry either. Wholesale-replacing
// the stored TopicReplication with that narrower payload used to silently
// erase both on every Update call that touched topicReplication at all.
func TestUpdateReplicationInfo_PreservesStartingPositionAndTopicNameConfig(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	sourceArn := "arn:aws:kafka:us-east-1:000000000000:cluster/preserve-source/abc"
	targetArn := "arn:aws:kafka:us-east-1:000000000000:cluster/preserve-target/def"

	rec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators",
		map[string]any{
			"replicatorName":          "preserve-starting-position-repl",
			"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
			"kafkaClusters": []map[string]any{
				{"amazonMskCluster": map[string]any{"mskClusterArn": sourceArn}},
				{"amazonMskCluster": map[string]any{"mskClusterArn": targetArn}},
			},
			"replicationInfoList": []map[string]any{
				{
					"sourceKafkaClusterArn": sourceArn,
					"targetKafkaClusterArn": targetArn,
					"topicReplication": map[string]any{
						"topicsToReplicate":               []string{".*"},
						"copyAccessControlListsForTopics": true,
						"copyTopicConfigurations":         true,
						"detectAndCopyNewTopics":          true,
						"startingPosition":                map[string]any{"type": "TIMESTAMP"},
						"topicNameConfiguration":          map[string]any{"type": "PREFIXED_WITH_SOURCE_CLUSTER_ALIAS"},
					},
					"consumerGroupReplication": map[string]any{
						"consumerGroupsToReplicate": []string{".*"},
					},
				},
			},
		})
	require.Equal(t, http.StatusOK, rec.Code, rec.Body.String())

	resp := decodeJSONResponse(t, rec)
	replicatorArn, _ := resp["replicatorArn"].(string)
	require.NotEmpty(t, replicatorArn)
	encoded := url.PathEscape(replicatorArn)

	descRec := doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	descResp := decodeJSONResponse(t, descRec)
	currentVersion, _ := descResp["currentVersion"].(string)
	require.NotEmpty(t, currentVersion)

	// A real client's UpdateReplicationInfoInput.TopicReplication can never
	// carry startingPosition/topicNameConfiguration -- only the fields
	// below exist on the wire for this op.
	updateRec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+encoded+"/replication-info",
		map[string]any{
			"currentVersion":        currentVersion,
			"sourceKafkaClusterArn": sourceArn,
			"targetKafkaClusterArn": targetArn,
			"topicReplication": map[string]any{
				"topicsToReplicate":               []string{"orders.*"},
				"copyAccessControlListsForTopics": false,
				"copyTopicConfigurations":         false,
				"detectAndCopyNewTopics":          false,
			},
		})
	require.Equal(t, http.StatusOK, updateRec.Code, updateRec.Body.String())

	descRec = doKafkaRequest(t, h, http.MethodGet, "/replication/v1/replicators/"+encoded, nil)
	require.Equal(t, http.StatusOK, descRec.Code)
	descResp = decodeJSONResponse(t, descRec)
	flows := descResp["replicationInfoList"].([]any)
	require.Len(t, flows, 1)
	flow := flows[0].(map[string]any)
	topicReplication := flow["topicReplication"].(map[string]any)

	topicsToReplicate := topicReplication["topicsToReplicate"].([]any)
	require.Len(t, topicsToReplicate, 1)
	assert.Equal(t, "orders.*", topicsToReplicate[0], "the update's own field must apply")

	startingPosition, ok := topicReplication["startingPosition"].(map[string]any)
	require.True(t, ok, "startingPosition must survive an Update the real API can never send it on")
	assert.Equal(t, "TIMESTAMP", startingPosition["type"])

	topicNameConfiguration, ok := topicReplication["topicNameConfiguration"].(map[string]any)
	require.True(t, ok, "topicNameConfiguration must survive an Update the real API can never send it on")
	assert.Equal(t, "PREFIXED_WITH_SOURCE_CLUSTER_ALIAS", topicNameConfiguration["type"])
}
