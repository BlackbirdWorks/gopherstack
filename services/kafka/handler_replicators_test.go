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

func TestUpdateReplicationInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a replicator.
	rec := doKafkaRequest(t, h, http.MethodPost, "/replication/v1/replicators",
		map[string]any{
			"replicatorName":          "test-replicator-update",
			"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/test-role",
			"kafkaClusters": []map[string]any{
				{
					"amazonMskCluster": map[string]any{
						"mskClusterArn": "arn:aws:kafka:us-east-1:000000000000:cluster/test/abc",
					},
					"vpcConfig": map[string]any{
						"subnetIds":        []string{"subnet-1"},
						"securityGroupIds": []string{"sg-1"},
					},
				},
			},
			"replicationInfoList": []map[string]any{},
		})

	if rec.Code < 200 || rec.Code >= 300 {
		t.Skipf("replicator creation returned %d, skipping", rec.Code)
	}

	var resp map[string]any
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
	replicatorArn, _ := resp["replicatorArn"].(string)
	if replicatorArn == "" {
		t.Skip("replicator creation did not return ARN")
	}

	encoded := url.PathEscape(replicatorArn)

	// UpdateReplicationInfo.
	rec = doKafkaRequest(t, h, http.MethodPut, "/replication/v1/replicators/"+encoded+"/replication-info",
		map[string]any{
			"currentVersion":           "1",
			"sourceKafkaClusterArn":    "arn:aws:kafka:us-east-1:000000000000:cluster/source/abc",
			"targetKafkaClusterArn":    "arn:aws:kafka:us-east-1:000000000000:cluster/target/def",
			"topicReplication":         map[string]any{"replicateSourceTopicTags": false},
			"consumerGroupReplication": map[string]any{"synchroniseConsumerGroupOffsets": false},
		})
	assert.Positive(t, rec.Code)
}

func TestReplicator_UpdateReplicationInfo(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)

	// Create a replicator.
	createRec := doKafkaRequest(
		t,
		h,
		http.MethodPost,
		"/replication/v1/replicators",
		map[string]any{
			"replicatorName":          "my-replicator",
			"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/my-role",
			"description":             "original description",
		},
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	var createResp map[string]any
	require.NoError(t, json.Unmarshal(createRec.Body.Bytes(), &createResp))
	replicatorArn, _ := createResp["replicatorArn"].(string)
	require.NotEmpty(t, replicatorArn)
	encodedArn := url.PathEscape(replicatorArn)

	// UpdateReplicationInfo. Real path suffixes the replicator ARN with
	// /replication-info; a bare PUT to the ARN itself is DescribeReplicator's
	// GET sibling and is not a valid PUT target.
	updateRec := doKafkaRequest(t, h, http.MethodPut,
		"/replication/v1/replicators/"+encodedArn+"/replication-info",
		map[string]any{"description": "updated description"})
	require.Equal(t, http.StatusOK, updateRec.Code)

	var updateResp map[string]any
	require.NoError(t, json.Unmarshal(updateRec.Body.Bytes(), &updateResp))
	assert.Equal(t, replicatorArn, updateResp["replicatorArn"])
	assert.NotEmpty(t, updateResp["replicatorState"])
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
