package kafka_test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/labstack/echo/v5"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKafka_CreateTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name: "success",
			body: map[string]any{
				"topicName":         "my-topic",
				"replicationFactor": 1,
				"partitionCount":    3,
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

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "topic-cluster",
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
			encodedArn := url.PathEscape(clusterArn)

			var bodyBytes []byte
			if tt.body != nil {
				var err error
				bodyBytes, err = json.Marshal(tt.body)
				require.NoError(t, err)
			} else {
				bodyBytes = []byte("not-json")
			}

			e := echo.New()
			req := httptest.NewRequest(
				http.MethodPost,
				"/v1/clusters/"+encodedArn+"/topics",
				bytes.NewReader(bodyBytes),
			)
			req.Header.Set("Content-Type", "application/json")
			rec := httptest.NewRecorder()
			c := e.NewContext(req, rec)
			err := h.Handler()(c)
			require.NoError(t, err)

			assert.Equal(t, tt.wantStatus, rec.Code)

			if tt.wantStatus == http.StatusOK {
				var resp map[string]any
				require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp))
				assert.Equal(t, "my-topic", resp["topicName"])
			}
		})
	}
}

func TestKafka_DeleteTopic(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		topicName  string
		wantStatus int
	}{
		{name: "success", topicName: "my-topic", wantStatus: http.StatusOK},
		{name: "not_found", topicName: "nonexistent-topic", wantStatus: http.StatusNotFound},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)

			createRec := doKafkaRequest(t, h, http.MethodPost, "/v1/clusters", map[string]any{
				"clusterName":         "topic-cluster",
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
			encodedArn := url.PathEscape(clusterArn)

			// Create topic
			e := echo.New()
			topicBody, _ := json.Marshal(
				map[string]any{"topicName": "my-topic", "replicationFactor": 1, "partitionCount": 3},
			)
			reqCreate := httptest.NewRequest(
				http.MethodPost,
				"/v1/clusters/"+encodedArn+"/topics",
				bytes.NewReader(topicBody),
			)
			reqCreate.Header.Set("Content-Type", "application/json")
			recCreate := httptest.NewRecorder()
			cCreate := e.NewContext(reqCreate, recCreate)
			require.NoError(t, h.Handler()(cCreate))
			require.Equal(t, http.StatusOK, recCreate.Code)

			// Delete topic
			reqDel := httptest.NewRequest(
				http.MethodDelete,
				"/v1/clusters/"+encodedArn+"/topics/"+tt.topicName,
				http.NoBody,
			)
			recDel := httptest.NewRecorder()
			cDel := e.NewContext(reqDel, recDel)
			require.NoError(t, h.Handler()(cDel))

			assert.Equal(t, tt.wantStatus, recDel.Code)
		})
	}
}

// ----------------------------------------
// VPC connection handler tests
// ----------------------------------------

func TestTopicsLifecycle(t *testing.T) {
	t.Parallel()

	h, be := newTestHandlerWithBackend(t)
	clusterArn := createTestClusterOneBroker(t, h, "topic-cluster")

	// CreateTopic
	topic, err := be.CreateTopic(context.Background(), clusterArn, "my-topic", 1, 3, "")
	require.NoError(t, err)
	assert.Equal(t, "my-topic", topic.TopicName)

	// DescribeTopicPartitions
	parts, err := be.DescribeTopicPartitions(context.Background(), clusterArn, "my-topic")
	require.NoError(t, err)
	assert.Len(t, parts, 3)

	// ListTopics
	topics, err := be.ListTopics(context.Background(), clusterArn, "")
	require.NoError(t, err)
	assert.NotEmpty(t, topics)

	// UpdateTopic
	updated, err := be.UpdateTopic(
		context.Background(),
		clusterArn,
		"my-topic",
		6,
		"cmV0ZW50aW9uLm1zPTg2NDAwMDAw",
	)
	require.NoError(t, err)
	assert.Equal(t, int32(6), updated.PartitionCount)

	// DeleteTopic
	err = be.DeleteTopic(context.Background(), clusterArn, "my-topic")
	require.NoError(t, err)

	// DescribeTopicPartitions after delete → not found
	_, err = be.DescribeTopicPartitions(context.Background(), clusterArn, "my-topic")
	assert.Error(t, err)
}

func TestDescribeTopicViaBackend(t *testing.T) {
	t.Parallel()

	_, be := newTestHandlerWithBackend(t)
	h, be2 := newTestHandlerWithBackend(t)
	_ = be
	clusterArn := createTestClusterOneBroker(t, h, "dt-cluster")

	_, err := be2.CreateTopic(context.Background(), clusterArn, "dt-topic", 1, 1, "")
	require.NoError(t, err)

	// DescribeTopic
	topic, err := be2.DescribeTopic(context.Background(), clusterArn, "dt-topic")
	require.NoError(t, err)
	assert.Equal(t, "dt-topic", topic.TopicName)
}

func TestListTopicsPagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)

	cl := b.AddClusterInternal("tpc-cluster", "3.6.0")
	for i := range 5 {
		b.AddTopicInternal(cl.ClusterArn, fmt.Sprintf("topic-%02d", i))
	}

	encodedArn := url.PathEscape(cl.ClusterArn)
	path1 := fmt.Sprintf("/v1/clusters/%s/topics?maxResults=3", encodedArn)
	rec1 := doKafkaRequest(t, h, http.MethodGet, path1, nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	topics1 := resp1["topics"].([]any)
	assert.Len(t, topics1, 3)

	nextToken, _ := resp1["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	// Page 2.
	path2 := fmt.Sprintf(
		"/v1/clusters/%s/topics?maxResults=3&nextToken=%s",
		encodedArn,
		url.QueryEscape(nextToken),
	)
	rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	topics2 := resp2["topics"].([]any)
	assert.Len(t, topics2, 2)
	assert.Empty(t, resp2["nextToken"])
}

// ----------------------------------------
// CREATING→ACTIVE state transition
// ----------------------------------------
