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

// s3ChannelCreateBody returns a CreateChannel JSON body wired for a valid S3
// destination, driven as raw map[string]any (not our own request struct) so
// the test round-trips through real JSON marshal/unmarshal the same way a
// real aws-sdk-go-v2 client request body would.
func s3ChannelCreateBody(channelName, topicArn string) map[string]any {
	return map[string]any{
		"channelName": channelName,
		"topicConfigurationList": []map[string]any{
			{
				"recordConverter": map[string]any{"valueConverter": "JSON"},
				"topicArn":        topicArn,
			},
		},
		"s3DestinationConfiguration": map[string]any{
			"deadLetterQueueS3":       map[string]any{"bucketArn": "arn:aws:s3:::dlq-bucket"},
			"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/channel-role",
			"storage": map[string]any{
				"bucketArn":       "arn:aws:s3:::dest-bucket",
				"compressionType": "GZIP",
				"storageClass":    "STANDARD",
			},
		},
	}
}

func TestKafka_ChannelLifecycle_HTTP(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "channel-cluster")
	encodedCluster := url.PathEscape(clusterArn)
	topicArn := clusterArn + "/topic/my-topic"

	// CreateChannel via the real POST /v1/clusters/{ClusterArn}/channels wire path.
	createRec := doKafkaRequest(
		t, h, http.MethodPost, "/v1/clusters/"+encodedCluster+"/channels",
		s3ChannelCreateBody("my-channel", topicArn),
	)
	require.Equal(t, http.StatusOK, createRec.Code, "create channel: %s", createRec.Body.String())

	createResp := decodeJSONResponse(t, createRec)
	channelArn, _ := createResp["channelArn"].(string)
	require.NotEmpty(t, channelArn)
	assert.NotEmpty(t, createResp["clusterOperationArn"])

	encodedChannel := url.PathEscape(channelArn)

	// DescribeChannel via GET .../channels/{ChannelArn}.
	describeRec := doKafkaRequest(
		t, h, http.MethodGet, "/v1/clusters/"+encodedCluster+"/channels/"+encodedChannel, nil,
	)
	require.Equal(t, http.StatusOK, describeRec.Code)
	describeResp := decodeJSONResponse(t, describeRec)
	assert.Equal(t, "my-channel", describeResp["channelName"])
	assert.Equal(t, "S3", describeResp["destinationType"])
	assert.Equal(t, "ACTIVE", describeResp["status"])
	// clusterOperationArn is only present on the mutating response, not on a
	// completed channel's Describe (real MSK: "Returned only while the
	// channel is in CREATING, UPDATING, or DELETING").
	assert.Empty(t, describeResp["clusterOperationArn"])
	assert.Nil(t, describeResp["clusterArn"], "clusterArn must not leak into the wire response")

	// ListChannels via GET .../channels.
	listRec := doKafkaRequest(t, h, http.MethodGet, "/v1/clusters/"+encodedCluster+"/channels", nil)
	require.Equal(t, http.StatusOK, listRec.Code)
	listResp := decodeJSONResponse(t, listRec)
	channels, _ := listResp["channels"].([]any)
	require.Len(t, channels, 1)
	first, _ := channels[0].(map[string]any)
	assert.Equal(t, channelArn, first["channelArn"])

	// UpdateChannel via PUT .../channels/{ChannelArn}.
	updateRec := doKafkaRequest(
		t, h, http.MethodPut, "/v1/clusters/"+encodedCluster+"/channels/"+encodedChannel,
		map[string]any{"s3DestinationUpdate": map[string]any{"dataFreshnessInSeconds": 600}},
	)
	require.Equal(t, http.StatusOK, updateRec.Code, "update channel: %s", updateRec.Body.String())
	updateResp := decodeJSONResponse(t, updateRec)
	assert.Equal(t, channelArn, updateResp["channelArn"])
	assert.NotEmpty(t, updateResp["clusterOperationArn"])

	describeAfterUpdateRec := doKafkaRequest(
		t, h, http.MethodGet, "/v1/clusters/"+encodedCluster+"/channels/"+encodedChannel, nil,
	)
	require.Equal(t, http.StatusOK, describeAfterUpdateRec.Code)
	describeAfterUpdateResp := decodeJSONResponse(t, describeAfterUpdateRec)
	s3Dest, _ := describeAfterUpdateResp["s3DestinationConfiguration"].(map[string]any)
	require.NotNil(t, s3Dest)
	assert.InDelta(t, float64(600), s3Dest["dataFreshnessInSeconds"], 0)

	// DeleteChannel via DELETE .../channels/{ChannelArn}.
	deleteRec := doKafkaRequest(
		t, h, http.MethodDelete, "/v1/clusters/"+encodedCluster+"/channels/"+encodedChannel, nil,
	)
	require.Equal(t, http.StatusOK, deleteRec.Code)
	deleteResp := decodeJSONResponse(t, deleteRec)
	assert.Equal(t, channelArn, deleteResp["channelArn"])

	// DescribeChannel after delete -> 404 NotFoundException.
	describeAfterDeleteRec := doKafkaRequest(
		t, h, http.MethodGet, "/v1/clusters/"+encodedCluster+"/channels/"+encodedChannel, nil,
	)
	assert.Equal(t, http.StatusNotFound, describeAfterDeleteRec.Code)
	notFoundResp := decodeJSONResponse(t, describeAfterDeleteRec)
	assert.Equal(t, "NotFoundException", notFoundResp["errorCode"])
}

func TestKafka_CreateChannel_ValidationErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		body       map[string]any
		name       string
		wantStatus int
	}{
		{
			name:       "invalid_json_body",
			body:       nil,
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "missing_channel_name",
			body: map[string]any{
				"topicConfigurationList": []map[string]any{
					{
						"recordConverter": map[string]any{"valueConverter": "JSON"},
						"topicArn":        "arn:aws:kafka:us-east-1:000000000000:topic/c/u/t",
					},
				},
				"s3DestinationConfiguration": map[string]any{
					"deadLetterQueueS3":       map[string]any{"bucketArn": "arn:aws:s3:::dlq"},
					"serviceExecutionRoleArn": "arn:aws:iam::000000000000:role/r",
					"storage": map[string]any{
						"bucketArn":       "arn:aws:s3:::dest",
						"compressionType": "GZIP",
						"storageClass":    "STANDARD",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
		{
			name: "neither_destination",
			body: map[string]any{
				"channelName": "my-channel",
				"topicConfigurationList": []map[string]any{
					{
						"recordConverter": map[string]any{"valueConverter": "JSON"},
						"topicArn":        "arn:aws:kafka:us-east-1:000000000000:topic/c/u/t",
					},
				},
			},
			wantStatus: http.StatusBadRequest,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			h := newTestHandler(t)
			clusterArn := createTestCluster(t, h, "channel-validation-cluster")
			encodedCluster := url.PathEscape(clusterArn)
			path := "/v1/clusters/" + encodedCluster + "/channels"

			var rec *httptest.ResponseRecorder
			if tt.body == nil {
				rec = doRawKafkaPost(t, h, path, []byte("not-json"))
			} else {
				rec = doKafkaRequest(t, h, http.MethodPost, path, tt.body)
			}

			assert.Equal(t, tt.wantStatus, rec.Code)
		})
	}
}

// doRawKafkaPost issues a POST with a raw (non-JSON-marshaled) body, for
// exercising the invalid-JSON-body error path.
func doRawKafkaPost(t *testing.T, h *kafka.Handler, path string, rawBody []byte) *httptest.ResponseRecorder {
	t.Helper()

	e := echo.New()
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(rawBody))
	req.Header.Set("Content-Type", "application/json")
	rec := httptest.NewRecorder()
	c := e.NewContext(req, rec)

	err := h.Handler()(c)
	require.NoError(t, err)

	return rec
}

func TestKafka_DescribeChannel_InvalidResource(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "channel-invalid-resource-cluster")
	encodedCluster := url.PathEscape(clusterArn)

	// GET on the bare /channels root with no trailing channel ARN routes to
	// ListChannels, not DescribeChannel, so exercise the composite-resource
	// guard the other way: a PUT (UpdateChannel) to that same bare root
	// carries no channel ARN to split and must fail routing entirely (404,
	// not 400), since parseClusterResourceV1Channels' /channels branch only
	// recognizes POST/GET on the bare root.
	rec := doKafkaRequest(t, h, http.MethodPut, "/v1/clusters/"+encodedCluster+"/channels", nil)
	assert.Equal(t, http.StatusNotFound, rec.Code)
}

func TestKafka_ListChannels_TopicNameFilter(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	clusterArn := createTestCluster(t, h, "channel-filter-cluster")
	encodedCluster := url.PathEscape(clusterArn)
	matchingTopicArn := clusterArn + "/topic/wanted-topic"
	otherTopicArn := clusterArn + "/topic/other-topic"

	createRec := doKafkaRequest(
		t, h, http.MethodPost, "/v1/clusters/"+encodedCluster+"/channels",
		s3ChannelCreateBody("chan-a", matchingTopicArn),
	)
	require.Equal(t, http.StatusOK, createRec.Code)

	createRec2 := doKafkaRequest(
		t, h, http.MethodPost, "/v1/clusters/"+encodedCluster+"/channels",
		s3ChannelCreateBody("chan-b", otherTopicArn),
	)
	require.Equal(t, http.StatusOK, createRec2.Code)

	listRec := doKafkaRequest(
		t, h, http.MethodGet,
		"/v1/clusters/"+encodedCluster+"/channels?topicNameFilter=wanted-topic",
		nil,
	)
	require.Equal(t, http.StatusOK, listRec.Code)

	listResp := decodeJSONResponse(t, listRec)
	channels, _ := listResp["channels"].([]any)
	require.Len(t, channels, 1)
	first, _ := channels[0].(map[string]any)
	assert.Equal(t, "chan-a", first["channelName"])
}

func TestKafka_ListChannelsPagination(t *testing.T) {
	t.Parallel()

	h, b := newTestHandlerWithBackend(t)
	clusterArn := createTestCluster(t, h, "channel-page-cluster")
	encodedCluster := url.PathEscape(clusterArn)

	for i := range 5 {
		s3Dest, topics := s3ChannelFixtures()
		_, err := b.CreateChannel(
			t.Context(), clusterArn, fmt.Sprintf("chan-%02d", i), topics, nil, nil, s3Dest, nil, nil,
		)
		require.NoError(t, err)
	}

	path1 := fmt.Sprintf("/v1/clusters/%s/channels?maxResults=3", encodedCluster)
	rec1 := doKafkaRequest(t, h, http.MethodGet, path1, nil)
	require.Equal(t, http.StatusOK, rec1.Code)

	var resp1 map[string]any
	require.NoError(t, json.Unmarshal(rec1.Body.Bytes(), &resp1))
	channels1, _ := resp1["channels"].([]any)
	assert.Len(t, channels1, 3)

	nextToken, _ := resp1["nextToken"].(string)
	require.NotEmpty(t, nextToken)

	path2 := fmt.Sprintf(
		"/v1/clusters/%s/channels?maxResults=3&nextToken=%s", encodedCluster, url.QueryEscape(nextToken),
	)
	rec2 := doKafkaRequest(t, h, http.MethodGet, path2, nil)
	require.Equal(t, http.StatusOK, rec2.Code)

	var resp2 map[string]any
	require.NoError(t, json.Unmarshal(rec2.Body.Bytes(), &resp2))
	channels2, _ := resp2["channels"].([]any)
	assert.Len(t, channels2, 2)
	assert.Empty(t, resp2["nextToken"])
}

func TestKafka_GetSupportedOperations_IncludesChannels(t *testing.T) {
	t.Parallel()

	h := newTestHandler(t)
	ops := h.GetSupportedOperations()

	for _, op := range []string{"CreateChannel", "DeleteChannel", "DescribeChannel", "ListChannels", "UpdateChannel"} {
		assert.Contains(t, ops, op)
	}
}
