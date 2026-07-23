package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// createTopicInput mirrors aws-sdk-go-v2/service/kafka's CreateTopicInput
// wire body: partitionCount/replicationFactor/topicName (required) plus an
// optional opaque Base64 "configs" string.
type createTopicInput struct {
	TopicName         string `json:"topicName"`
	Configs           string `json:"configs,omitempty"`
	ReplicationFactor int32  `json:"replicationFactor"`
	PartitionCount    int32  `json:"partitionCount"`
}

// createTopicOutput mirrors CreateTopicOutput: status/topicArn/topicName only
// (no partitionCount/configs echoed back).
type createTopicOutput struct {
	Status    string `json:"status"`
	TopicArn  string `json:"topicArn"`
	TopicName string `json:"topicName"`
}

func (h *Handler) handleCreateTopic(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in createTopicInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	topic, err := h.Backend.CreateTopic(ctx,
		clusterArn,
		in.TopicName,
		in.ReplicationFactor,
		in.PartitionCount,
		in.Configs,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createTopicOutput{
		Status:    topic.Status,
		TopicArn:  topic.TopicArn,
		TopicName: topic.TopicName,
	})
}

func (h *Handler) handleDeleteTopic(ctx context.Context, c *echo.Context, resource string) error {
	clusterArn, topicName, ok := splitTopicResource(resource)
	if !ok {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	if err := h.Backend.DeleteTopic(ctx, clusterArn, topicName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

// splitTopicResource splits the handler's internal "clusterArn|topicName"
// resource key (see topicKeySeparator) into its two parts.
func splitTopicResource(resource string) (string, string, bool) {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return "", "", false
	}

	return parts[0], parts[1], true
}

// topicInfoOutput mirrors types.TopicInfo, the ListTopics element shape,
// which is a distinct (smaller) shape from Topic/DescribeTopicOutput.
type topicInfoOutput struct {
	TopicArn              string `json:"topicArn"`
	TopicName             string `json:"topicName"`
	PartitionCount        int32  `json:"partitionCount"`
	ReplicationFactor     int32  `json:"replicationFactor"`
	OutOfSyncReplicaCount int32  `json:"outOfSyncReplicaCount"`
}

type listTopicsOutput struct {
	NextToken string            `json:"nextToken,omitempty"`
	Topics    []topicInfoOutput `json:"topics"`
}

// updateTopicInput mirrors UpdateTopicInput: an optional new partitionCount
// and/or configs.
type updateTopicInput struct {
	Configs        string `json:"configs,omitempty"`
	PartitionCount int32  `json:"partitionCount"`
}

// updateTopicOutput mirrors UpdateTopicOutput: status/topicArn/topicName only.
type updateTopicOutput struct {
	Status    string `json:"status"`
	TopicArn  string `json:"topicArn"`
	TopicName string `json:"topicName"`
}

// describeTopicOutput mirrors DescribeTopicOutput exactly: configs/
// partitionCount/replicationFactor/status/topicArn/topicName. Built
// explicitly (rather than marshaling *Topic directly) so the internal-only
// ClusterArn field -- load-bearing for persistence, see the Topic doc comment
// in models.go -- never leaks into the API response.
type describeTopicOutput struct {
	Configs           string `json:"configs,omitempty"`
	Status            string `json:"status"`
	TopicArn          string `json:"topicArn"`
	TopicName         string `json:"topicName"`
	PartitionCount    int32  `json:"partitionCount"`
	ReplicationFactor int32  `json:"replicationFactor"`
}

func describeTopicOutputFrom(t *Topic) describeTopicOutput {
	return describeTopicOutput{
		Configs:           t.Configs,
		Status:            t.Status,
		TopicArn:          t.TopicArn,
		TopicName:         t.TopicName,
		PartitionCount:    t.PartitionCount,
		ReplicationFactor: t.ReplicationFactor,
	}
}

func (h *Handler) handleDescribeTopic(ctx context.Context, c *echo.Context, resource string) error {
	clusterArn, topicName, ok := splitTopicResource(resource)
	if !ok {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	topic, err := h.Backend.DescribeTopic(ctx, clusterArn, topicName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeTopicOutputFrom(topic))
}

// topicPartitionInfoOutput mirrors types.TopicPartitionInfo.
type topicPartitionInfoOutput struct {
	Isr       []int32 `json:"isr"`
	Replicas  []int32 `json:"replicas"`
	Partition int32   `json:"partition"`
	Leader    int32   `json:"leader"`
}

type describeTopicPartitionsOutput struct {
	NextToken  string                     `json:"nextToken,omitempty"`
	Partitions []topicPartitionInfoOutput `json:"partitions"`
}

func (h *Handler) handleDescribeTopicPartitions(
	ctx context.Context,
	c *echo.Context,
	resource string,
) error {
	clusterArn, topicName, ok := splitTopicResource(resource)
	if !ok {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	all, err := h.Backend.DescribeTopicPartitions(ctx, clusterArn, topicName)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	token := c.Request().URL.Query().Get("nextToken")
	offset := decodeKafkaPageToken(token)
	offset = min(offset, len(all))

	page := all[offset:]
	pageSize := kafkaPageSize(c)

	var nextToken string
	if len(page) > pageSize {
		page = page[:pageSize]
		nextToken = encodeKafkaPageToken(offset + pageSize)
	}

	out := make([]topicPartitionInfoOutput, len(page))
	for i, p := range page {
		out[i] = topicPartitionInfoOutput{
			Partition: p.Partition,
			Leader:    p.Leader,
			Replicas:  p.Replicas,
			Isr:       p.Isr,
		}
	}

	return c.JSON(http.StatusOK, describeTopicPartitionsOutput{Partitions: out, NextToken: nextToken})
}

func (h *Handler) handleListTopics(ctx context.Context, c *echo.Context, clusterArn string) error {
	nameFilter := c.Request().URL.Query().Get("topicNameFilter")

	all, err := h.Backend.ListTopics(ctx, clusterArn, nameFilter)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	token := c.Request().URL.Query().Get("nextToken")
	offset := decodeKafkaPageToken(token)

	offset = min(offset, len(all))

	page := all[offset:]
	pageSize := kafkaPageSize(c)

	var nextToken string

	if len(page) > pageSize {
		page = page[:pageSize]
		nextToken = encodeKafkaPageToken(offset + pageSize)
	}

	out := make([]topicInfoOutput, len(page))
	for i, t := range page {
		out[i] = topicInfoOutput{
			TopicArn:          t.TopicArn,
			TopicName:         t.TopicName,
			PartitionCount:    t.PartitionCount,
			ReplicationFactor: t.ReplicationFactor,
		}
	}

	return c.JSON(http.StatusOK, listTopicsOutput{Topics: out, NextToken: nextToken})
}

func (h *Handler) handleUpdateTopic(
	ctx context.Context,
	c *echo.Context,
	resource string,
	body []byte,
) error {
	clusterArn, topicName, ok := splitTopicResource(resource)
	if !ok {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	var in updateTopicInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid request body: "+err.Error(),
		)
	}

	topic, err := h.Backend.UpdateTopic(ctx, clusterArn, topicName, in.PartitionCount, in.Configs)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, updateTopicOutput{
		Status:    topic.Status,
		TopicArn:  topic.TopicArn,
		TopicName: topic.TopicName,
	})
}
