package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

type createTopicInput struct {
	ConfigEntries     map[string]string `json:"configEntries,omitempty"`
	TopicName         string            `json:"topicName"`
	ReplicationFactor int32             `json:"replicationFactor"`
	NumPartitions     int32             `json:"numPartitions"`
}

type createTopicOutput struct {
	ConfigEntries     map[string]string `json:"configEntries,omitempty"`
	TopicName         string            `json:"topicName"`
	ReplicationFactor int32             `json:"replicationFactor"`
	NumPartitions     int32             `json:"numPartitions"`
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
		in.NumPartitions,
		in.ConfigEntries,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, createTopicOutput{
		TopicName:         topic.TopicName,
		ReplicationFactor: topic.ReplicationFactor,
		NumPartitions:     topic.NumPartitions,
		ConfigEntries:     topic.ConfigEntries,
	})
}

func (h *Handler) handleDeleteTopic(ctx context.Context, c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	clusterArn, topicName := parts[0], parts[1]

	if err := h.Backend.DeleteTopic(ctx, clusterArn, topicName); err != nil {
		return h.writeBackendError(c, err)
	}

	return c.NoContent(http.StatusOK)
}

type listTopicsOutput struct {
	NextToken string   `json:"nextToken,omitempty"`
	Topics    []*Topic `json:"topics"`
}

type updateTopicInput struct {
	ConfigEntries map[string]string `json:"configEntries,omitempty"`
	NumPartitions int32             `json:"numPartitions"`
}

func (h *Handler) handleDescribeTopic(ctx context.Context, c *echo.Context, resource string) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	topic, err := h.Backend.DescribeTopic(ctx, parts[0], parts[1])
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, topic)
}

func (h *Handler) handleDescribeTopicPartitions(
	ctx context.Context,
	c *echo.Context,
	resource string,
) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return h.writeError(
			c,
			http.StatusBadRequest,
			"BadRequestException",
			"invalid resource: missing topic name",
		)
	}

	topic, err := h.Backend.DescribeTopicPartitions(ctx, parts[0], parts[1])
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, topic)
}

func (h *Handler) handleListTopics(ctx context.Context, c *echo.Context, clusterArn string) error {
	all, err := h.Backend.ListTopics(ctx, clusterArn)
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

	return c.JSON(http.StatusOK, listTopicsOutput{Topics: page, NextToken: nextToken})
}

func (h *Handler) handleUpdateTopic(
	ctx context.Context,
	c *echo.Context,
	resource string,
	body []byte,
) error {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
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

	topic, err := h.Backend.UpdateTopic(ctx, parts[0], parts[1], in.NumPartitions, in.ConfigEntries)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, topic)
}
