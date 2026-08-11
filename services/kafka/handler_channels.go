package kafka

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"

	"github.com/labstack/echo/v5"
)

// splitChannelResource splits the handler's internal "clusterArn|channelArn"
// resource key (see parseClusterResourceV1Channels) into its two parts, the
// same composite-key convention topicKeySeparator already uses for
// "clusterArn|topicName" (splitTopicResource) and
// "configArn|revision" (parseConfigurationResource).
func splitChannelResource(resource string) (string, string, bool) {
	parts := strings.SplitN(resource, topicKeySeparator, topicKeySeparatorParts)
	if len(parts) != topicKeySeparatorParts {
		return "", "", false
	}

	return parts[0], parts[1], true
}

// createChannelInput mirrors CreateChannelInput's JSON body. ChannelArn/
// ClusterArn travel via the URI (see
// awsRestjson1_serializeOpHttpBindingsCreateChannelInput in serializers.go),
// not the body.
type createChannelInput struct {
	EncryptionConfiguration         *ChannelEncryptionConfiguration  `json:"encryptionConfiguration,omitempty"`
	IcebergDestinationConfiguration *IcebergDestinationConfiguration `json:"icebergDestinationConfiguration,omitempty"`
	LoggingInfo                     *ChannelLoggingInfo              `json:"loggingInfo,omitempty"`
	S3DestinationConfiguration      *S3DestinationConfiguration      `json:"s3DestinationConfiguration,omitempty"`
	Tags                            map[string]string                `json:"tags,omitempty"`
	ChannelName                     string                           `json:"channelName"`
	TopicConfigurationList          []TopicConfiguration             `json:"topicConfigurationList"`
}

// channelOperationOutput mirrors the shared response shape of
// CreateChannelOutput / DeleteChannelOutput / UpdateChannelOutput: each
// mutating Channel operation returns only the channel ARN and the
// cluster-operation ARN that tracks the (real API: asynchronous) change.
type channelOperationOutput struct {
	ChannelArn          string `json:"channelArn"`
	ClusterOperationArn string `json:"clusterOperationArn,omitempty"`
}

func (h *Handler) handleCreateChannel(
	ctx context.Context,
	c *echo.Context,
	clusterArn string,
	body []byte,
) error {
	var in createChannelInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	ch, err := h.Backend.CreateChannel(
		ctx,
		clusterArn,
		in.ChannelName,
		in.TopicConfigurationList,
		in.EncryptionConfiguration,
		in.IcebergDestinationConfiguration,
		in.S3DestinationConfiguration,
		in.LoggingInfo,
		in.Tags,
	)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, channelOperationOutput{
		ChannelArn:          ch.ChannelArn,
		ClusterOperationArn: ch.ClusterOperationArn,
	})
}

func (h *Handler) handleDeleteChannel(ctx context.Context, c *echo.Context, resource string) error {
	clusterArn, channelArn, ok := splitChannelResource(resource)
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid resource: missing channel ARN")
	}

	ch, err := h.Backend.DeleteChannel(ctx, clusterArn, channelArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, channelOperationOutput{
		ChannelArn:          ch.ChannelArn,
		ClusterOperationArn: ch.ClusterOperationArn,
	})
}

// describeChannelOutput mirrors DescribeChannelOutput exactly. Built
// explicitly (rather than marshaling *Channel directly) so the internal-only
// ClusterArn field never leaks into the API response, the same pattern
// describeTopicOutputFrom uses for Topic (see the Topic/Channel doc comments
// in models.go).
type describeChannelOutput struct {
	EncryptionConfiguration         *ChannelEncryptionConfiguration  `json:"encryptionConfiguration,omitempty"`
	IcebergDestinationConfiguration *IcebergDestinationConfiguration `json:"icebergDestinationConfiguration,omitempty"`
	LoggingInfo                     *ChannelLoggingInfo              `json:"loggingInfo,omitempty"`
	S3DestinationConfiguration      *S3DestinationConfiguration      `json:"s3DestinationConfiguration,omitempty"`
	StateInfo                       *ChannelStateInfo                `json:"stateInfo,omitempty"`
	Tags                            map[string]string                `json:"tags,omitempty"`
	ClusterOperationArn             string                           `json:"clusterOperationArn,omitempty"`
	ChannelArn                      string                           `json:"channelArn"`
	ChannelName                     string                           `json:"channelName"`
	CreationTime                    string                           `json:"creationTime,omitempty"`
	DestinationType                 string                           `json:"destinationType"`
	Status                          string                           `json:"status"`
	TopicConfigurationList          []TopicConfiguration             `json:"topicConfigurationList"`
}

func describeChannelOutputFrom(ch *Channel) describeChannelOutput {
	return describeChannelOutput{
		ChannelArn:                      ch.ChannelArn,
		ChannelName:                     ch.ChannelName,
		ClusterOperationArn:             ch.ClusterOperationArn,
		CreationTime:                    ch.CreationTime,
		DestinationType:                 ch.DestinationType,
		Status:                          ch.Status,
		EncryptionConfiguration:         ch.EncryptionConfiguration,
		IcebergDestinationConfiguration: ch.IcebergDestinationConfiguration,
		LoggingInfo:                     ch.LoggingInfo,
		S3DestinationConfiguration:      ch.S3DestinationConfiguration,
		StateInfo:                       ch.StateInfo,
		Tags:                            ch.Tags,
		TopicConfigurationList:          ch.TopicConfigurationList,
	}
}

func (h *Handler) handleDescribeChannel(ctx context.Context, c *echo.Context, resource string) error {
	clusterArn, channelArn, ok := splitChannelResource(resource)
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid resource: missing channel ARN")
	}

	ch, err := h.Backend.DescribeChannel(ctx, clusterArn, channelArn)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, describeChannelOutputFrom(ch))
}

// channelInfoOutput mirrors types.ChannelInfo, the ListChannels element
// shape -- a distinct (smaller) shape from Channel/DescribeChannelOutput: no
// destination-configuration/logging/tags/topicConfigurationList detail.
type channelInfoOutput struct {
	ChannelArn          string `json:"channelArn"`
	ChannelName         string `json:"channelName"`
	ClusterOperationArn string `json:"clusterOperationArn,omitempty"`
	CreationTime        string `json:"creationTime,omitempty"`
	DestinationType     string `json:"destinationType"`
	Status              string `json:"status"`
}

type listChannelsOutput struct {
	NextToken string              `json:"nextToken,omitempty"`
	Channels  []channelInfoOutput `json:"channels"`
}

func (h *Handler) handleListChannels(ctx context.Context, c *echo.Context, clusterArn string) error {
	topicNameFilter := c.Request().URL.Query().Get("topicNameFilter")

	all, err := h.Backend.ListChannels(ctx, clusterArn, topicNameFilter)
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

	out := make([]channelInfoOutput, len(page))
	for i, ch := range page {
		out[i] = channelInfoOutput{
			ChannelArn:          ch.ChannelArn,
			ChannelName:         ch.ChannelName,
			ClusterOperationArn: ch.ClusterOperationArn,
			CreationTime:        ch.CreationTime,
			DestinationType:     ch.DestinationType,
			Status:              ch.Status,
		}
	}

	return c.JSON(http.StatusOK, listChannelsOutput{Channels: out, NextToken: nextToken})
}

// updateChannelInput mirrors UpdateChannelInput's JSON body.
type updateChannelInput struct {
	IcebergDestinationUpdate *IcebergDestinationUpdate `json:"icebergDestinationUpdate,omitempty"`
	S3DestinationUpdate      *S3DestinationUpdate      `json:"s3DestinationUpdate,omitempty"`
}

func (h *Handler) handleUpdateChannel(
	ctx context.Context,
	c *echo.Context,
	resource string,
	body []byte,
) error {
	clusterArn, channelArn, ok := splitChannelResource(resource)
	if !ok {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid resource: missing channel ARN")
	}

	var in updateChannelInput
	if err := json.Unmarshal(body, &in); err != nil {
		return h.writeError(c, http.StatusBadRequest, "BadRequestException", "invalid request body: "+err.Error())
	}

	ch, err := h.Backend.UpdateChannel(ctx, clusterArn, channelArn, in.IcebergDestinationUpdate, in.S3DestinationUpdate)
	if err != nil {
		return h.writeBackendError(c, err)
	}

	return c.JSON(http.StatusOK, channelOperationOutput{
		ChannelArn:          ch.ChannelArn,
		ClusterOperationArn: ch.ClusterOperationArn,
	})
}
