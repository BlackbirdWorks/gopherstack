package awsconfig

import (
	"context"
	"encoding/json"

	"github.com/blackbirdworks/gopherstack/pkgs/service"
)

// Operation name constants for delivery channel ops.
const (
	opDeleteDeliveryChannel         = "DeleteDeliveryChannel"
	opDeliverConfigSnapshot         = "DeliverConfigSnapshot"
	opDescribeDeliveryChannelStatus = "DescribeDeliveryChannelStatus"
	opDescribeDeliveryChannels      = "DescribeDeliveryChannels"
	opPutDeliveryChannel            = "PutDeliveryChannel"
)

// deliveryChannelSupportedOps returns the operation names this family handles.
func deliveryChannelSupportedOps() []string {
	return []string{
		opPutDeliveryChannel,
		opDescribeDeliveryChannels,
		opDeleteDeliveryChannel,
		opDeliverConfigSnapshot,
		opDescribeDeliveryChannelStatus,
	}
}

type extractDeliveryChannelNameInput struct {
	DeliveryChannel configNameBody `json:"DeliveryChannel"`
}

func extractDeliveryChannelName(body []byte) string {
	var req extractDeliveryChannelNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}

	return req.DeliveryChannel.Name
}

type extractFirstDeliveryChannelNameInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames"`
}

func extractFirstDeliveryChannelName(body []byte) string {
	var req extractFirstDeliveryChannelNameInput
	if unmarshalErr := json.Unmarshal(body, &req); unmarshalErr != nil {
		return ""
	}
	if len(req.DeliveryChannelNames) > 0 {
		return req.DeliveryChannelNames[0]
	}

	return ""
}

// deliveryChannelBody is the nested JSON body for a delivery channel.
type deliveryChannelBody struct {
	ConfigSnapshotDeliveryProperties *DeliverySnapshotProperties `json:"configSnapshotDeliveryProperties,omitempty"`
	Name                             string                      `json:"name"`
	S3BucketName                     string                      `json:"s3BucketName"`
	S3KeyPrefix                      string                      `json:"s3KeyPrefix,omitempty"`
	SnsTopicARN                      string                      `json:"snsTopicARN"`
}

type handlePutDeliveryChannelInput struct {
	DeliveryChannel deliveryChannelBody `json:"DeliveryChannel"`
}

type putDeliveryChannelOutput struct{}

func (h *Handler) handlePutDeliveryChannel(
	_ context.Context,
	in *handlePutDeliveryChannelInput,
) (*putDeliveryChannelOutput, error) {
	if err := h.Backend.PutDeliveryChannel(
		in.DeliveryChannel.Name,
		in.DeliveryChannel.S3BucketName,
		in.DeliveryChannel.SnsTopicARN,
		in.DeliveryChannel.S3KeyPrefix,
		in.DeliveryChannel.ConfigSnapshotDeliveryProperties,
	); err != nil {
		return nil, err
	}

	return &putDeliveryChannelOutput{}, nil
}

type describeDeliveryChannelsInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames,omitempty"`
}

type describeDeliveryChannelsOutput struct {
	DeliveryChannels []DeliveryChannel `json:"DeliveryChannels"`
}

func (h *Handler) handleDescribeDeliveryChannels(
	_ context.Context,
	in *describeDeliveryChannelsInput,
) (*describeDeliveryChannelsOutput, error) {
	channels := h.Backend.DescribeDeliveryChannels(in.DeliveryChannelNames)

	return &describeDeliveryChannelsOutput{DeliveryChannels: channels}, nil
}

type deleteDeliveryChannelInput struct {
	DeliveryChannelName string `json:"DeliveryChannelName"`
}

type deleteDeliveryChannelOutput struct{}

func (h *Handler) handleDeleteDeliveryChannel(
	_ context.Context,
	in *deleteDeliveryChannelInput,
) (*deleteDeliveryChannelOutput, error) {
	if err := h.Backend.DeleteDeliveryChannel(in.DeliveryChannelName); err != nil {
		return nil, err
	}

	return &deleteDeliveryChannelOutput{}, nil
}

// DeliverConfigSnapshot request/response types and handler.
type deliverConfigSnapshotInput struct {
	DeliveryChannelName string `json:"deliveryChannelName"`
}

func (h *Handler) handleDeliverConfigSnapshot(
	_ context.Context, in *deliverConfigSnapshotInput,
) (*emptyOutput, error) {
	return &emptyOutput{}, h.Backend.DeliverConfigSnapshot(in.DeliveryChannelName)
}

// DescribeDeliveryChannelStatus request/response types and handler.
type describeDeliveryChannelStatusInput struct {
	DeliveryChannelNames []string `json:"DeliveryChannelNames"`
}
type describeDeliveryChannelStatusOutput struct {
	DeliveryChannelsStatus []DeliveryChannelStatus `json:"DeliveryChannelsStatus"`
}

func (h *Handler) handleDescribeDeliveryChannelStatus(
	_ context.Context, in *describeDeliveryChannelStatusInput,
) (*describeDeliveryChannelStatusOutput, error) {
	return &describeDeliveryChannelStatusOutput{
		DeliveryChannelsStatus: h.Backend.DescribeDeliveryChannelStatus(in.DeliveryChannelNames),
	}, nil
}

// buildDeliveryChannelDispatch returns dispatch entries for delivery channel ops.
func (h *Handler) buildDeliveryChannelDispatch() map[string]service.JSONOpFunc {
	return map[string]service.JSONOpFunc{
		opPutDeliveryChannel:            service.WrapOp(h.handlePutDeliveryChannel),
		opDescribeDeliveryChannels:      service.WrapOp(h.handleDescribeDeliveryChannels),
		opDeleteDeliveryChannel:         service.WrapOp(h.handleDeleteDeliveryChannel),
		opDeliverConfigSnapshot:         service.WrapOp(h.handleDeliverConfigSnapshot),
		opDescribeDeliveryChannelStatus: service.WrapOp(h.handleDescribeDeliveryChannelStatus),
	}
}
