// Package dynamodb implements the AWS DynamoDB mock service.
// handler_kinesis_streaming.go implements the wire-JSON handlers for the
// Kinesis streaming destination family. Routing (dispatchExtraOps) stays in
// handler.go; these are the leaf implementations it calls into. Backend
// logic lives in kinesis_streaming.go.
package dynamodb

import (
	"context"
	"encoding/json"

	sdkDDB "github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/blackbirdworks/gopherstack/pkgs/ptrconv"
)

type describeKinesisInput struct {
	TableName string `json:"TableName"`
}

type kinesisDestinationWire struct {
	StreamArn                            string `json:"StreamArn,omitempty"`
	DestinationStatus                    string `json:"DestinationStatus,omitempty"`
	ApproximateCreationDateTimePrecision string `json:"ApproximateCreationDateTimePrecision,omitempty"`
}

type describeKinesisOutput struct {
	TableName                     string                   `json:"TableName,omitempty"`
	KinesisDataStreamDestinations []kinesisDestinationWire `json:"KinesisDataStreamDestinations"`
}

type disableKinesisInput struct {
	TableName string `json:"TableName"`
	StreamArn string `json:"StreamArn"`
}

type disableKinesisOutput struct {
	StreamingConfig   *enableKinesisStreamingConfigWire `json:"EnableKinesisStreamingConfiguration,omitempty"`
	TableName         string                            `json:"TableName,omitempty"`
	StreamArn         string                            `json:"StreamArn,omitempty"`
	DestinationStatus string                            `json:"DestinationStatus,omitempty"`
}

type enableKinesisStreamingConfigWire struct {
	ApproximateCreationDateTimePrecision string `json:"ApproximateCreationDateTimePrecision,omitempty"`
}

type enableKinesisInput struct {
	StreamingConfig *enableKinesisStreamingConfigWire `json:"EnableKinesisStreamingConfiguration,omitempty"`
	TableName       string                            `json:"TableName"`
	StreamArn       string                            `json:"StreamArn"`
}

type enableKinesisOutput struct {
	StreamingConfig   *enableKinesisStreamingConfigWire `json:"EnableKinesisStreamingConfiguration,omitempty"`
	TableName         string                            `json:"TableName,omitempty"`
	StreamArn         string                            `json:"StreamArn,omitempty"`
	DestinationStatus string                            `json:"DestinationStatus,omitempty"`
}

func (h *DynamoDBHandler) handleDescribeKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req describeKinesisInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DescribeKinesisStreamingDestination(
		ctx,
		&sdkDDB.DescribeKinesisStreamingDestinationInput{TableName: &req.TableName},
	)
	if err != nil {
		return nil, err
	}

	destinations := make([]kinesisDestinationWire, 0, len(out.KinesisDataStreamDestinations))
	for _, d := range out.KinesisDataStreamDestinations {
		destinations = append(destinations, kinesisDestinationWire{
			StreamArn:                            ptrconv.String(d.StreamArn),
			DestinationStatus:                    string(d.DestinationStatus),
			ApproximateCreationDateTimePrecision: string(d.ApproximateCreationDateTimePrecision),
		})
	}

	return &describeKinesisOutput{
		TableName:                     ptrconv.String(out.TableName),
		KinesisDataStreamDestinations: destinations,
	}, nil
}

func (h *DynamoDBHandler) handleDisableKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req disableKinesisInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.DisableKinesisStreamingDestination(
		ctx,
		&sdkDDB.DisableKinesisStreamingDestinationInput{
			TableName: &req.TableName,
			StreamArn: &req.StreamArn,
		},
	)
	if err != nil {
		return nil, err
	}

	return &disableKinesisOutput{
		TableName:         ptrconv.String(out.TableName),
		StreamArn:         ptrconv.String(out.StreamArn),
		DestinationStatus: string(out.DestinationStatus),
		StreamingConfig:   fromSDKEnableKinesisStreamingConfig(out.EnableKinesisStreamingConfiguration),
	}, nil
}

// toSDKEnableKinesisStreamingConfig converts the wire precision config to the
// SDK type, or nil if w is nil (an omitted request member).
func toSDKEnableKinesisStreamingConfig(
	w *enableKinesisStreamingConfigWire,
) *types.EnableKinesisStreamingConfiguration {
	if w == nil {
		return nil
	}

	return &types.EnableKinesisStreamingConfiguration{
		ApproximateCreationDateTimePrecision: types.ApproximateCreationDateTimePrecision(
			w.ApproximateCreationDateTimePrecision,
		),
	}
}

// fromSDKEnableKinesisStreamingConfig converts the SDK precision config to the
// wire type, or nil if c is nil.
func fromSDKEnableKinesisStreamingConfig(
	c *types.EnableKinesisStreamingConfiguration,
) *enableKinesisStreamingConfigWire {
	if c == nil {
		return nil
	}

	return &enableKinesisStreamingConfigWire{
		ApproximateCreationDateTimePrecision: string(c.ApproximateCreationDateTimePrecision),
	}
}

func (h *DynamoDBHandler) handleEnableKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req enableKinesisInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.EnableKinesisStreamingDestination(ctx, &sdkDDB.EnableKinesisStreamingDestinationInput{
		TableName:                           &req.TableName,
		StreamArn:                           &req.StreamArn,
		EnableKinesisStreamingConfiguration: toSDKEnableKinesisStreamingConfig(req.StreamingConfig),
	})
	if err != nil {
		return nil, err
	}

	return &enableKinesisOutput{
		TableName:         ptrconv.String(out.TableName),
		StreamArn:         ptrconv.String(out.StreamArn),
		DestinationStatus: string(out.DestinationStatus),
		StreamingConfig:   fromSDKEnableKinesisStreamingConfig(out.EnableKinesisStreamingConfiguration),
	}, nil
}

// --- UpdateKinesisStreamingDestination handler ---

type updateKinesisStreamingConfigWire struct {
	ApproximateCreationDateTimePrecision string `json:"ApproximateCreationDateTimePrecision,omitempty"`
}

type updateKinesisStreamingDestinationInput struct {
	StreamingConfig *updateKinesisStreamingConfigWire `json:"UpdateKinesisStreamingConfiguration,omitempty"`
	TableName       string                            `json:"TableName"`
	StreamArn       string                            `json:"StreamArn"`
}

type updateKinesisStreamingDestinationOutput struct {
	StreamingConfig   *updateKinesisStreamingConfigWire `json:"UpdateKinesisStreamingConfiguration,omitempty"`
	TableName         string                            `json:"TableName"`
	StreamArn         string                            `json:"StreamArn"`
	DestinationStatus string                            `json:"DestinationStatus"`
}

// toSDKUpdateKinesisStreamingConfig converts the wire precision config to the
// SDK type, or nil if w is nil (an omitted request member).
func toSDKUpdateKinesisStreamingConfig(
	w *updateKinesisStreamingConfigWire,
) *types.UpdateKinesisStreamingConfiguration {
	if w == nil {
		return nil
	}

	return &types.UpdateKinesisStreamingConfiguration{
		ApproximateCreationDateTimePrecision: types.ApproximateCreationDateTimePrecision(
			w.ApproximateCreationDateTimePrecision,
		),
	}
}

// fromSDKUpdateKinesisStreamingConfig converts the SDK precision config to the
// wire type, or nil if c is nil.
func fromSDKUpdateKinesisStreamingConfig(
	c *types.UpdateKinesisStreamingConfiguration,
) *updateKinesisStreamingConfigWire {
	if c == nil {
		return nil
	}

	return &updateKinesisStreamingConfigWire{
		ApproximateCreationDateTimePrecision: string(c.ApproximateCreationDateTimePrecision),
	}
}

func (h *DynamoDBHandler) handleUpdateKinesisStreamingDestination(
	ctx context.Context,
	body []byte,
) (any, error) {
	var req updateKinesisStreamingDestinationInput
	if err := json.Unmarshal(body, &req); err != nil {
		return nil, err
	}

	out, err := h.Backend.UpdateKinesisStreamingDestination(ctx, &sdkDDB.UpdateKinesisStreamingDestinationInput{
		TableName:                           &req.TableName,
		StreamArn:                           &req.StreamArn,
		UpdateKinesisStreamingConfiguration: toSDKUpdateKinesisStreamingConfig(req.StreamingConfig),
	})
	if err != nil {
		return nil, err
	}

	return &updateKinesisStreamingDestinationOutput{
		TableName:         ptrconv.String(out.TableName),
		StreamArn:         ptrconv.String(out.StreamArn),
		DestinationStatus: string(out.DestinationStatus),
		StreamingConfig:   fromSDKUpdateKinesisStreamingConfig(out.UpdateKinesisStreamingConfiguration),
	}, nil
}
