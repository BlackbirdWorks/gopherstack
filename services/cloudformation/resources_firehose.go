package cloudformation

import (
	"context"
	"fmt"

	firehosebackend "github.com/blackbirdworks/gopherstack/services/firehose"
)

// ---- Firehose ----

func (rc *ResourceCreator) createFirehoseDeliveryStream(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Firehose == nil {
		return logicalID + "-stub", nil
	}

	name := strProp(props, "DeliveryStreamName", params, physicalIDs)
	if name == "" {
		name = logicalID
	}

	_, err := rc.backends.Firehose.Backend.CreateDeliveryStream(ctx, firehosebackend.CreateDeliveryStreamInput{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("create Firehose delivery stream %s: %w", name, err)
	}

	// Ref returns the delivery stream name per the CFN docs (both the real
	// AWS::KinesisFirehose::DeliveryStream and its AWS::Firehose::DeliveryStream
	// alias); Arn is a GetAtt-only attribute, see getExtraResourceAttribute.
	return name, nil
}

func (rc *ResourceCreator) deleteFirehoseDeliveryStream(ctx context.Context, physicalID string) error {
	if rc.backends.Firehose == nil {
		return nil
	}

	return rc.backends.Firehose.Backend.DeleteDeliveryStream(ctx, physicalID)
}
