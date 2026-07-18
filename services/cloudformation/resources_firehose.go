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

	stream, err := rc.backends.Firehose.Backend.CreateDeliveryStream(ctx, firehosebackend.CreateDeliveryStreamInput{
		Name: name,
	})
	if err != nil {
		return "", fmt.Errorf("create Firehose delivery stream %s: %w", name, err)
	}

	return stream.ARN, nil
}

func (rc *ResourceCreator) deleteFirehoseDeliveryStream(ctx context.Context, arn string) error {
	if rc.backends.Firehose == nil {
		return nil
	}

	// Extract stream name from ARN: arn:aws:firehose:{region}:{account}:deliverystream/{name}
	name := resourceNameFromARN(arn)

	return rc.backends.Firehose.Backend.DeleteDeliveryStream(ctx, name)
}
