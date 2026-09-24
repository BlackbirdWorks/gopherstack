package cloudformation

import (
	"context"
	"errors"
	"fmt"

	kinesisbackend "github.com/blackbirdworks/gopherstack/services/kinesis"
)

const resTypeKinesisStreamConsumer = "AWS::Kinesis::StreamConsumer"

// resTypeFirehoseDeliveryStream is the real CFN type name (the CFN spec has no
// "AWS::Firehose::DeliveryStream"); resTypeFirehoseDeliveryStreamAlias is kept
// working since existing tests/templates in this repo use it.
const resTypeFirehoseDeliveryStream = "AWS::KinesisFirehose::DeliveryStream"

const resTypeFirehoseDeliveryStreamAlias = "AWS::Firehose::DeliveryStream"

// deleteKinesisFamilyResource handles AWS::Kinesis::Stream/StreamConsumer
// deletions (split out of deleteDataPlatformResource to keep its cyclomatic
// complexity down).
func (rc *ResourceCreator) deleteKinesisFamilyResource(
	ctx context.Context, resourceType, physicalID string,
) (bool, error) {
	switch resourceType {
	case "AWS::Kinesis::Stream":
		return true, rc.deleteKinesisStream(ctx, physicalID)
	case resTypeKinesisStreamConsumer:
		return true, rc.deleteKinesisStreamConsumer(ctx, physicalID)
	default:
		return false, nil
	}
}

// ---- Kinesis StreamConsumer ----

func (rc *ResourceCreator) createKinesisStreamConsumer(
	ctx context.Context,
	logicalID string,
	props map[string]any,
	params, physicalIDs map[string]string,
) (string, error) {
	if rc.backends.Kinesis == nil {
		return logicalID + "-stub", nil
	}

	streamARN := strProp(props, "StreamARN", params, physicalIDs)
	consumerName := strProp(props, "ConsumerName", params, physicalIDs)
	if consumerName == "" {
		consumerName = logicalID
	}

	out, err := rc.backends.Kinesis.Backend.RegisterStreamConsumer(ctx, &kinesisbackend.RegisterStreamConsumerInput{
		StreamARN:    streamARN,
		ConsumerName: consumerName,
		Tags:         tagListProp(props, params, physicalIDs),
	})
	if err != nil {
		return "", fmt.Errorf("register Kinesis stream consumer %s: %w", consumerName, err)
	}

	physicalIDs[logicalID+"/ConsumerCreationTimestamp"] = out.Consumer.ConsumerCreationTimestamp.UTC().Format(
		"2006-01-02T15:04:05Z07:00",
	)

	return out.Consumer.ConsumerARN, nil
}

func (rc *ResourceCreator) deleteKinesisStreamConsumer(ctx context.Context, physicalID string) error {
	if rc.backends.Kinesis == nil {
		return nil
	}

	err := rc.backends.Kinesis.Backend.DeregisterStreamConsumer(
		ctx, &kinesisbackend.DeregisterStreamConsumerInput{ConsumerARN: physicalID},
	)
	if errors.Is(err, kinesisbackend.ErrConsumerNotFound) || errors.Is(err, kinesisbackend.ErrStreamNotFound) {
		return nil
	}

	return err
}
