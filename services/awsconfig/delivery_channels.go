package awsconfig

import (
	"fmt"
	"slices"
	"strings"

	"github.com/aws/aws-sdk-go-v2/aws/arn"
)

// PutDeliveryChannel creates or updates a delivery channel. An empty/blank name
// errors InvalidDeliveryChannelNameException, matching real AWS Config's declared
// error model (verified against aws-sdk-go-v2/service/configservice's
// PutDeliveryChannel deserializer).
func (b *InMemoryBackend) PutDeliveryChannel(
	name, s3Bucket, snsArn, s3KeyPrefix string,
	props *DeliverySnapshotProperties,
) error {
	return b.PutDeliveryChannelConfig(&DeliveryChannel{
		Name:                             name,
		S3Bucket:                         s3Bucket,
		SNSArn:                           snsArn,
		S3KeyPrefix:                      s3KeyPrefix,
		ConfigSnapshotDeliveryProperties: props,
	})
}

// PutDeliveryChannelConfig is PutDeliveryChannel taking the full channel, including S3KmsKeyArn.
func (b *InMemoryBackend) PutDeliveryChannelConfig(ch *DeliveryChannel) error {
	name, s3Bucket, snsArn, s3KmsKeyArn := ch.Name, ch.S3Bucket, ch.SNSArn, ch.S3KmsKeyArn

	if strings.TrimSpace(name) == "" {
		return fmt.Errorf("%w: DeliveryChannel name is required", ErrInvalidDeliveryChannelName)
	}

	if s3Bucket == "" {
		// PutDeliveryChannel's declared set has no code for a missing s3BucketName:
		// NoSuchBucketException is "the specified bucket does not exist", not "required"
		// (configservice@v1.68.4 types/errors.go), and ValidationException/
		// InvalidParameterValueException aren't declared for this op either.
		return fmt.Errorf("%w: DeliveryChannel s3BucketName is required", ErrValidation)
	}

	if snsArn != "" && !arn.IsARN(snsArn) {
		return fmt.Errorf("%w: %q is not a valid ARN", ErrInvalidSNSTopicARN, snsArn)
	}

	if parsed, err := arn.Parse(s3KmsKeyArn); s3KmsKeyArn != "" && (err != nil || parsed.Service != "kms") {
		return fmt.Errorf("%w: %q is not a valid KMS ARN", ErrInvalidS3KmsKeyArn, s3KmsKeyArn)
	}

	b.mu.Lock("PutDeliveryChannel")
	defer b.mu.Unlock()

	cp := ch.clone()
	b.channels.Put(&cp)

	return nil
}

// DescribeDeliveryChannels returns delivery channels filtered by the provided name list.
// An empty/nil names list returns all channels sorted by name.
func (b *InMemoryBackend) DescribeDeliveryChannels(names []string) []DeliveryChannel {
	b.mu.RLock("DescribeDeliveryChannels")
	defer b.mu.RUnlock()

	out := make([]DeliveryChannel, 0, b.channels.Len())

	if len(names) == 0 {
		for _, c := range b.channels.All() {
			out = append(out, c.clone())
		}
	} else {
		for _, n := range names {
			if c, ok := b.channels.Get(n); ok {
				out = append(out, c.clone())
			}
		}
	}

	slices.SortFunc(out, func(a, b DeliveryChannel) int {
		if a.Name < b.Name {
			return -1
		}

		if a.Name > b.Name {
			return 1
		}

		return 0
	})

	return out
}

// DeleteDeliveryChannel removes a delivery channel by name. Real AWS:
// "Before you can delete the delivery channel, you must stop the customer
// managed configuration recorder".
func (b *InMemoryBackend) DeleteDeliveryChannel(name string) error {
	if name == "" {
		// Declared set is LastDeliveryChannelDeleteFailedException/
		// NoSuchDeliveryChannelException only -- no validation-shaped code fits an
		// empty name (configservice@v1.68.4 deserializers.go).
		return fmt.Errorf("%w: DeliveryChannelName is required", ErrValidation)
	}

	b.mu.Lock("DeleteDeliveryChannel")
	defer b.mu.Unlock()

	if !b.channels.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchDeliveryChannel, name)
	}

	for _, r := range b.recorders.All() {
		if r.Status == recorderStatusActive {
			return fmt.Errorf(
				"%w: configuration recorder %s is still recording",
				ErrLastDeliveryChannelDeleteFailed, r.Name,
			)
		}
	}

	b.channels.Delete(name)

	return nil
}

// DeliverConfigSnapshot and DescribeDeliveryChannelStatus live in
// delivery_status.go, alongside the delivery-state tracking they share
// (gopherstack-ru0y).

func (c *DeliveryChannel) clone() DeliveryChannel {
	cp := *c
	if c.ConfigSnapshotDeliveryProperties != nil {
		props := *c.ConfigSnapshotDeliveryProperties
		cp.ConfigSnapshotDeliveryProperties = &props
	}

	return cp
}
