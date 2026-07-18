package awsconfig

import (
	"fmt"
	"slices"
)

// PutDeliveryChannel creates or updates a delivery channel.
func (b *InMemoryBackend) PutDeliveryChannel(
	name, s3Bucket, snsArn, s3KeyPrefix string,
	props *DeliverySnapshotProperties,
) error {
	if name == "" {
		return fmt.Errorf("%w: DeliveryChannel name is required", ErrValidation)
	}

	if s3Bucket == "" {
		return fmt.Errorf("%w: DeliveryChannel s3BucketName is required", ErrValidation)
	}

	b.mu.Lock("PutDeliveryChannel")
	defer b.mu.Unlock()

	b.channels.Put(&DeliveryChannel{
		Name:                             name,
		S3Bucket:                         s3Bucket,
		SNSArn:                           snsArn,
		S3KeyPrefix:                      s3KeyPrefix,
		ConfigSnapshotDeliveryProperties: props,
	})

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
			out = append(out, *c)
		}
	} else {
		for _, n := range names {
			if c, ok := b.channels.Get(n); ok {
				out = append(out, *c)
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

// DeleteDeliveryChannel removes a delivery channel by name.
func (b *InMemoryBackend) DeleteDeliveryChannel(name string) error {
	if name == "" {
		return fmt.Errorf("%w: DeliveryChannelName is required", ErrValidation)
	}

	b.mu.Lock("DeleteDeliveryChannel")
	defer b.mu.Unlock()

	if !b.channels.Has(name) {
		return fmt.Errorf("%w: %s", ErrNoSuchDeliveryChannel, name)
	}

	b.channels.Delete(name)

	return nil
}

// DeliverConfigSnapshot is a no-op stub.
func (b *InMemoryBackend) DeliverConfigSnapshot(_ string) error { return nil }

// DescribeDeliveryChannelStatus returns statuses for delivery channels.
// If names is empty, all channels are returned.
func (b *InMemoryBackend) DescribeDeliveryChannelStatus(names []string) []DeliveryChannelStatus {
	b.mu.RLock("DescribeDeliveryChannelStatus")
	defer b.mu.RUnlock()

	var channelNames []string

	if len(names) == 0 {
		for _, c := range b.channels.All() {
			channelNames = append(channelNames, c.Name)
		}
	} else {
		for _, name := range names {
			if b.channels.Has(name) {
				channelNames = append(channelNames, name)
			}
		}
	}

	out := make([]DeliveryChannelStatus, 0, len(channelNames))

	for _, name := range channelNames {
		out = append(out, DeliveryChannelStatus{
			Name:                      name,
			ConfigHistoryDeliveryInfo: &DeliveryChannelStatusInfo{LastStatus: recorderStatusSuccess},
			ConfigStreamDeliveryInfo:  &DeliveryChannelStatusInfo{LastStatus: recorderStatusSuccess},
		})
	}

	return out
}
