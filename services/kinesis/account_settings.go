package kinesis

import "context"

// DescribeAccountSettings returns account-level limits for this Kinesis account.
// The ON_DEMAND stream count is reported per region (AWS account-level limits
// are tracked per region), using the region carried on ctx.
func (b *InMemoryBackend) DescribeAccountSettings(ctx context.Context) (*DescribeAccountSettingsOutput, error) {
	region := getRegion(ctx, b.region)

	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	onDemandCount := 0
	for _, s := range b.streamsByRegion.Get(region) {
		s.mu.RLock("DescribeAccountSettings.stream")
		if s.StreamMode == streamModeOnDemand {
			onDemandCount++
		}
		s.mu.RUnlock()
	}

	return &DescribeAccountSettingsOutput{
		ShardLimit:               kinesisDefaultShardLimit,
		OnDemandStreamCount:      onDemandCount,
		OnDemandStreamCountLimit: b.onDemandStreamCountLimit,
	}, nil
}

// UpdateAccountSettings updates account-level settings such as the ON_DEMAND stream count limit.
func (b *InMemoryBackend) UpdateAccountSettings(_ context.Context, input *UpdateAccountSettingsInput) error {
	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	if input.OnDemandStreamCountLimit < 0 {
		return ErrInvalidArgument
	}

	if input.OnDemandStreamCountLimit > 0 {
		b.onDemandStreamCountLimit = input.OnDemandStreamCountLimit
	}

	return nil
}

// UpdateMaxRecordSize changes the per-record data payload size limit for a stream.
// The value must be between defaultMaxRecordSizeBytes (1 MiB) and
// absoluteMaxRecordSizeBytes (10 MiB).
func (b *InMemoryBackend) UpdateMaxRecordSize(ctx context.Context, input *UpdateMaxRecordSizeInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("UpdateMaxRecordSize")

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("UpdateMaxRecordSize.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	if input.MaxRecordSizeBytes < defaultMaxRecordSizeBytes || input.MaxRecordSizeBytes > absoluteMaxRecordSizeBytes {
		return ErrInvalidArgument
	}

	stream.MaxRecordSizeBytes = input.MaxRecordSizeBytes

	return nil
}
