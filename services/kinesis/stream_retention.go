package kinesis

import "context"

// IncreaseStreamRetentionPeriod increases the retention period for a stream.
// A target equal to the current retention period is treated as an idempotent
// no-op returning success (HTTP 200), matching real AWS behaviour: the
// Terraform AWS provider calls IncreaseStreamRetentionPeriod on stream create
// for ANY configured retention_period > 0 (see the provider's resourceStreamCreate,
// guard `v.(int) > 0`), so a stream created with the default retention_period
// of 24h receives IncreaseStreamRetentionPeriod(24) against a stream already at
// 24h. Rejecting that equal value with InvalidArgumentException (as a strict
// reading of the SDK doc "Must be more than the current retention period" would
// suggest) breaks every default-retention Terraform stream, so real AWS accepts
// it. A target below the current period, below minRetentionHours (24h), or above
// maxRetentionHours (8760h) is rejected with InvalidArgumentException.
func (b *InMemoryBackend) IncreaseStreamRetentionPeriod(
	ctx context.Context,
	input *IncreaseStreamRetentionPeriodInput,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("IncreaseStreamRetentionPeriod")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(region, input.StreamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("IncreaseStreamRetentionPeriod.stream")
	defer stream.mu.Unlock()

	// Idempotent: a target equal to the current retention period is a no-op.
	if input.RetentionPeriodHours == stream.RetentionPeriod {
		return nil
	}

	if input.RetentionPeriodHours < stream.RetentionPeriod ||
		input.RetentionPeriodHours < minRetentionHours ||
		input.RetentionPeriodHours > maxRetentionHours {
		return ErrInvalidArgument
	}

	stream.RetentionPeriod = input.RetentionPeriodHours

	return nil
}

// DecreaseStreamRetentionPeriod decreases the retention period for a stream.
// Mirroring IncreaseStreamRetentionPeriod, a target equal to the current
// retention period is an idempotent no-op returning success (HTTP 200), matching
// real AWS behaviour. A target above the current period or below
// minRetentionHours (24h) is rejected with InvalidArgumentException.
func (b *InMemoryBackend) DecreaseStreamRetentionPeriod(
	ctx context.Context,
	input *DecreaseStreamRetentionPeriodInput,
) error {
	region := getRegion(ctx, b.region)

	b.mu.Lock("DecreaseStreamRetentionPeriod")
	defer b.mu.Unlock()

	stream, ok := b.streams.Get(streamKey(region, input.StreamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("DecreaseStreamRetentionPeriod.stream")
	defer stream.mu.Unlock()

	// Idempotent: a target equal to the current retention period is a no-op.
	if input.RetentionPeriodHours == stream.RetentionPeriod {
		return nil
	}

	if input.RetentionPeriodHours > stream.RetentionPeriod ||
		input.RetentionPeriodHours < minRetentionHours {
		return ErrInvalidArgument
	}

	stream.RetentionPeriod = input.RetentionPeriodHours

	return nil
}
