package kinesis

import "context"

// UpdateStreamWarmThroughput configures pre-warmed throughput for a stream.
// This is a no-op in the in-memory backend (no actual warm-up is needed).
func (b *InMemoryBackend) UpdateStreamWarmThroughput(
	ctx context.Context,
	input *UpdateStreamWarmThroughputInput,
) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("UpdateStreamWarmThroughput")

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	_, ok := b.streams.Get(streamKey(region, streamName))
	b.mu.RUnlock()

	if !ok {
		return ErrStreamNotFound
	}

	return nil
}

// UpdateStreamMode changes the mode of a stream identified by its ARN.
func (b *InMemoryBackend) UpdateStreamMode(ctx context.Context, input *UpdateStreamModeInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.Lock("UpdateStreamMode")
	defer b.mu.Unlock()

	streamName := streamNameFromARN(input.StreamARN)
	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		return ErrStreamNotFound
	}
	stream.mu.Lock("UpdateStreamMode.stream")
	defer stream.mu.Unlock()

	if input.StreamModeDetails.StreamMode != streamModeProvisioned &&
		input.StreamModeDetails.StreamMode != streamModeOnDemand {
		return ErrInvalidArgument
	}

	stream.StreamMode = input.StreamModeDetails.StreamMode

	return nil
}
