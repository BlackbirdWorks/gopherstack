package kinesis

import "context"

// UpdateStreamWarmThroughput configures pre-warmed throughput for a stream
// (kinesis@v1.46.4 api_op_UpdateStreamWarmThroughput.go:63-70, required
// WarmThroughputMiBps). Real AWS applies this asynchronously (stream goes
// UPDATING then back to ACTIVE); this backend has no transient-state model
// for that (streams are always ACTIVE), so the change is applied
// synchronously and Current/Target always match on read -- see
// UpdateStreamWarmThroughputOutput and PARITY.md.
func (b *InMemoryBackend) UpdateStreamWarmThroughput(
	ctx context.Context,
	input *UpdateStreamWarmThroughputInput,
) (*UpdateStreamWarmThroughputOutput, error) {
	if input.WarmThroughputMiBps <= 0 || input.WarmThroughputMiBps > maxWarmThroughputMiBps {
		return nil, ErrInvalidArgument
	}

	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)

	b.mu.RLock("UpdateStreamWarmThroughput")

	streamName := input.StreamName
	if streamName == "" {
		streamName = streamNameFromARN(input.StreamARN)
	}

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		b.mu.RUnlock()

		return nil, ErrStreamNotFound
	}
	stream.mu.Lock("UpdateStreamWarmThroughput.stream")
	b.mu.RUnlock()

	stream.WarmThroughputMiBps = input.WarmThroughputMiBps
	arnOut, nameOut := stream.ARN, stream.Name
	stream.mu.Unlock()

	return &UpdateStreamWarmThroughputOutput{
		StreamARN:  arnOut,
		StreamName: nameOut,
		WarmThroughput: WarmThroughputObject{
			CurrentMiBps: input.WarmThroughputMiBps,
			TargetMiBps:  input.WarmThroughputMiBps,
		},
	}, nil
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

	newMode := input.StreamModeDetails.StreamMode
	if newMode != streamModeProvisioned && newMode != streamModeOnDemand {
		return ErrInvalidArgument
	}

	// AWS auto-scales a stream's shard count when it transitions to ON_DEMAND,
	// "to handle up to double the maximum throughput ... or up to double the
	// peak throughput within the last 30 days, whichever is higher." This
	// emulator has no throughput-history model to compute that from, so it
	// approximates with the same floor CreateStream uses for a fresh ON_DEMAND
	// stream (defaultOnDemandShardCount): if the stream is currently under that
	// floor, reshard up to it. A stream already at or above the floor is left
	// alone (real AWS would only grow it further under sustained load, which
	// this emulator also has no model for -- see PARITY.md). The reverse
	// transition (ON_DEMAND -> PROVISIONED) keeps the current shard count as
	// the new provisioned baseline; AWS does not reshard on that direction.
	if stream.StreamMode == streamModeProvisioned && newMode == streamModeOnDemand {
		if countOpenShards(stream.Shards) < defaultOnDemandShardCount {
			reshardTo(stream, defaultOnDemandShardCount)
		}
	}

	stream.StreamMode = newMode

	return nil
}
