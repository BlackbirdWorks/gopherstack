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

	// PROVISIONED -> ON_DEMAND does NOT reshard at transition time: "When you
	// switch from provisioned to on-demand capacity mode, your data stream
	// initially retains whatever shard count it had before the transition,
	// and from this point on, Kinesis Data Streams monitors your data
	// traffic and scales the shard count of this on-demand data stream
	// depending on your write throughput." (docs.aws.amazon.com/streams/
	// latest/dev/how-do-i-size-a-stream.html#switchingmodes). Any needed
	// growth happens reactively afterward, via maybeAutoScaleOnDemand on
	// subsequent PutRecord calls (see ondemand_scaling.go and PARITY.md) --
	// not here. A brand-new ON_DEMAND stream still starts at
	// defaultOnDemandShardCount via CreateStream, which this transition path
	// does not touch. ON_DEMAND -> PROVISIONED also keeps the current shard
	// count as the new provisioned baseline; AWS does not reshard on that
	// direction either.
	stream.StreamMode = newMode

	// WarmThroughputMiBps is "only valid when the stream mode is being
	// updated to on-demand" (api_op_UpdateStreamMode.go); a value supplied
	// alongside a PROVISIONED transition is ignored, matching that
	// documented constraint.
	if newMode == streamModeOnDemand && input.WarmThroughputMiBps > 0 {
		if input.WarmThroughputMiBps > maxWarmThroughputMiBps {
			return ErrInvalidArgument
		}
		stream.WarmThroughputMiBps = input.WarmThroughputMiBps
	}

	return nil
}
