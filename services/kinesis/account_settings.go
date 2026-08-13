package kinesis

import (
	"context"
	"time"
)

// DescribeAccountSettings returns the account's minimum throughput billing
// commitment configuration (kinesis@v1.46.4 api_op_DescribeAccountSettings.go:34-45).
func (b *InMemoryBackend) DescribeAccountSettings(_ context.Context) (*DescribeAccountSettingsOutput, error) {
	b.mu.RLock("DescribeAccountSettings")
	defer b.mu.RUnlock()

	return &DescribeAccountSettingsOutput{
		MinimumThroughputBillingCommitment: b.minimumThroughputBillingCommitment,
	}, nil
}

// UpdateAccountSettings sets the account's minimum throughput billing
// commitment status (kinesis@v1.46.4 api_op_UpdateAccountSettings.go:42-51).
// This backend has no billing engine: no billing behaviour follows from
// enabling the commitment. Status/StartedAt/EndedAt only track the requested
// transition; see MinimumThroughputBillingCommitmentOutput.
func (b *InMemoryBackend) UpdateAccountSettings(
	_ context.Context,
	input *UpdateAccountSettingsInput,
) (*UpdateAccountSettingsOutput, error) {
	if input.MinimumThroughputBillingCommitment == nil {
		return nil, ErrInvalidArgument
	}

	status := input.MinimumThroughputBillingCommitment.Status
	if status != minimumThroughputBillingCommitmentEnabled && status != minimumThroughputBillingCommitmentDisabled {
		return nil, ErrInvalidArgument
	}

	b.mu.Lock("UpdateAccountSettings")
	defer b.mu.Unlock()

	now := time.Now()

	switch {
	case status == minimumThroughputBillingCommitmentEnabled &&
		b.minimumThroughputBillingCommitment.Status != minimumThroughputBillingCommitmentEnabled:
		b.minimumThroughputBillingCommitment = MinimumThroughputBillingCommitmentOutput{
			Status:    minimumThroughputBillingCommitmentEnabled,
			StartedAt: now,
		}
	case status == minimumThroughputBillingCommitmentDisabled &&
		b.minimumThroughputBillingCommitment.Status == minimumThroughputBillingCommitmentEnabled:
		b.minimumThroughputBillingCommitment.Status = minimumThroughputBillingCommitmentDisabled
		b.minimumThroughputBillingCommitment.EndedAt = now
	default:
		b.minimumThroughputBillingCommitment.Status = status
	}

	return &UpdateAccountSettingsOutput{
		MinimumThroughputBillingCommitment: b.minimumThroughputBillingCommitment,
	}, nil
}

// CountOnDemandStreams returns the number of ON_DEMAND streams in the region
// carried on ctx, for DescribeLimits' required OnDemandStreamCount member
// (kinesis@v1.46.4 api_op_DescribeLimits.go:34-45). DescribeLimits is
// region-scoped in AWS, matching CountOpenShards' convention.
func (b *InMemoryBackend) CountOnDemandStreams(ctx context.Context) int {
	region := getRegion(ctx, b.region)

	b.mu.RLock("CountOnDemandStreams")
	defer b.mu.RUnlock()

	count := 0

	for _, s := range b.streamsByRegion.Get(region) {
		s.mu.RLock("CountOnDemandStreams.stream")
		if s.StreamMode == streamModeOnDemand {
			count++
		}
		s.mu.RUnlock()
	}

	return count
}

// OnDemandStreamCountLimit returns the account's current cap on ON_DEMAND
// streams, for DescribeLimits' required OnDemandStreamCountLimit member.
// Real AWS manages this as a Service Quota, not adjustable via
// UpdateAccountSettings; see SetOnDemandStreamCountLimit for how this backend
// exposes changing it (a Go-level config knob, not a wire operation).
func (b *InMemoryBackend) OnDemandStreamCountLimit(_ context.Context) int {
	b.mu.RLock("OnDemandStreamCountLimit")
	defer b.mu.RUnlock()

	return b.onDemandStreamCountLimit
}

// SetOnDemandStreamCountLimit configures the account-level cap CreateStream
// enforces for ON_DEMAND streams (default defaultOnDemandStreamCountLimit).
// No real Kinesis wire operation can change this account setting -- it was
// previously (and incorrectly) exposed as a fabricated field on
// UpdateAccountSettingsInput; real AWS manages it as a Service Quota. This
// method is the Go-level replacement, mirroring how WithKMSValidator wires
// cross-service config outside the wire protocol.
func (b *InMemoryBackend) SetOnDemandStreamCountLimit(n int) {
	b.mu.Lock("SetOnDemandStreamCountLimit")
	defer b.mu.Unlock()

	b.onDemandStreamCountLimit = n
}

// UpdateMaxRecordSize changes the per-record data payload size limit for a stream.
// The wire input is MaxRecordSizeInKiB (kinesis@v1.46.4 api_op_UpdateMaxRecordSize.go:30-47);
// this backend stores the limit in bytes on Stream.MaxRecordSizeBytes, so the
// requested KiB value is converted via bytesPerKiB. The valid range is
// [defaultMaxRecordSizeBytes, absoluteMaxRecordSizeBytes] (1 MiB - 10 MiB).
// The real Input has no StreamName member, only StreamARN.
func (b *InMemoryBackend) UpdateMaxRecordSize(ctx context.Context, input *UpdateMaxRecordSizeInput) error {
	region := regionFromARNOrCtx(ctx, input.StreamARN, b.region)
	streamName := streamNameFromARN(input.StreamARN)

	b.mu.RLock("UpdateMaxRecordSize")

	stream, ok := b.streams.Get(streamKey(region, streamName))
	if !ok {
		b.mu.RUnlock()

		return ErrStreamNotFound
	}
	stream.mu.Lock("UpdateMaxRecordSize.stream")
	b.mu.RUnlock()
	defer stream.mu.Unlock()

	sizeBytes := input.MaxRecordSizeInKiB * bytesPerKiB
	if sizeBytes < defaultMaxRecordSizeBytes || sizeBytes > absoluteMaxRecordSizeBytes {
		return ErrInvalidArgument
	}

	stream.MaxRecordSizeBytes = sizeBytes

	return nil
}
