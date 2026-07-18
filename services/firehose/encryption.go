package firehose

import (
	"context"
	"fmt"
	"strings"
	"time"
)

// StartDeliveryStreamEncryption enables server-side encryption for a delivery stream.
// In this in-memory implementation the status transitions directly to ENABLED.
func (b *InMemoryBackend) StartDeliveryStreamEncryption(
	ctx context.Context, name string, input *EncryptionConfigInput,
) error {
	if input != nil && input.KeyType == "CUSTOMER_MANAGED_CMK" && strings.TrimSpace(input.KeyARN) == "" {
		return fmt.Errorf("%w: KeyARN is required when KeyType is CUSTOMER_MANAGED_CMK", ErrValidation)
	}

	b.mu.Lock("StartDeliveryStreamEncryption")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	if s.DeliveryStreamType == deliveryStreamTypeKinesisSource {
		return fmt.Errorf("%w: cannot enable SSE on a KinesisStreamAsSource stream", ErrValidation)
	}

	cfg := &EncryptionConfig{Status: "ENABLED", KeyType: "AWS_OWNED_CMK"}
	if input != nil {
		if input.KeyType != "" {
			cfg.KeyType = input.KeyType
		}
		cfg.KeyARN = input.KeyARN
	}

	s.Encryption = cfg
	s.LastUpdateTimestamp = time.Now()

	return nil
}

// StopDeliveryStreamEncryption disables server-side encryption for a delivery stream.
// In this in-memory implementation the status transitions directly to DISABLED.
func (b *InMemoryBackend) StopDeliveryStreamEncryption(ctx context.Context, name string) error {
	b.mu.Lock("StopDeliveryStreamEncryption")
	defer b.mu.Unlock()

	region := getRegionFromContext(ctx, b)

	s, ok := b.streams.Get(regionKey(region, name))
	if !ok {
		return fmt.Errorf("%w: stream %s not found", ErrNotFound, name)
	}

	s.Encryption = &EncryptionConfig{Status: "DISABLED"}
	s.LastUpdateTimestamp = time.Now()

	return nil
}
