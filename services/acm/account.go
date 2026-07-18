package acm

import (
	"context"
	"fmt"
	"time"
)

// accountConfigFor returns the account config for the region, defaulting when unset.
// Callers must hold b.mu.
func (b *InMemoryBackend) accountConfigFor(region string) AccountConfig {
	if cfg, ok := b.accountConfig[region]; ok {
		return cfg
	}

	return AccountConfig{DaysBeforeExpiry: defaultDaysBeforeExpiry}
}

// GetAccountConfiguration returns the account-level ACM configuration for the request region.
func (b *InMemoryBackend) GetAccountConfiguration(ctx context.Context) AccountConfig {
	region := getRegion(ctx, b.region)

	b.mu.RLock("GetAccountConfiguration")
	defer b.mu.RUnlock()

	return b.accountConfigFor(region)
}

// PutAccountConfiguration stores the account-level ACM configuration.
// idempotencyToken must be non-empty; repeated calls with the same token are
// silently accepted only when the configuration is identical (AWS behavior).
// A conflicting call with the same token but different settings returns ErrConflict.
func (b *InMemoryBackend) PutAccountConfiguration(
	ctx context.Context, idempotencyToken string, daysBeforeExpiry *int32,
) error {
	if idempotencyToken == "" {
		return fmt.Errorf("%w: IdempotencyToken is required", ErrInvalidParameter)
	}

	wantDays := defaultDaysBeforeExpiry
	if daysBeforeExpiry != nil {
		if *daysBeforeExpiry < 0 {
			return fmt.Errorf("%w: DaysBeforeExpiry must be non-negative", ErrInvalidParameter)
		}

		wantDays = *daysBeforeExpiry
	}

	region := getRegion(ctx, b.region)

	b.mu.Lock("PutAccountConfiguration")
	defer b.mu.Unlock()

	accountIdempotency := b.accountIdempotencyStore(region)
	if prev, seen := accountIdempotency[idempotencyToken]; seen {
		if prev.DaysBeforeExpiry != wantDays {
			return fmt.Errorf(
				"%w: IdempotencyToken %q was already used with different settings",
				ErrConflict, idempotencyToken,
			)
		}
		// Same token + same settings → idempotent success.
		return nil
	}

	accountIdempotency[idempotencyToken] = accountIdempotencyEntry{
		DaysBeforeExpiry: wantDays,
		CreatedAt:        time.Now().UTC(),
	}
	b.accountConfig[region] = AccountConfig{DaysBeforeExpiry: wantDays}

	return nil
}
