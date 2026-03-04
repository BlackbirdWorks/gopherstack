package dynamodb

import (
	"sync"
	"time"
)

// tableBucket holds separate read and write token buckets for a single table.
type tableBucket struct {
	lastRefillAt time.Time
	readTokens   float64
	writeTokens  float64
	readRate     float64
	writeRate    float64
	mu           sync.Mutex
}

// Throttler manages per-table token buckets for provisioned throughput enforcement.
// When disabled (enabled == false) all capacity checks pass unconditionally.
type Throttler struct {
	now     func() time.Time
	buckets map[string]*tableBucket
	mu      sync.RWMutex
	enabled bool
}

// NewThrottler creates a Throttler. When enabled is false all operations pass without any checks.
func NewThrottler(enabled bool) *Throttler {
	return &Throttler{
		buckets: make(map[string]*tableBucket),
		enabled: enabled,
		now:     time.Now,
	}
}

// SetTableCapacity registers or updates the provisioned capacity for the given key
// (typically "region:tableName"). Existing token counts are preserved (capped at the
// new rate) so that an UpdateTable does not instantly grant a full bucket.
func (t *Throttler) SetTableCapacity(key string, rcu, wcu int64) {
	if !t.enabled {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	now := t.now()

	b, exists := t.buckets[key]
	if !exists {
		b = &tableBucket{
			readTokens:   float64(rcu),
			writeTokens:  float64(wcu),
			readRate:     float64(rcu),
			writeRate:    float64(wcu),
			lastRefillAt: now,
		}
		t.buckets[key] = b

		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked(now)
	b.readRate = float64(rcu)
	b.writeRate = float64(wcu)
	// On a capacity increase, fill the bucket up to the new rate so that the
	// new capacity is immediately available (matches AWS behaviour on scale-up).
	// On a decrease, cap existing tokens to the new ceiling.
	if b.readTokens < b.readRate {
		b.readTokens = b.readRate
	}
	if b.writeTokens < b.writeRate {
		b.writeTokens = b.writeRate
	}
}

// DeleteTable removes the bucket for the given key.
func (t *Throttler) DeleteTable(key string) {
	if !t.enabled {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.buckets, key)
}

// ConsumeRead attempts to deduct units RCUs from the table bucket identified by key.
// Returns ProvisionedThroughputExceededException when the bucket is exhausted.
// Returns nil when throttling is disabled or no bucket exists for the key.
func (t *Throttler) ConsumeRead(key string, units float64) error {
	if !t.enabled {
		return nil
	}

	b := t.getBucket(key)
	if b == nil {
		return nil
	}

	return b.consumeRead(units, t.now())
}

// ConsumeWrite attempts to deduct units WCUs from the table bucket identified by key.
// Returns ProvisionedThroughputExceededException when the bucket is exhausted.
// Returns nil when throttling is disabled or no bucket exists for the key.
func (t *Throttler) ConsumeWrite(key string, units float64) error {
	if !t.enabled {
		return nil
	}

	b := t.getBucket(key)
	if b == nil {
		return nil
	}

	return b.consumeWrite(units, t.now())
}

func (t *Throttler) getBucket(key string) *tableBucket {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.buckets[key]
}

// refillLocked adds elapsed-time tokens up to the rate ceiling.
// Caller must hold b.mu.
func (b *tableBucket) refillLocked(now time.Time) {
	elapsed := now.Sub(b.lastRefillAt).Seconds()
	if elapsed <= 0 {
		return
	}

	b.readTokens = min(b.readRate, b.readTokens+elapsed*b.readRate)
	b.writeTokens = min(b.writeRate, b.writeTokens+elapsed*b.writeRate)
	b.lastRefillAt = now
}

func (b *tableBucket) consumeRead(units float64, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked(now)

	if b.readTokens < units {
		return NewProvisionedThroughputExceededException(
			"The level of configured provisioned throughput for the table was exceeded. " +
				"Consider increasing your provisioning level with the UpdateTable API.",
		)
	}

	b.readTokens -= units

	return nil
}

func (b *tableBucket) consumeWrite(units float64, now time.Time) error {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.refillLocked(now)

	if b.writeTokens < units {
		return NewProvisionedThroughputExceededException(
			"The level of configured provisioned throughput for the table was exceeded. " +
				"Consider increasing your provisioning level with the UpdateTable API.",
		)
	}

	b.writeTokens -= units

	return nil
}
