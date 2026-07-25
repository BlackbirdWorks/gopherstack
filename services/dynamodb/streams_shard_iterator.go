// Package dynamodb implements the AWS DynamoDB mock service.
// streams_shard_iterator.go implements opaque shard-iterator tokens for the
// DynamoDB Streams GetShardIterator/GetRecords API.
package dynamodb

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

const (
	// shardIteratorTTL is how long a shard iterator token remains valid (15 min per AWS spec).
	shardIteratorTTL = 15 * time.Minute
	// shardIteratorSweepThreshold is the entry count above which Put performs an
	// inline sweep of expired tokens, bounding the store even when the janitor's
	// Sweep() is not being called on a schedule.
	shardIteratorSweepThreshold = 1024
	// shardIteratorTokenLen is the number of random bytes used in each opaque iterator token.
	shardIteratorTokenLen = 16
)

// ShardIteratorEntry holds server-side state for an opaque shard iterator token.
type ShardIteratorEntry struct {
	ExpiresAt time.Time
	TableName string
	StartSeq  int64
	// EndSeq is the EndingSequenceNumber of the shard this iterator belongs to,
	// or 0 for an open (still-active) shard. Once a consumer reads past EndSeq on
	// a closed shard, GetRecords returns a nil NextShardIterator (AWS semantics).
	EndSeq int64
}

// ShardIteratorStore maps opaque random tokens to server-side iterator state.
// It is goroutine-safe.
type ShardIteratorStore struct {
	entries map[string]*ShardIteratorEntry
	// now is the store's clock seam: all TTL/expiry math reads the current
	// time through this func instead of calling time.Now() directly, so
	// tests can simulate the 15-minute ExpiredIteratorException TTL without
	// a real sleep. Defaults to time.Now; override with SetClock.
	now func() time.Time
	mu  sync.Mutex
}

// NewShardIteratorStore creates an empty ShardIteratorStore using the real
// wall clock (time.Now). Use SetClock on the returned store to inject a
// fake clock for expiry testing.
func NewShardIteratorStore() *ShardIteratorStore {
	return &ShardIteratorStore{
		entries: make(map[string]*ShardIteratorEntry),
		now:     time.Now,
	}
}

// SetClock overrides the store's clock. Intended for tests that need to
// simulate shard-iterator expiry (the 15-minute TTL) without sleeping:
// advance the injected clock past ExpiresAt and the next Get-based lookup
// (via Now/IsExpired) observes the entry as expired.
func (s *ShardIteratorStore) SetClock(clock func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.now = clock
}

// Now returns the current time according to the store's clock (real time by
// default, or the injected test clock after SetClock).
func (s *ShardIteratorStore) Now() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.now()
}

// Put stores a new iterator entry for an open shard and returns the opaque token.
func (s *ShardIteratorStore) Put(tableName string, startSeq int64) (string, error) {
	return s.PutWithEnd(tableName, startSeq, 0)
}

// PutWithEnd stores a new iterator entry carrying the owning shard's ending
// sequence number (endSeq == 0 for an open shard) and returns the opaque token.
func (s *ShardIteratorStore) PutWithEnd(tableName string, startSeq, endSeq int64) (string, error) {
	token, err := generateOpaqueToken()
	if err != nil {
		return "", err
	}

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Read the clock under the lock: SetClock mutates s.now concurrently
		// from tests, so this must not race with that write.
		now := s.now()

		// Opportunistically drop expired tokens once the store grows large so it
		// stays bounded between scheduled Sweep() calls. The threshold keeps the
		// common small-store case allocation- and scan-free.
		if len(s.entries) >= shardIteratorSweepThreshold {
			for tok, entry := range s.entries {
				if now.After(entry.ExpiresAt) {
					delete(s.entries, tok)
				}
			}
		}
		s.entries[token] = &ShardIteratorEntry{
			TableName: tableName,
			StartSeq:  startSeq,
			EndSeq:    endSeq,
			ExpiresAt: now.Add(shardIteratorTTL),
		}
	}()

	return token, nil
}

// Get retrieves the entry for a token. Returns nil if the token is unknown.
func (s *ShardIteratorStore) Get(token string) *ShardIteratorEntry {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.entries[token]
}

// Delete removes a token from the store.
func (s *ShardIteratorStore) Delete(token string) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.entries, token)
}

// Sweep removes expired entries from the store.
func (s *ShardIteratorStore) Sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := s.now()

	for token, entry := range s.entries {
		if now.After(entry.ExpiresAt) {
			delete(s.entries, token)
		}
	}
}

// Size returns the number of entries currently in the store (including expired ones
// that have not yet been swept).
func (s *ShardIteratorStore) Size() int {
	s.mu.Lock()
	defer s.mu.Unlock()

	return len(s.entries)
}

// generateOpaqueToken generates a cryptographically-random hex token.
func generateOpaqueToken() (string, error) {
	b := make([]byte, shardIteratorTokenLen)
	if _, err := rand.Read(b); err != nil {
		return "", fmt.Errorf("generate iterator token: %w", err)
	}

	return hex.EncodeToString(b), nil
}
