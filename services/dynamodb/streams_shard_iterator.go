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

// shardIteratorEntry holds server-side state for an opaque shard iterator token.
type shardIteratorEntry struct {
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
	entries map[string]*shardIteratorEntry
	mu      sync.Mutex
}

// NewShardIteratorStore creates an empty ShardIteratorStore.
func NewShardIteratorStore() *ShardIteratorStore {
	return &ShardIteratorStore{
		entries: make(map[string]*shardIteratorEntry),
	}
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

	now := time.Now()

	func() {
		s.mu.Lock()
		defer s.mu.Unlock()

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
		s.entries[token] = &shardIteratorEntry{
			TableName: tableName,
			StartSeq:  startSeq,
			EndSeq:    endSeq,
			ExpiresAt: now.Add(shardIteratorTTL),
		}
	}()

	return token, nil
}

// Get retrieves the entry for a token. Returns nil if the token is unknown.
func (s *ShardIteratorStore) Get(token string) *shardIteratorEntry {
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
	now := time.Now()

	s.mu.Lock()
	defer s.mu.Unlock()

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
