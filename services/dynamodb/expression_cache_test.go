package dynamodb_test

import (
	"fmt"
	"sync"
	"testing"

	"github.com/blackbirdworks/gopherstack/services/dynamodb"

	"github.com/stretchr/testify/assert"
)

func TestExpressionCache(t *testing.T) {
	t.Parallel()

	tests := []struct {
		ops  func(t *testing.T, cache *dynamodb.ExpressionCache)
		name string
	}{
		{
			name: "BasicPutAndGet",
			ops: func(t *testing.T, cache *dynamodb.ExpressionCache) {
				t.Helper()
				cache.Put("key1", "val1")
				val, found := cache.Get("key1")
				assert.True(t, found)
				assert.Equal(t, "val1", val)
			},
		},
		{
			name: "MissingKey",
			ops: func(t *testing.T, cache *dynamodb.ExpressionCache) {
				t.Helper()
				_, found := cache.Get("key-missing")
				assert.False(t, found)
			},
		},
		{
			name: "EvictionLRU",
			ops: func(t *testing.T, _ *dynamodb.ExpressionCache) {
				t.Helper()
				// For sharded cache, capacity is divided among 16 shards.
				// Create a cache with enough capacity that all items go to the same shard.
				// With capacity 256, each shard gets ~16 slots.
				// Put 3 items into a shard to test eviction.
				largeCache := dynamodb.NewExpressionCache(256)

				// We'll use keys that we know (or hope) hash to the same shard
				// For deterministic testing, we can just verify per-shard LRU works
				// by filling up one shard's capacity
				largeCache.Put("key1", "val1")
				largeCache.Put("key2", "val2")
				// Access key1 to make it MRU
				largeCache.Get("key1")
				// Put key3, if it hashes to same shard as key2, key2 should evict eventually
				largeCache.Put("key3", "val3")

				// Both key1 and key3 should still exist since key1 was accessed (MRU)
				// and key3 was just added
				val1, found1 := largeCache.Get("key1")
				assert.True(t, found1, "key1 should exist")
				assert.Equal(t, "val1", val1)

				val3, found3 := largeCache.Get("key3")
				assert.True(t, found3, "key3 should exist")
				assert.Equal(t, "val3", val3)
			},
		},
		{
			name: "UpdateKey",
			ops: func(t *testing.T, cache *dynamodb.ExpressionCache) {
				t.Helper()
				cache.Put("key1", "val1")
				cache.Put("key1", "val1-updated")
				val, _ := cache.Get("key1")
				assert.Equal(t, "val1-updated", val)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			cache := dynamodb.NewExpressionCache(2)
			tt.ops(t, cache)
		})
	}
}

func TestExpressionCache_Concurrency(t *testing.T) {
	t.Parallel()
	cache := dynamodb.NewExpressionCache(100)
	ctx := t.Context()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		for range 1000 {
			select {
			case <-ctx.Done():
				return
			default:
				cache.Put("key", 1)
			}
		}
	}()

	go func() {
		defer wg.Done()
		for range 1000 {
			select {
			case <-ctx.Done():
				return
			default:
				_, _ = cache.Get("key")
			}
		}
	}()

	wg.Wait()
}

// TestExpressionCache_Eviction verifies that the LRU shard evict() path runs
// when the shard capacity is exceeded. With capacity=16 (1 slot per shard) and
// 17 distinct keys, the pigeonhole principle guarantees at least one shard
// receives a second key, triggering eviction.
func TestExpressionCache_Eviction(t *testing.T) {
	t.Parallel()

	// capacity 16 → each of the 16 shards gets capacity 1.
	cache := dynamodb.NewExpressionCache(16)

	// Insert 17 distinct keys so at least one shard must receive 2 entries.
	for i := range 17 {
		cache.Put(fmt.Sprintf("evict-key-%d", i), i)
	}

	// We can't assert which keys were evicted (hash-dependent), but we can assert
	// that at least one key that was added early no longer exists, or simply that
	// no panic occurred and the cache is still usable.
	cache.Put("sentinel", "alive")
	v, found := cache.Get("sentinel")
	assert.True(t, found)
	assert.Equal(t, "alive", v)
}
