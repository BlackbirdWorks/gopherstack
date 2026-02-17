package dynamodb_test

import (
	"sync"
	"testing"

	"Gopherstack/dynamodb"

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
			ops: func(t *testing.T, cache *dynamodb.ExpressionCache) {
				t.Helper()
				// Cache size is 2 (created in subtest)
				cache.Put("key1", "val1")
				cache.Put("key2", "val2")
				// Access key1 to make it MRU
				cache.Get("key1")
				// Put key3, should evict key2
				cache.Put("key3", "val3")

				_, found2 := cache.Get("key2")
				assert.False(t, found2, "key2 should be evicted")

				val1, found1 := cache.Get("key1")
				assert.True(t, found1)
				assert.Equal(t, "val1", val1)

				val3, found3 := cache.Get("key3")
				assert.True(t, found3)
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
