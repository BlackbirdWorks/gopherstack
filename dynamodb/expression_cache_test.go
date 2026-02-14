package dynamodb_test

import (
	"Gopherstack/dynamodb"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestExpressionCache(t *testing.T) {
	t.Parallel()
	cache := dynamodb.NewExpressionCache(2)

	// Test Put
	cache.Put("key1", "val1")
	cache.Put("key2", "val2")

	// Test Get
	val1, found1 := cache.Get("key1")
	assert.True(t, found1)
	assert.Equal(t, "val1", val1)

	val2, found2 := cache.Get("key2")
	assert.True(t, found2)
	assert.Equal(t, "val2", val2)

	// Test Missing
	_, found3 := cache.Get("key3")
	assert.False(t, found3)

	// Test Eviction (LRU)
	// key1 was accessed last (MoveToFront in Get), so key2 is LRU?
	// Wait, Get("key1") moves key1 to front. Get("key2") moves key2 to front.
	// Order: key1, key2. Front is key2. Back is key1.
	// Access key1 again
	cache.Get("key1")
	// Order: key2, key1. Front is key1. Back is key2.

	// Add key3. Should evict key2.
	cache.Put("key3", "val3")

	_, found2 = cache.Get("key2")
	assert.False(t, found2, "key2 should be evicted")

	val3, found3 := cache.Get("key3")
	assert.True(t, found3)
	assert.Equal(t, "val3", val3)

	_, found1 = cache.Get("key1")
	assert.True(t, found1, "key1 should remain")

	// Test Update
	cache.Put("key3", "val3-updated")
	val3, _ = cache.Get("key3")
	assert.Equal(t, "val3-updated", val3)
}

func TestExpressionCache_Concurrency(t *testing.T) {
	t.Parallel()
	cache := dynamodb.NewExpressionCache(100)
	done := make(chan bool)

	go func() {
		for range 1000 {
			cache.Put("key", 1)
		}
		done <- true
	}()

	go func() {
		for range 1000 {
			_, _ = cache.Get("key")
		}
		done <- true
	}()

	<-done
	<-done
}
