package dynamodb

import (
	"container/list"
	"fmt"
	"hash/fnv"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ExpressionCache is a sharded LRU cache for parsed expressions.
// Uses multiple independent shards to reduce lock contention under concurrent access.
type ExpressionCache struct {
	shards []*cacheShard
}

type cacheShard struct {
	lru      *list.List
	cache    map[string]*list.Element
	mu       *lockmetrics.RWMutex
	capacity int
}

type cacheEntry struct {
	value any // parsed expression result (could be a pre-computed structure)
	key   string
}

const defaultShardCount = 16

// NewExpressionCache creates a new sharded LRU cache with the given capacity.
// Capacity is divided equally among shards.
func NewExpressionCache(capacity int) *ExpressionCache {
	shardSize := (capacity + defaultShardCount - 1) / defaultShardCount
	shards := make([]*cacheShard, defaultShardCount)

	for i := range shards {
		shards[i] = &cacheShard{
			capacity: shardSize,
			cache:    make(map[string]*list.Element),
			lru:      list.New(),
			mu:       lockmetrics.New(fmt.Sprintf("dynamodb.expr_cache.%d", i)),
		}
	}

	return &ExpressionCache{
		shards: shards,
	}
}

// getShard returns the shard for a given key.
func (c *ExpressionCache) getShard(key string) *cacheShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	shardCount := uint32(len(c.shards)) // #nosec G115 - shard count is constant 16

	return c.shards[h.Sum32()%shardCount]
}

// Get retrieves a value from the cache.
func (c *ExpressionCache) Get(key string) (any, bool) {
	shard := c.getShard(key)
	shard.mu.Lock("Get")
	defer shard.mu.Unlock()

	if elem, elemOk := shard.cache[key]; elemOk {
		shard.lru.MoveToFront(elem)

		if entry, entryOk := elem.Value.(*cacheEntry); entryOk {
			return entry.value, true
		}
	}

	return nil, false
}

// Put adds a value to the cache.
func (c *ExpressionCache) Put(key string, value any) {
	shard := c.getShard(key)
	shard.mu.Lock("Put")
	defer shard.mu.Unlock()

	if elem, elemOk := shard.cache[key]; elemOk {
		shard.lru.MoveToFront(elem)

		if entry, entryOk := elem.Value.(*cacheEntry); entryOk {
			entry.value = value
		}

		return
	}

	entry := &cacheEntry{key: key, value: value}
	elem := shard.lru.PushFront(entry)
	shard.cache[key] = elem

	if shard.lru.Len() > shard.capacity {
		shard.evict()
	}
}

// evict removes the least recently used item from the shard.
func (s *cacheShard) evict() {
	elem := s.lru.Back()
	if elem != nil {
		s.lru.Remove(elem)
		if entry, ok := elem.Value.(*cacheEntry); ok {
			delete(s.cache, entry.key)
		}
	}
}
