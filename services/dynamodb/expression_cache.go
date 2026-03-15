package dynamodb

import (
	"container/list"
	"fmt"
	"hash/fnv"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

// ExpressionCache is a sharded LRU cache for parsed expressions.
// Uses multiple independent shards to reduce lock contention under concurrent access.
// Each entry has a TTL; expired entries are evicted lazily on Get and periodically by Sweep.
type ExpressionCache struct {
	shards []*cacheShard
	ttl    time.Duration
}

type cacheShard struct {
	lru      *list.List
	cache    map[string]*list.Element
	mu       *lockmetrics.RWMutex
	capacity int
}

type cacheEntry struct {
	expiresAt time.Time // zero means no TTL
	value     any       // parsed expression result (could be a pre-computed structure)
	key       string
}

const (
	defaultShardCount   = 16
	defaultExprCacheTTL = 10 * time.Minute
)

// NewExpressionCache creates a new sharded LRU cache with the given capacity.
// Capacity is divided equally among shards. Uses defaultExprCacheTTL for entry TTL.
func NewExpressionCache(capacity int) *ExpressionCache {
	return newExpressionCacheWithTTL(capacity, defaultExprCacheTTL)
}

// newExpressionCacheWithTTL creates a new sharded LRU cache with a custom TTL.
func newExpressionCacheWithTTL(capacity int, ttl time.Duration) *ExpressionCache {
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
		ttl:    ttl,
	}
}

// getShard returns the shard for a given key.
func (c *ExpressionCache) getShard(key string) *cacheShard {
	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	shardCount := uint32(len(c.shards)) // #nosec G115 - shard count is constant 16

	return c.shards[h.Sum32()%shardCount]
}

// Get retrieves a value from the cache. Expired entries are removed and a miss is returned.
func (c *ExpressionCache) Get(key string) (any, bool) {
	shard := c.getShard(key)
	shard.mu.Lock("Get")
	defer shard.mu.Unlock()

	elem, elemOk := shard.cache[key]
	if !elemOk {
		return nil, false
	}

	entry, entryOk := elem.Value.(*cacheEntry)
	if !entryOk {
		return nil, false
	}

	// Lazy TTL eviction: remove and return miss if expired.
	if !entry.expiresAt.IsZero() && time.Now().After(entry.expiresAt) {
		shard.lru.Remove(elem)
		delete(shard.cache, key)

		return nil, false
	}

	shard.lru.MoveToFront(elem)

	return entry.value, true
}

// Put adds a value to the cache with the configured TTL.
func (c *ExpressionCache) Put(key string, value any) {
	shard := c.getShard(key)
	shard.mu.Lock("Put")
	defer shard.mu.Unlock()

	var expiresAt time.Time
	if c.ttl > 0 {
		expiresAt = time.Now().Add(c.ttl)
	}

	if elem, elemOk := shard.cache[key]; elemOk {
		shard.lru.MoveToFront(elem)

		if entry, entryOk := elem.Value.(*cacheEntry); entryOk {
			entry.value = value
			entry.expiresAt = expiresAt
		}

		return
	}

	entry := &cacheEntry{key: key, value: value, expiresAt: expiresAt}
	elem := shard.lru.PushFront(entry)
	shard.cache[key] = elem

	if shard.lru.Len() > shard.capacity {
		shard.evict()
	}
}

// Sweep removes all expired entries from the cache. Intended to be called
// periodically by the janitor to bound memory usage over long-running sessions.
func (c *ExpressionCache) Sweep() {
	now := time.Now()
	for _, shard := range c.shards {
		shard.sweepExpired(now)
	}
}

// sweepExpired removes expired entries from a shard.
func (s *cacheShard) sweepExpired(now time.Time) {
	s.mu.Lock("sweepExpired")
	defer s.mu.Unlock()

	elem := s.lru.Back()
	for elem != nil {
		prev := elem.Prev()
		entry, ok := elem.Value.(*cacheEntry)
		if ok && !entry.expiresAt.IsZero() && now.After(entry.expiresAt) {
			s.lru.Remove(elem)
			delete(s.cache, entry.key)
		}
		elem = prev
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
