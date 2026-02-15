package dynamodb

import (
	"container/list"
	"sync"
)

// ExpressionCache is a simple LRU cache for parsed expressions.
type ExpressionCache struct {
	lru      *list.List
	cache    map[string]*list.Element
	mu       sync.RWMutex
	capacity int
}

type cacheEntry struct {
	value any // parsed expression result (could be a pre-computed structure)
	key   string
}

// NewExpressionCache creates a new LRU cache with the given capacity.
func NewExpressionCache(capacity int) *ExpressionCache {
	return &ExpressionCache{
		capacity: capacity,
		cache:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

// Get retrieves a value from the cache.
func (c *ExpressionCache) Get(key string) (any, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, okElem := c.cache[key]; okElem {
		c.lru.MoveToFront(elem)

		if entry, okEntry := elem.Value.(*cacheEntry); okEntry {
			return entry.value, true
		}
	}

	return nil, false
}

// Put adds a value to the cache.
func (c *ExpressionCache) Put(key string, value any) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if elem, okElem := c.cache[key]; okElem {
		c.lru.MoveToFront(elem)

		if entry, okEntry := elem.Value.(*cacheEntry); okEntry {
			entry.value = value
		}

		return
	}

	entry := &cacheEntry{key: key, value: value}
	elem := c.lru.PushFront(entry)
	c.cache[key] = elem

	if c.lru.Len() > c.capacity {
		c.evict()
	}
}

// evict removes the least recently used item.
func (c *ExpressionCache) evict() {
	elem := c.lru.Back()
	if elem != nil {
		c.lru.Remove(elem)
		if entry, okEntry := elem.Value.(*cacheEntry); okEntry {
			delete(c.cache, entry.key)
		}
	}
}
