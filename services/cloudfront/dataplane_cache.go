package cloudfront

import (
	"container/list"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	defaultCacheBytes    = 64 << 20
	cacheEntryOverhead   = 256
	cacheMaxEntryDivisor = 4
)

type cacheEntry struct {
	storedAt time.Time
	expires  time.Time
	header   http.Header
	key      string
	distID   string
	path     string
	query    string
	body     []byte
	size     int64
	status   int
}

// responseCache is a byte-bounded LRU of origin responses.
type responseCache struct {
	mu      *lockmetrics.RWMutex
	items   map[string]*list.Element
	order   *list.List
	now     func() time.Time
	maxSize int64
	size    int64
}

func newResponseCache(maxBytes int64) *responseCache {
	return &responseCache{
		mu:      lockmetrics.New("cloudfront.dataplane.cache"),
		items:   make(map[string]*list.Element),
		order:   list.New(),
		now:     time.Now,
		maxSize: maxBytes,
	}
}

func (c *responseCache) get(key string) (*cacheEntry, bool) {
	c.mu.Lock("get")
	defer c.mu.Unlock()

	el, ok := c.items[key]
	if !ok {
		return nil, false
	}

	e, _ := el.Value.(*cacheEntry)
	if !c.now().Before(e.expires) {
		c.removeLocked(el)

		return nil, false
	}

	c.order.MoveToFront(el)

	return e, true
}

func entrySize(e *cacheEntry) int64 {
	n := int64(len(e.body) + len(e.key) + len(e.path) + len(e.query) + cacheEntryOverhead)
	for k, vs := range e.header {
		n += int64(len(k))
		for _, v := range vs {
			n += int64(len(v))
		}
	}

	return n
}

func (c *responseCache) put(e *cacheEntry) {
	e.size = entrySize(e)

	c.mu.Lock("put")
	defer c.mu.Unlock()

	if old, ok := c.items[e.key]; ok {
		c.removeLocked(old)
	}

	if e.size > c.maxSize/cacheMaxEntryDivisor {
		return
	}

	c.items[e.key] = c.order.PushFront(e)
	c.size += e.size

	for c.size > c.maxSize {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}

		c.removeLocked(oldest)
	}
}

func (c *responseCache) removeLocked(el *list.Element) {
	e, _ := el.Value.(*cacheEntry)
	c.order.Remove(el)
	delete(c.items, e.key)
	c.size -= e.size
}

// invalidate drops a distribution's entries matching any pattern; nil patterns drops them all.
func (c *responseCache) invalidate(distID string, patterns []string) {
	c.mu.Lock("invalidate")
	defer c.mu.Unlock()

	for el := c.order.Front(); el != nil; {
		next := el.Next()
		e, _ := el.Value.(*cacheEntry)

		if e.distID == distID && invalidationMatches(patterns, e) {
			c.removeLocked(el)
		}

		el = next
	}
}

func invalidationMatches(patterns []string, e *cacheEntry) bool {
	if patterns == nil {
		return true
	}

	for _, p := range patterns {
		if globMatchCS(p, e.path) {
			return true
		}

		if e.query != "" && strings.Contains(p, "?") && globMatchCS(p, e.path+"?"+e.query) {
			return true
		}
	}

	return false
}

func (c *responseCache) clear() {
	c.mu.Lock("clear")
	defer c.mu.Unlock()

	c.items = make(map[string]*list.Element)
	c.order.Init()
	c.size = 0
}

func (c *responseCache) setMax(n int64) {
	c.mu.Lock("setMax")
	defer c.mu.Unlock()

	c.maxSize = n

	for c.size > c.maxSize {
		oldest := c.order.Back()
		if oldest == nil {
			break
		}

		c.removeLocked(oldest)
	}
}

func (c *responseCache) stats() (int, int64) {
	c.mu.RLock("stats")
	defer c.mu.RUnlock()

	return len(c.items), c.size
}

func (c *responseCache) close() { c.mu.Close() }
