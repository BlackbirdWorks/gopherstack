package azurearm

import (
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	apimMaxQuotaEntries = 100000
	apimMaxCacheEntries = 10000
	apimMaxCacheBytes   = 64 << 20
	apimMaxCacheEntry   = 1 << 20
	apimMaxCounterKey   = 2048
	apimMaxOneWay       = 64
)

func apimNewGUID() string { return uuid.NewString() }

// apimKeyOf length-prefixes each part so that parts containing the separator cannot collide.
func apimKeyOf(parts ...string) string {
	var sb strings.Builder

	for _, p := range parts {
		sb.WriteString(strconv.Itoa(len(p)))
		sb.WriteByte(':')
		sb.WriteString(p)
	}

	return sb.String()
}

type apimQuotaEntry struct {
	start, end   time.Time
	calls, bytes int64
}

// apimQuotaStore holds quota counters in memory; like the built-in cache it is volatile.
type apimQuotaStore struct {
	mu *lockmetrics.RWMutex
	m  map[string]*apimQuotaEntry
}

func newAPIMQuotaStore() *apimQuotaStore {
	return &apimQuotaStore{mu: lockmetrics.New("azurearm.apim.quota"), m: map[string]*apimQuotaEntry{}}
}

func (q *apimQuotaStore) reset() {
	q.mu.Lock("reset")
	q.m = map[string]*apimQuotaEntry{}
	q.mu.Unlock()
}

// quotaWindow is the fixed window containing now; windows start at first and repeat every period (0: one window).
func quotaWindow(now, first time.Time, period time.Duration) (time.Time, time.Time) {
	if period <= 0 {
		return first, time.Time{}
	}

	secs := int64(period / time.Second)
	delta := now.Unix() - first.Unix()

	if delta < 0 {
		return first, first.Add(period)
	}

	start := first.Add(time.Duration(delta/secs*secs) * time.Second)

	return start, start.Add(period)
}

func (q *apimQuotaStore) entry(key string, now, first time.Time, period time.Duration) *apimQuotaEntry {
	start, end := quotaWindow(now, first, period)

	e := q.m[key]
	if e == nil || !e.start.Equal(start) {
		if len(q.m) >= apimMaxQuotaEntries/2 {
			for k, o := range q.m {
				if !o.end.IsZero() && !now.Before(o.end) {
					delete(q.m, k)
				}
			}
		}

		for len(q.m) >= apimMaxQuotaEntries {
			for k := range q.m {
				delete(q.m, k)

				break
			}
		}

		e = &apimQuotaEntry{start: start, end: end}
		q.m[key] = e
	}

	return e
}

// quotaReq is one counter an admission checks and, when every counter has room, charges.
type quotaReq struct {
	first       time.Time
	key         string
	period      time.Duration
	calls, kb   int64
	chargeCalls int64
}

// admit checks every counter and, only when all have room, charges their calls, in one critical section so that
// concurrent requests cannot all pass the same check.
func (q *apimQuotaStore) admit(now time.Time, reqs []quotaReq) (string, time.Duration) {
	q.mu.Lock("admit")
	defer q.mu.Unlock()

	entries := make([]*apimQuotaEntry, len(reqs))

	for i, r := range reqs {
		e := q.entry(r.key, now, r.first, r.period)
		entries[i] = e

		var retry time.Duration
		if !e.end.IsZero() {
			retry = e.end.Sub(now)
		}

		switch {
		case r.calls > 0 && e.calls >= r.calls:
			return "calls", retry
		case r.kb > 0 && e.bytes >= r.kb*1024:
			return "bandwidth", retry
		}
	}

	for i, r := range reqs {
		entries[i].calls += r.chargeCalls
	}

	return "", 0
}

func (q *apimQuotaStore) add(key string, now, first time.Time, period time.Duration, calls, bytes int64) {
	q.mu.Lock("add")
	defer q.mu.Unlock()

	e := q.entry(key, now, first, period)
	e.calls += calls
	e.bytes += bytes
}

type apimCacheEntry struct {
	expires time.Time
	value   any
	size    int
}

type gwCachedResponse struct {
	header http.Header
	body   []byte
	status int
}

// apimCache is the volatile per-service response and value cache.
type apimCache struct {
	mu    *lockmetrics.RWMutex
	m     map[string]*apimCacheEntry
	bytes int
}

func newAPIMCache() *apimCache {
	return &apimCache{mu: lockmetrics.New("azurearm.apim.cache"), m: map[string]*apimCacheEntry{}}
}

func (c *apimCache) reset() {
	c.mu.Lock("reset")
	c.m, c.bytes = map[string]*apimCacheEntry{}, 0
	c.mu.Unlock()
}

func (c *apimCache) get(key string, now time.Time) (any, time.Time, bool) {
	c.mu.Lock("lookup")
	defer c.mu.Unlock()

	e := c.m[key]
	if e == nil {
		return nil, time.Time{}, false
	}

	if !now.Before(e.expires) {
		c.drop(key)

		return nil, time.Time{}, false
	}

	return e.value, e.expires, true
}

func (c *apimCache) drop(key string) {
	if e := c.m[key]; e != nil {
		c.bytes -= e.size
		delete(c.m, key)
	}
}

func (c *apimCache) remove(key string) {
	c.mu.Lock("remove")
	c.drop(key)
	c.mu.Unlock()
}

// put stores a value unless it exceeds the per-entry bound.
func (c *apimCache) put(key string, v any, size int, ttl time.Duration, now time.Time) {
	if size > apimMaxCacheEntry || ttl <= 0 {
		return
	}

	c.mu.Lock("put")
	defer c.mu.Unlock()

	c.drop(key)
	c.evict(now, size)

	c.m[key] = &apimCacheEntry{value: v, size: size, expires: now.Add(ttl)}
	c.bytes += size
}

func (c *apimCache) evict(now time.Time, incoming int) {
	if len(c.m) < apimMaxCacheEntries && c.bytes+incoming <= apimMaxCacheBytes {
		return
	}

	for k, e := range c.m {
		if !now.Before(e.expires) {
			c.drop(k)
		}
	}

	for len(c.m) >= apimMaxCacheEntries || c.bytes+incoming > apimMaxCacheBytes {
		var (
			oldest string
			at     time.Time
		)

		for k, e := range c.m {
			if oldest == "" || e.expires.Before(at) {
				oldest, at = k, e.expires
			}
		}

		if oldest == "" {
			return
		}

		c.drop(oldest)
	}
}
