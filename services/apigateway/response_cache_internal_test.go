package apigateway

import (
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestResponseCache_TTLAndBounds(t *testing.T) {
	t.Parallel()

	tests := []struct {
		run  func(t *testing.T, c *responseCache, clock *time.Time)
		name string
	}{
		{name: "expires_after_ttl", run: func(t *testing.T, c *responseCache, clock *time.Time) {
			t.Helper()
			c.put("k", &cachedResponse{expires: clock.Add(time.Minute)})
			_, ok := c.get("k")
			assert.True(t, ok)
			*clock = clock.Add(time.Minute)
			_, ok = c.get("k")
			assert.False(t, ok, "entry past its TTL is gone")
		}},
		{name: "flush_scoped_to_stage", run: func(t *testing.T, c *responseCache, clock *time.Time) {
			t.Helper()
			c.put(cacheEntryPrefix("a", "s1")+"x", &cachedResponse{expires: clock.Add(time.Hour)})
			c.put(cacheEntryPrefix("a", "s2")+"x", &cachedResponse{expires: clock.Add(time.Hour)})
			c.flush("a", "s1")
			_, gone := c.get(cacheEntryPrefix("a", "s1") + "x")
			_, kept := c.get(cacheEntryPrefix("a", "s2") + "x")
			assert.False(t, gone)
			assert.True(t, kept)
			c.flushAPI("a")
			_, kept = c.get(cacheEntryPrefix("a", "s2") + "x")
			assert.False(t, kept)
		}},
		{name: "bounded", run: func(t *testing.T, c *responseCache, clock *time.Time) {
			t.Helper()
			for i := range maxCachedResponses + 50 {
				c.put(strconv.Itoa(i), &cachedResponse{expires: clock.Add(time.Duration(i+1) * time.Second)})
			}
			assert.LessOrEqual(t, len(c.entries), maxCachedResponses)
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			clock := time.Unix(1_700_000_000, 0)
			c := newResponseCache()
			c.now = func() time.Time { return clock }
			tt.run(t, c, &clock)
		})
	}
}
