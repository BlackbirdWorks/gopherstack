package cloudfront

import (
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testEntry(dist, path string, body int, expires time.Time) *cacheEntry {
	return &cacheEntry{
		key: dist + "|" + path, distID: dist, path: path, header: http.Header{},
		body: []byte(strings.Repeat("x", body)), expires: expires,
	}
}

func TestResponseCache_Expiry(t *testing.T) {
	t.Parallel()

	c := newResponseCache(1 << 20)
	t.Cleanup(c.close)

	now := time.Unix(1_000_000, 0)
	c.now = func() time.Time { return now }
	c.put(testEntry("E1", "/a", 10, now.Add(time.Minute)))

	_, ok := c.get("E1|/a")
	require.True(t, ok)

	now = now.Add(time.Minute)

	_, ok = c.get("E1|/a")
	assert.False(t, ok, "entry past its TTL must not be served")

	n, size := c.stats()
	assert.Zero(t, n)
	assert.Zero(t, size)
}

func TestResponseCache_ByteBound(t *testing.T) {
	t.Parallel()

	const limit = 64 << 10

	c := newResponseCache(limit)
	t.Cleanup(c.close)

	far := time.Now().Add(time.Hour)

	for i := range 5000 {
		c.put(testEntry("E1", "/o/"+strconv.Itoa(i), 1024, far))

		_, size := c.stats()
		require.LessOrEqual(t, size, int64(limit), "cache exceeded its byte cap after %d puts", i+1)
	}

	n, size := c.stats()
	assert.Positive(t, n)
	assert.LessOrEqual(t, size, int64(limit))
	assert.Len(t, c.items, n)
	assert.Equal(t, n, c.order.Len())
}

func TestResponseCache_LRUOrder(t *testing.T) {
	t.Parallel()

	c := newResponseCache(8 << 10)
	t.Cleanup(c.close)

	far := time.Now().Add(time.Hour)
	c.put(testEntry("E1", "/keep", 900, far))

	for i := range 40 {
		c.put(testEntry("E1", "/n/"+strconv.Itoa(i), 900, far))

		_, ok := c.get("E1|/keep")
		require.True(t, ok, "recently used entry evicted at put %d", i)
	}

	_, ok := c.get("E1|/n/0")
	assert.False(t, ok, "least recently used entry must be evicted first")
}

func TestResponseCache_OversizedEntrySkipped(t *testing.T) {
	t.Parallel()

	c := newResponseCache(4 << 10)
	t.Cleanup(c.close)

	c.put(testEntry("E1", "/big", 4<<10, time.Now().Add(time.Hour)))

	n, size := c.stats()
	assert.Zero(t, n)
	assert.Zero(t, size)
}

func TestResponseCache_Invalidate(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		patterns []string
		kept     []string
	}{
		{name: "exact", patterns: []string{"/a.txt"}, kept: []string{"/img/b.png", "/img/c.png"}},
		{name: "wildcard_prefix", patterns: []string{"/img/*"}, kept: []string{"/a.txt"}},
		{name: "wildcard_all", patterns: []string{"/*"}},
		{name: "nil_purges_distribution", patterns: nil},
		{name: "no_match", patterns: []string{"/zzz"}, kept: []string{"/a.txt", "/img/b.png", "/img/c.png"}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			c := newResponseCache(1 << 20)
			t.Cleanup(c.close)

			far := time.Now().Add(time.Hour)
			for _, p := range []string{"/a.txt", "/img/b.png", "/img/c.png"} {
				c.put(testEntry("E1", p, 10, far))
			}

			c.put(testEntry("E2", "/a.txt", 10, far))

			c.invalidate("E1", tc.patterns)

			for _, p := range tc.kept {
				_, ok := c.get("E1|" + p)
				assert.True(t, ok, p)
			}

			n, _ := c.stats()
			assert.Equal(t, len(tc.kept)+1, n)

			_, ok := c.get("E2|/a.txt")
			assert.True(t, ok, "other distributions must be untouched")
		})
	}
}

func TestEffectiveTTL(t *testing.T) {
	t.Parallel()

	now := time.Unix(2_000_000, 0)
	ttl := dpTTL{min: 10, def: 100, max: 1000}

	tests := []struct {
		header http.Header
		name   string
		ttl    dpTTL
		want   time.Duration
	}{
		{name: "default", header: http.Header{}, ttl: ttl, want: 100 * time.Second},
		{name: "max_age", header: http.Header{"Cache-Control": {"max-age=500"}}, ttl: ttl, want: 500 * time.Second},
		{
			name:   "s_maxage_wins",
			header: http.Header{"Cache-Control": {"max-age=500, s-maxage=200"}},
			ttl:    ttl,
			want:   200 * time.Second,
		},
		{
			name:   "clamped_to_max",
			header: http.Header{"Cache-Control": {"max-age=99999"}},
			ttl:    ttl,
			want:   1000 * time.Second,
		},
		{name: "clamped_to_min", header: http.Header{"Cache-Control": {"max-age=1"}}, ttl: ttl, want: 10 * time.Second},
		{
			name:   "no_store_uses_min",
			header: http.Header{"Cache-Control": {"no-store"}},
			ttl:    ttl,
			want:   10 * time.Second,
		},
		{
			name:   "no_store_zero_min",
			header: http.Header{"Cache-Control": {"no-store"}},
			ttl:    dpTTL{def: 100, max: 1000},
		},
		{name: "private_zero_min", header: http.Header{"Cache-Control": {"private"}}, ttl: dpTTL{def: 100, max: 1000}},
		{
			name: "expires", ttl: ttl, want: 300 * time.Second,
			header: http.Header{"Expires": {now.Add(300 * time.Second).UTC().Format(http.TimeFormat)}},
		},
		{name: "disabled_policy", header: http.Header{}, ttl: dpTTL{}},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, effectiveTTL(tc.header, tc.ttl, now))
		})
	}
}

func TestMatchPathPattern(t *testing.T) {
	t.Parallel()

	tests := []struct {
		pattern, path string
		want          bool
	}{
		{"*", "/anything/at/all", true},
		{"images/*", "/images/a/b.png", true},
		{"/images/*", "/images/a.png", true},
		{"images/*", "/other/a.png", false},
		{"*.jpg", "/x/y/z.jpg", true},
		{"*.jpg", "/x/y/z.png", false},
		{"a?c", "/abc", true},
		{"a?c", "/abbc", false},
		{"/Images/*", "/images/a.png", false},
		{"static", "/static", true},
		{"static", "/static/x", false},
	}

	for _, tc := range tests {
		t.Run(tc.pattern+"_"+tc.path, func(t *testing.T) {
			t.Parallel()

			assert.Equal(t, tc.want, matchPathPattern(tc.pattern, tc.path))
		})
	}
}
