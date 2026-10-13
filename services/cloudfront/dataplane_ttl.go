package cloudfront

import (
	"net/http"
	"strconv"
	"strings"
	"time"
)

var cacheableStatuses = map[int]bool{ //nolint:gochecknoglobals // static status set
	http.StatusOK: true, http.StatusNonAuthoritativeInfo: true, http.StatusNoContent: true,
	http.StatusMultipleChoices: true, http.StatusMovedPermanently: true,
}

type cacheDirectives struct {
	maxAge  int64
	sMaxAge int64
	noStore bool
	noCache bool
	private bool
	hasMax  bool
	hasSMax bool
}

func parseCacheControl(v string) cacheDirectives {
	var d cacheDirectives

	for part := range strings.SplitSeq(strings.ToLower(v), ",") {
		name, val, _ := strings.Cut(strings.TrimSpace(part), "=")
		val = strings.Trim(val, `"`)

		switch name {
		case "no-store":
			d.noStore = true
		case "no-cache":
			d.noCache = true
		case "private":
			d.private = true
		case "max-age":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				d.maxAge, d.hasMax = n, true
			}
		case "s-maxage":
			if n, err := strconv.ParseInt(val, 10, 64); err == nil {
				d.sMaxAge, d.hasSMax = n, true
			}
		}
	}

	return d
}

// effectiveTTL applies the distribution's min/default/max TTLs to the origin's caching headers.
func effectiveTTL(hd http.Header, t dpTTL, now time.Time) time.Duration {
	d := parseCacheControl(hd.Get("Cache-Control"))
	secs := t.def

	switch {
	case d.noStore || d.noCache || d.private:
		secs = max(t.min, 0)

		return time.Duration(secs) * time.Second
	case d.hasSMax:
		secs = d.sMaxAge
	case d.hasMax:
		secs = d.maxAge
	default:
		if exp, err := http.ParseTime(hd.Get("Expires")); err == nil {
			secs = int64(exp.Sub(now).Seconds())
		}
	}

	secs = min(max(secs, t.min), t.max)

	return time.Duration(secs) * time.Second
}

func (h *Handler) storeInCache(distID, key string, v *viewerReq, pol *dpPolicy, resp *originResp) {
	if !cacheableStatuses[resp.status] || v.r.Header.Get("Range") != "" {
		return
	}

	now := h.dp.cache.now()

	ttl := effectiveTTL(resp.header, pol.ttl, now)
	if ttl <= 0 {
		return
	}

	hd := resp.header.Clone()
	hd.Del("Set-Cookie")

	h.dp.cache.put(&cacheEntry{
		key: key, distID: distID, path: v.path, query: cacheKeyQuery(v.r.URL.Query(), pol),
		status: resp.status, header: hd, body: resp.body, storedAt: now, expires: now.Add(ttl),
	})
}
