package ociapigateway

import (
	"math"
	"net"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	maxBuckets     = 10000
	bucketIdle     = time.Minute
	ipv6PrefixBits = 64
	ipv6Bits       = 128
)

type bucket struct {
	last   time.Time
	tokens float64
}

// rateLimiter is a per-key token bucket: capacity and refill rate both equal the configured requests per second.
type rateLimiter struct {
	mu      *lockmetrics.RWMutex
	buckets map[string]*bucket
}

func newRateLimiter() *rateLimiter {
	return &rateLimiter{mu: lockmetrics.New("ociapigateway.ratelimit"), buckets: map[string]*bucket{}}
}

func (l *rateLimiter) reset() {
	l.mu.Lock("reset")
	l.buckets = map[string]*bucket{}
	l.mu.Unlock()
}

// allow takes one token; when refused it returns the seconds until a token is available.
func (l *rateLimiter) allow(key string, rate int, now time.Time) (bool, int) {
	l.mu.Lock("allow")
	defer l.mu.Unlock()

	capacity := float64(rate)

	bk, ok := l.buckets[key]
	if !ok {
		if len(l.buckets) >= maxBuckets {
			l.prune(now)
		}

		bk = &bucket{tokens: capacity, last: now}
		l.buckets[key] = bk
	}

	if elapsed := now.Sub(bk.last).Seconds(); elapsed > 0 {
		bk.tokens = math.Min(capacity, bk.tokens+elapsed*capacity)
		bk.last = now
	}

	if bk.tokens >= 1 {
		bk.tokens--

		return true, 0
	}

	return false, max(1, int(math.Ceil((1-bk.tokens)/capacity)))
}

func (l *rateLimiter) prune(now time.Time) {
	for k, bk := range l.buckets {
		if now.Sub(bk.last) > bucketIdle {
			delete(l.buckets, k)
		}
	}
}

// clientKey identifies the API client: the IPv4 address, or the IPv6 /64 prefix.
func clientKey(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		host = r.RemoteAddr
	}

	ip := net.ParseIP(host)
	if ip == nil {
		return host
	}

	if v4 := ip.To4(); v4 != nil {
		return v4.String()
	}

	return ip.Mask(net.CIDRMask(ipv6PrefixBits, ipv6Bits)).String()
}
