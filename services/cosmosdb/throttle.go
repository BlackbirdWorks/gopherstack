package cosmosdb

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/labstack/echo/v5"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

const (
	headerRetryAfterMS = "X-Ms-Retry-After-Ms"
	headerSubStatus    = "X-Ms-Substatus"
	subStatusThrottled = "3200"
	msPerSecond        = 1000.0
	throttledMessage   = "Request rate is large. More Request Units may be needed, so no changes were made. " +
		"Please retry after %d ms."
)

// ruBucket is a one-second token bucket of request units; tokens may go negative (a request that overdraws is
// served, the debt is repaid by refill before the next one is admitted), as the service does.
type ruBucket struct {
	last   time.Time
	tokens float64
	rate   float64
}

// ruMeter meters request units per throughput bucket on an injected clock.
type ruMeter struct {
	mu      *lockmetrics.RWMutex
	buckets map[string]*ruBucket
	epoch   int64
}

func newRUMeter() *ruMeter {
	return &ruMeter{mu: lockmetrics.New("cosmosdb.ru"), buckets: make(map[string]*ruBucket)}
}

func (m *ruMeter) reset() {
	m.mu.Lock("reset")
	defer m.mu.Unlock()

	m.buckets = make(map[string]*ruBucket)
}

// bucket returns the refilled bucket for key; the lock must be held.
func (m *ruMeter) bucket(key string, rate float64, epoch int64, now time.Time) *ruBucket {
	if epoch != m.epoch {
		m.epoch = epoch
		m.buckets = make(map[string]*ruBucket)
	}

	b, ok := m.buckets[key]
	if !ok || b.rate != rate {
		b = &ruBucket{tokens: rate, rate: rate, last: now}
		m.buckets[key] = b

		return b
	}

	b.tokens = math.Min(rate, b.tokens+rate*now.Sub(b.last).Seconds())
	b.last = now

	return b
}

// admit returns how long the caller must wait before the bucket is out of debt; 0 admits the request.
func (m *ruMeter) admit(key string, rate float64, epoch int64, now time.Time) time.Duration {
	m.mu.Lock("admit")
	defer m.mu.Unlock()

	b := m.bucket(key, rate, epoch, now)
	if b.tokens >= 0 {
		return 0
	}

	wait := time.Duration(math.Ceil(-b.tokens/rate*msPerSecond)) * time.Millisecond

	return max(wait, time.Millisecond)
}

func (m *ruMeter) charge(key string, rate float64, epoch int64, now time.Time, ru float64) {
	m.mu.Lock("charge")
	defer m.mu.Unlock()

	m.bucket(key, rate, epoch, now).tokens -= ru
}

// isMetered reports whether a resource kind consumes container request units.
func isMetered(kind resourceKind) bool {
	if _, _, ok := scriptKindOf(kind); ok {
		return true
	}

	return kind == resourceDocuments || kind == resourceDocumentItem
}

func chargeOf(c *echo.Context) float64 {
	ru, err := strconv.ParseFloat(c.Response().Header().Get(headerRequestCharge), 64)
	if err != nil {
		return 0
	}

	return ru
}

// serveMetered runs next unless the container's budget is overdrawn, then charges what the response reports.
func (h *Handler) serveMetered(c *echo.Context, dbID, collID string, next func() error) error {
	tb, ok := h.Backend.(ThroughputBackend)
	if !ok {
		return next()
	}

	key, rate, epoch := tb.ThroughputBudget(dbID, collID)
	if rate <= 0 {
		return next()
	}

	if wait := h.meter.admit(key, rate, epoch, h.now()); wait > 0 {
		return h.writeThrottled(c, wait)
	}

	err := next()
	h.meter.charge(key, rate, epoch, h.now(), chargeOf(c))

	return err
}

func (h *Handler) writeThrottled(c *echo.Context, wait time.Duration) error {
	hdr := c.Response().Header()
	hdr.Set(headerRetryAfterMS, strconv.FormatInt(wait.Milliseconds(), 10))
	hdr.Set(headerSubStatus, subStatusThrottled)
	hdr.Set(headerRequestCharge, "0")

	return h.writeError(c, http.StatusTooManyRequests, "TooManyRequests", sprintfThrottled(wait))
}

func sprintfThrottled(wait time.Duration) string {
	return fmt.Sprintf(throttledMessage, wait.Milliseconds())
}
