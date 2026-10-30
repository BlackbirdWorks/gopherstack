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
	// subStatusPKRangeGone is Microsoft.Azure.Cosmos SubStatusCodes.PartitionKeyRangeGone.
	subStatusPKRangeGone = "1002"
	burstCeilingRUs      = 3000.0
	burstWindowSeconds   = 300.0
	msPerSecond          = 1000.0
	throttledMessage     = "Request rate is large. More Request Units may be needed, so no changes were made. " +
		"Please retry after %d ms."
)

// ruBucket is a one-second token bucket of request units for one physical partition; tokens may go negative (a
// request that overdraws is served, the debt is repaid by refill before the next one is admitted), as the service
// does. idle is unused capacity banked for burst; burstTokens limits how fast it can be spent.
type ruBucket struct {
	last        time.Time
	tokens      float64
	rate        float64
	idle        float64
	burstTokens float64
}

// meterSlot addresses the bucket of one physical partition.
type meterSlot struct {
	key    string
	rate   float64
	weight float64
	burst  bool
}

// ruMeter meters request units per physical partition on an injected clock.
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

// sync drops every bucket when the offers changed; the lock must be held.
func (m *ruMeter) sync(epoch int64) {
	if epoch != m.epoch {
		m.epoch = epoch
		m.buckets = make(map[string]*ruBucket)
	}
}

func burstAllowance(rate float64) float64 { return math.Max(0, burstCeilingRUs-rate) }

// refill advances the bucket to now, banking overflow as idle capacity when burst is on.
func (b *ruBucket) refill(now time.Time, burst bool) {
	dt := now.Sub(b.last).Seconds()
	b.last = now
	b.tokens += b.rate * dt

	if over := b.tokens - b.rate; over > 0 {
		b.tokens = b.rate
		if burst {
			b.idle = math.Min(b.idle+over, b.rate*burstWindowSeconds)
		}
	}

	allow := burstAllowance(b.rate)
	b.burstTokens = math.Min(allow, b.burstTokens+allow*dt)
}

// spend charges ru and, when burst is on, settles any overdraft from banked idle capacity.
func (b *ruBucket) spend(ru float64, burst bool) {
	b.tokens -= ru
	if !burst || b.tokens >= 0 {
		return
	}

	d := math.Min(math.Min(b.idle, b.burstTokens), -b.tokens)
	b.tokens += d
	b.idle -= d
	b.burstTokens -= d
}

// bucket returns the refilled bucket of slot; the lock must be held.
func (m *ruMeter) bucket(s meterSlot, now time.Time) *ruBucket {
	b, ok := m.buckets[s.key]
	if !ok || b.rate != s.rate {
		b = &ruBucket{tokens: s.rate, rate: s.rate, last: now, burstTokens: burstAllowance(s.rate)}
		m.buckets[s.key] = b

		return b
	}

	b.refill(now, s.burst)

	return b
}

// admit returns how long the caller must wait before every slot is out of debt; 0 admits the request.
func (m *ruMeter) admit(epoch int64, now time.Time, slots []meterSlot) time.Duration {
	m.mu.Lock("admit")
	defer m.mu.Unlock()

	m.sync(epoch)

	var wait time.Duration

	for _, s := range slots {
		b := m.bucket(s, now)
		if b.tokens >= 0 {
			continue
		}

		w := time.Duration(math.Ceil(-b.tokens/s.rate*msPerSecond)) * time.Millisecond
		wait = max(wait, w, time.Millisecond)
	}

	return wait
}

// charge splits ru across the partitions the request touched, by the work each did.
func (m *ruMeter) charge(epoch int64, now time.Time, slots []meterSlot, ru float64) {
	m.mu.Lock("charge")
	defer m.mu.Unlock()

	m.sync(epoch)

	var total float64
	for _, s := range slots {
		total += s.weight
	}

	for _, s := range slots {
		share := ru / float64(len(slots))
		if total > 0 {
			share = ru * s.weight / total
		}

		m.bucket(s, now).spend(share, s.burst)
	}
}

// isMetered reports whether a resource kind consumes container request units.
func isMetered(kind resourceKind) bool {
	if _, _, ok := scriptKindOf(kind); ok {
		return true
	}

	return kind == resourceDocuments || kind == resourceDocumentItem
}

// metadataRUs is the throughput an account has for metadata operations; it cannot be raised (Learn, "Service
// quotas and default limits": maximum throughput supported by an account for metadata operations, 240 RU/s).
const metadataRUs = 240.0

// isMetadata reports whether a resource kind is served from the account's metadata partition: databases,
// containers, offers, users, permissions, scripts and the partition key ranges.
func isMetadata(kind resourceKind) bool {
	return kind != resourceInvalid && kind != resourceAccountRoot && !isMetered(kind)
}

// serveMetadata runs next unless the metadata partition is overdrawn, then charges what the response reports.
func (h *Handler) serveMetadata(c *echo.Context, next func() error) error {
	var epoch int64
	if tb, ok := h.Backend.(ThroughputBackend); ok {
		epoch = tb.ThroughputBudget("", "").Epoch
	}

	slots := []meterSlot{{key: "#metadata", rate: metadataRUs, weight: 1}}
	if wait := h.meter.admit(epoch, h.now(), slots); wait > 0 {
		return h.writeThrottled(c, wait)
	}

	err := next()
	h.meter.charge(epoch, h.now(), slots, chargeOf(c))

	return err
}

func chargeOf(c *echo.Context) float64 {
	ru, err := strconv.ParseFloat(c.Response().Header().Get(headerRequestCharge), 64)
	if err != nil {
		return 0
	}

	return ru
}

// serveMetered runs next unless a touched partition is overdrawn, then charges what the response reports.
func (h *Handler) serveMetered(c *echo.Context, dbID, collID string, next func() error) error {
	tb, ok := h.Backend.(ThroughputBackend)
	if !ok {
		return next()
	}

	bud := tb.ThroughputBudget(dbID, collID)
	if bud.Rate <= 0 {
		return next()
	}

	slots := h.slotsFor(c.Request(), bud)
	if wait := h.meter.admit(bud.Epoch, h.now(), slots); wait > 0 {
		return h.writeThrottled(c, wait)
	}

	err := next()

	if len(slots) > 1 {
		if shares := tb.RangeShares(dbID, collID); len(shares) == len(slots) {
			for i := range slots {
				slots[i].weight = shares[i]
			}
		}
	}

	h.meter.charge(bud.Epoch, h.now(), slots, chargeOf(c))

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
