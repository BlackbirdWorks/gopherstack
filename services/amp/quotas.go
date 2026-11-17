package amp

import (
	"fmt"
	"net/http"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/restroute"
	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

// IngestionQuotas are the opt-in per-workspace ingestion limits.
type IngestionQuotas struct {
	Rate         int
	Burst        int
	ActiveSeries int
	Enforce      bool
}

func quotasFromSettings(s Settings) IngestionQuotas {
	return IngestionQuotas{
		Rate: s.IngestionRate, Burst: s.IngestionBurst, ActiveSeries: s.ActiveSeries,
		Enforce: s.EnforceIngestionQuotas,
	}
}

// SetIngestionQuotas replaces the ingestion limits; token buckets restart full.
func (b *InMemoryBackend) SetIngestionQuotas(q IngestionQuotas) {
	b.mu.Lock("SetIngestionQuotas")
	defer b.mu.Unlock()

	b.quotas = q
	b.buckets = map[string]*tokenBucket{}
}

type tokenBucket struct {
	last   time.Time
	tokens float64
}

func (q IngestionQuotas) capacity() float64 {
	if q.Burst > 0 {
		return float64(q.Burst)
	}

	return float64(q.Rate)
}

// takeLocked refills the workspace's bucket on the backend clock and removes n tokens, or none if short:
// a request is never partially accepted (AMP_quotas.html, Ingestion throttling).
func (b *InMemoryBackend) takeLocked(wsID string, n int, now time.Time) bool {
	q := b.quotas
	bk := b.buckets[wsID]

	if bk == nil {
		bk = &tokenBucket{tokens: q.capacity(), last: now}
		b.buckets[wsID] = bk
	}

	if el := now.Sub(bk.last).Seconds(); el > 0 {
		bk.tokens = min(q.capacity(), bk.tokens+el*float64(q.Rate))
	}

	bk.last = now

	if float64(n) > bk.tokens {
		return false
	}

	bk.tokens -= float64(n)

	return true
}

func sampleCount(series []writeSeries) int {
	n := 0
	for _, s := range series {
		n += len(s.Samples)
	}

	return n
}

// errRateLimited is the documented 429 for an exhausted ingestion bucket (AMP-troubleshooting.html).
func (q IngestionQuotas) errRateLimited(samples int) error {
	return restroute.NewError(http.StatusTooManyRequests, "ThrottlingException",
		"ingestion rate limit (%v) exceeded while adding %d samples and 0 metadata", float64(q.Rate), samples)
}

func (q IngestionQuotas) seriesLimitMessage() string {
	return fmt.Sprintf("per-user series limit of %d exceeded", q.ActiveSeries)
}

// activeSeriesLocked counts the workspace's series with a sample in the last two hours.
func (b *InMemoryBackend) activeSeriesLocked(wsID string, now time.Time) int {
	cutoff := now.UnixMilli() - activeSeriesWindowMs
	n := 0

	for _, rec := range b.seriesByWS.Get(wsID) {
		if k := len(rec.Samples); k > 0 && rec.Samples[k-1].T >= cutoff {
			n++
		}
	}

	return n
}

// validateLabelLimits enforces the documented hard label limits (AMP_quotas.html, Labels per metric series
// and Label size).
func validateLabelLimits(l promql.Labels) error {
	if len(l) > maxLabelsPerSeries {
		return restroute.Validation("series has too many labels (actual: %d, limit: %d)", len(l), maxLabelsPerSeries)
	}

	size := 0
	for k, v := range l {
		size += len(k) + len(v)
	}

	if size > maxLabelBytesPerSeries {
		return restroute.Validation("series labels total %d bytes, over the %d byte limit",
			size, maxLabelBytesPerSeries)
	}

	return nil
}
