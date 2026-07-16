package apigateway

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

// usageTracker holds the mutable data-plane state for usage-plan enforcement: a
// per-(plan,key) quota counter and a per-(plan,key) token bucket for rate/burst
// throttling. All access is serialized by the owning InMemoryBackend's mutex, so the
// tracker itself carries no lock.
type usageTracker struct {
	quota   map[string]*quotaCounter
	buckets map[string]*tokenBucket
}

func newUsageTracker() *usageTracker {
	return &usageTracker{
		quota:   make(map[string]*quotaCounter),
		buckets: make(map[string]*tokenBucket),
	}
}

// quotaCounter tracks how many requests an API key has consumed against a usage-plan
// quota within the current fixed period. periodStart is reset whenever the period
// rolls over.
type quotaCounter struct {
	periodStart time.Time
	used        int
}

// tokenBucket is a classic token-bucket rate limiter. ratePerSec tokens are added per
// second up to burst; each allowed request consumes one token.
type tokenBucket struct {
	last       time.Time
	ratePerSec float64
	burst      float64
	tokens     float64
}

func newTokenBucket(ratePerSec float64, burst int, now time.Time) *tokenBucket {
	b := float64(burst)
	if b <= 0 {
		// AWS applies a default burst when only a rate is configured.
		b = ratePerSec
	}
	if b < 1 {
		b = 1
	}

	return &tokenBucket{ratePerSec: ratePerSec, burst: b, tokens: b, last: now}
}

// allow refills the bucket based on elapsed time and consumes one token, returning
// whether the request is permitted.
func (tb *tokenBucket) allow(now time.Time) bool {
	if elapsed := now.Sub(tb.last).Seconds(); elapsed > 0 {
		tb.tokens += elapsed * tb.ratePerSec
		if tb.tokens > tb.burst {
			tb.tokens = tb.burst
		}
		tb.last = now
	}

	if tb.tokens >= 1 {
		tb.tokens--

		return true
	}

	return false
}

// usageKey composes the map key for a (plan, key) pair.
func usageKey(planID, keyID string) string {
	return planID + "\x00" + keyID
}

// AWS usage-plan quota periods, expressed as fixed durations.
const (
	quotaPeriodDay   = 24 * time.Hour
	quotaPeriodWeek  = 7 * quotaPeriodDay
	quotaPeriodMonth = 30 * quotaPeriodDay
)

// quotaPeriodDuration maps an AWS quota period to its duration.
func quotaPeriodDuration(period string) time.Duration {
	switch period {
	case "DAY":
		return quotaPeriodDay
	case "WEEK":
		return quotaPeriodWeek
	case "MONTH":
		return quotaPeriodMonth
	default:
		// Treat an unspecified period as a day (AWS requires a period on the quota).
		return quotaPeriodDay
	}
}

// effectiveThrottle returns the throttle settings that apply to a key on a stage:
// a stage-wide ("*/*") apiStage override if present, otherwise the plan default.
func effectiveThrottle(plan *UsagePlan, apiID, stageName string) *ThrottleSettings {
	for i := range plan.APIStages {
		st := plan.APIStages[i]
		if st.RestAPIID != apiID || st.Stage != stageName {
			continue
		}
		if override, ok := st.Throttle["*/*"]; ok && override != nil {
			return override
		}
	}

	return plan.Throttle
}

// enforce applies quota then throttle limits for keyID under plan on api:stage at the
// given time. It returns ErrQuotaExceeded when the period quota is exhausted,
// ErrThrottled when the rate/burst limit is hit, or nil when the request is allowed.
// A rejected request does not consume quota.
func (u *usageTracker) enforce(plan *UsagePlan, apiID, stageName, keyID string, now time.Time) error {
	mapKey := usageKey(plan.ID, keyID)

	var counter *quotaCounter
	if plan.Quota != nil && plan.Quota.Limit > 0 {
		counter = u.quotaCounterFor(mapKey, plan.Quota.Period, now)
		if counter.used+1 > plan.Quota.Limit {
			return ErrQuotaExceeded
		}
	}

	if throttle := effectiveThrottle(plan, apiID, stageName); throttle != nil && throttle.RateLimit > 0 {
		bucket, ok := u.buckets[mapKey]
		if !ok {
			bucket = newTokenBucket(throttle.RateLimit, throttle.BurstLimit, now)
			u.buckets[mapKey] = bucket
		}
		if !bucket.allow(now) {
			return ErrThrottled
		}
	}

	if counter != nil {
		counter.used++
	}

	return nil
}

// quotaCounterFor returns the counter for mapKey, creating it or rolling it over to a
// fresh period as needed.
func (u *usageTracker) quotaCounterFor(mapKey, period string, now time.Time) *quotaCounter {
	counter, ok := u.quota[mapKey]
	if !ok {
		counter = &quotaCounter{periodStart: now}
		u.quota[mapKey] = counter

		return counter
	}

	if now.Sub(counter.periodStart) >= quotaPeriodDuration(period) {
		counter.used = 0
		counter.periodStart = now
	}

	return counter
}

// usageForKey returns the consumed count and remaining quota for a key under a plan.
// The remaining value is -1 when the plan has no quota (unlimited).
func (u *usageTracker) usageForKey(plan *UsagePlan, keyID string) (int, int) {
	used := 0
	if counter, ok := u.quota[usageKey(plan.ID, keyID)]; ok {
		used = counter.used
	}

	if plan.Quota != nil && plan.Quota.Limit > 0 {
		return used, max(0, plan.Quota.Limit-used)
	}

	return used, -1
}

// GetUsage returns real per-key usage data for a usage plan. Each item is keyed
// by API key ID and contains a [used, remaining] pair reflecting the requests
// the data plane has actually metered against the plan's quota. If a key's
// remaining quota was explicitly overridden via UpdateUsage, that override
// takes precedence over the computed remaining value (mirroring AWS's
// "temporary extension to the remaining quota" semantics for UpdateUsage).
func (b *InMemoryBackend) GetUsage(input GetUsageInput) (*UsageData, error) {
	b.mu.RLock("GetUsage")
	defer b.mu.RUnlock()

	plan, ok := b.usagePlans.Get(input.UsagePlanID)
	if !ok {
		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, input.UsagePlanID)
	}

	items := make(map[string][]any)

	for _, upk := range b.usagePlanKeysByPlan.Get(input.UsagePlanID) {
		keyID := upk.ID
		used, remaining := b.usage.usageForKey(plan, keyID)
		if override, hasOverride := b.usageOverrides[input.UsagePlanID][keyID]; hasOverride {
			remaining = int(override)
		}
		items[keyID] = []any{[]int{used, remaining}}
	}

	return &UsageData{
		UsagePlanID: input.UsagePlanID,
		StartDate:   input.StartDate,
		EndDate:     input.EndDate,
		Items:       items,
	}, nil
}

// UpdateUsage validates that the usage plan and API key association exist and
// records a remaining-quota override for that key, so a subsequent GetUsage
// call reflects the change. Real AWS's only supported UpdateUsage patch path
// is the single-segment scalar "/remaining" (see patch-operations.html's
// UpdateUsage table — there is no per-date path segment, unlike the
// superficially similar per-route paths on UpdateStage/UpdateUsagePlan), so
// handler.go's applyStructuredPatch flattens the request into a single
// "remaining" -> value entry via the generic top-level fallback and this
// method only ever needs the value, not the key; patchedFields is still a map
// (rather than a single value) purely to reuse that generic flattening path.
// non-integer values are ignored. Real API Gateway quota-consumption tracking
// (based on live request traffic) isn't modeled by this emulator — as with
// GetUsage's Items, which are always empty absent real traffic — but the
// override recorded here is genuinely read back by GetUsage.
func (b *InMemoryBackend) UpdateUsage(usagePlanID, keyID string, patchedFields map[string]string) (*UsageData, error) {
	b.mu.Lock("UpdateUsage")

	if !b.usagePlans.Has(usagePlanID) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: usage plan %s not found", ErrUsagePlanNotFound, usagePlanID)
	}

	if !b.usagePlanKeys.Has(usagePlanKeyKey(usagePlanID, keyID)) {
		b.mu.Unlock()

		return nil, fmt.Errorf("%w: usage plan key %s not found", ErrUsagePlanKeyNotFound, keyID)
	}

	for _, v := range patchedFields {
		remaining, perr := strconv.ParseInt(strings.TrimSpace(v), 10, 64)
		if perr != nil {
			continue
		}

		if b.usageOverrides == nil {
			b.usageOverrides = make(map[string]map[string]int64)
		}

		if b.usageOverrides[usagePlanID] == nil {
			b.usageOverrides[usagePlanID] = make(map[string]int64)
		}

		b.usageOverrides[usagePlanID][keyID] = remaining
	}

	b.mu.Unlock()

	return b.GetUsage(GetUsageInput{UsagePlanID: usagePlanID})
}
