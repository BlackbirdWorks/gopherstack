package ociapigateway

import (
	"math"
	"net/http"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/lockmetrics"
)

type quotaCount struct {
	windowEnd time.Time
	used      int
}

// usageTracker counts requests against calendar quotas; state is in memory and per process.
type usageTracker struct {
	mu     *lockmetrics.RWMutex
	quotas map[string]*quotaCount
}

func newUsageTracker() *usageTracker {
	return &usageTracker{mu: lockmetrics.New("ociapigateway.usage"), quotas: map[string]*quotaCount{}}
}

func (u *usageTracker) reset() {
	u.mu.Lock("reset")
	u.quotas = map[string]*quotaCount{}
	u.mu.Unlock()
}

func (u *usageTracker) forgetPlan(planID string) {
	u.mu.Lock("forgetPlan")
	defer u.mu.Unlock()

	for k := range u.quotas {
		if strings.HasPrefix(k, planID+"|") {
			delete(u.quotas, k)
		}
	}
}

// windowEnd is the end of the UTC calendar period containing t.
func windowEnd(unit string, t time.Time) time.Time {
	t = t.UTC()

	switch unit {
	case unitMinute:
		return t.Truncate(time.Minute).Add(time.Minute)
	case unitHour:
		return t.Truncate(time.Hour).Add(time.Hour)
	case unitDay:
		return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
	case unitWeek:
		daysSinceMonday := (int(t.Weekday()) + 6) % 7 //nolint:mnd // Monday-based week

		return time.Date(t.Year(), t.Month(), t.Day()-daysSinceMonday+7, 0, 0, 0, 0, time.UTC) //nolint:mnd // days per week
	default:
		return time.Date(t.Year(), t.Month()+1, 1, 0, 0, 0, 0, time.UTC)
	}
}

// take counts one request; when over the quota it reports the seconds until the window resets.
func (u *usageTracker) take(key string, q *Quota, now time.Time) (bool, int) {
	u.mu.Lock("take")
	defer u.mu.Unlock()

	c := u.quotas[key]
	if c == nil || !c.windowEnd.After(now) {
		c = &quotaCount{windowEnd: windowEnd(q.Unit, now)}
		u.quotas[key] = c
	}

	if c.used >= q.Value {
		return false, max(1, int(math.Ceil(c.windowEnd.Sub(now).Seconds())))
	}

	c.used++

	return true, 0
}

func (u *usageTracker) refund(key string) {
	u.mu.Lock("refund")
	defer u.mu.Unlock()

	if c := u.quotas[key]; c != nil && c.used > 0 {
		c.used--
	}
}

type appliedEntitlement struct {
	key   string
	rate  *RateLimit
	quota *Quota
}

// usageVerdict is the outcome of usage-plan enforcement; a zero status admits the request.
type usageVerdict struct {
	refund []string
	msg    string
	status int
	retry  int
}

func clientTokenOf(r *http.Request, locations []string) string {
	for _, loc := range locations {
		kind, name, ok := parseVariable(loc)
		if !ok {
			continue
		}

		var v string

		switch kind {
		case locationHeader:
			v = r.Header.Get(name)
		case locationQuery:
			v = r.URL.Query().Get(name)
		}

		if v != "" {
			return v
		}
	}

	return ""
}

// entitlementsFor finds the subscriber owning the token and the entitlements that target the deployment.
func (b *InMemoryBackend) entitlementsFor(token, depID string) ([]appliedEntitlement, bool) {
	b.mu.RLock("entitlementsFor")
	defer b.mu.RUnlock()

	for _, s := range b.subscribers {
		if !servable(s.State) {
			continue
		}

		owns := false

		for _, c := range s.Clients {
			owns = owns || c.Token == token
		}

		if !owns {
			continue
		}

		var out []appliedEntitlement

		for _, planID := range s.UsagePlans {
			p := b.usagePlans[planID]
			if p == nil || !servable(p.State) {
				continue
			}

			for _, e := range p.Entitlements {
				if targets(e.Targets, depID) {
					out = append(out, appliedEntitlement{
						key: planID + "|" + e.Name + "|" + s.ID, rate: e.RateLimit, quota: e.Quota,
					})
				}
			}
		}

		return out, true
	}

	return nil, false
}

func targets(ts []EntitlementTarget, depID string) bool {
	for _, t := range ts {
		if t.DeploymentID == depID {
			return true
		}
	}

	return false
}

// enforceUsage applies the deployment's usagePlans policy to a request that already passed authentication.
func (b *InMemoryBackend) enforceUsage(r *http.Request, pol *usagePlansDoc, depID string) usageVerdict {
	if pol == nil {
		return usageVerdict{}
	}

	token := clientTokenOf(r, pol.TokenLocations)
	if token == "" {
		return usageVerdict{status: http.StatusUnauthorized, msg: "Unauthorized"}
	}

	ents, known := b.entitlementsFor(token, depID)
	if !known {
		return usageVerdict{status: http.StatusUnauthorized, msg: "Unauthorized"}
	}

	if len(ents) == 0 {
		return usageVerdict{status: http.StatusForbidden, msg: "Forbidden"}
	}

	now := b.clock()

	var v usageVerdict

	for _, e := range ents {
		if e.rate != nil {
			if ok, retry := b.limiter.allow("usage|"+e.key, e.rate.Value, now); !ok {
				b.refundQuota(v.refund)

				return usageVerdict{status: http.StatusTooManyRequests, msg: "Too Many Requests", retry: retry}
			}
		}

		if e.quota == nil {
			continue
		}

		ok, retry := b.usage.take(e.key, e.quota, now)

		switch {
		case ok:
			v.refund = append(v.refund, e.key)
		case e.quota.OperationOnBreach == breachReject:
			b.refundQuota(v.refund)

			return usageVerdict{status: http.StatusTooManyRequests, msg: "Too Many Requests", retry: retry}
		}
	}

	return v
}

// refundQuota gives back quota for a request the backend failed with a 5xx.
func (b *InMemoryBackend) refundQuota(keys []string) {
	for _, k := range keys {
		b.usage.refund(k)
	}
}
