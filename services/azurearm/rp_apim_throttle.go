package azurearm

import (
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	apimCRateLimitByKey = "rate-limit-by-key"
	apimCQuota          = "quota"
	apimCQuotaByKey     = "quota-by-key"
	apimCCounterKey     = "counter-key"
	apimCIncCondition   = "increment-condition"
	apimCIncCount       = "increment-count"
	apimCBandwidth      = "bandwidth"
	apimCFirstPeriod    = "first-period-start"
	apimMinKeyedRenewal = 300
	apimQuotaTimeFmt    = "2006-01-02T15:04:05Z"
	apimSecsPerDay      = 86400
	apimSecsPerHour     = 3600
	apimSecsPerMin      = 60
)

func apimQuotaMessage(kind string, left time.Duration) string {
	what := "call volume"
	if kind == apimCBandwidth {
		what = "bandwidth"
	}

	msg := "Out of " + what + " quota."
	if left <= 0 {
		return msg
	}

	secs := int64(math.Ceil(left.Seconds()))
	d, rest := secs/apimSecsPerDay, secs%apimSecsPerDay
	h, m, s := rest/apimSecsPerHour, rest%apimSecsPerHour/apimSecsPerMin, rest%apimSecsPerMin

	if d > 0 {
		return fmt.Sprintf("%s Quota will be replenished in %d.%02d:%02d:%02d.", msg, d, h, m, s)
	}

	return fmt.Sprintf("%s Quota will be replenished in %02d:%02d:%02d.", msg, h, m, s)
}

func apimIsExprAttr(n *polNode, attr string) bool {
	_, ok, _ := apimIsExprText(n.Attrs[attr])

	return ok
}

func (g *gwState) keyAttr(n *polNode) (string, bool) {
	key := g.sub(n.Attrs[apimCCounterKey])
	if g.evalErr != nil {
		return "", false
	}

	if len(key) > apimMaxCounterKey {
		g.evalErr = apimExprFail("the counter-key is longer than %d characters", apimMaxCounterKey)

		return "", false
	}

	return key, true
}

func (g *gwState) rateLimitByKey(n *polNode) {
	calls, period := g.intAttr(n, apimCCalls, 0), g.intAttr(n, apimCRenewalPeriod, 0)

	key, ok := g.keyAttr(n)
	if !ok {
		return
	}

	if calls < 1 || calls > math.MaxInt32 || period < 1 || period > apimMaxRenewalSecs {
		g.evalErr = apimExprFail("calls must be 1 to %d and renewal-period 1 to %d", math.MaxInt32, apimMaxRenewalSecs)

		return
	}

	full, per := apimKeyOf(g.plan.svcKey, "rlbk", key), time.Duration(period)*time.Second
	allowed, left, retry := g.rateCheck(n, full, calls, per)

	g.rateHeaders(n, calls, left)

	if allowed {
		return
	}

	secs := strconv.Itoa(int(math.Ceil(retry.Seconds())))

	g.failR(http.StatusTooManyRequests, "RateLimitExceeded", apimMsgRateLimit)
	g.respHeader.Set(firstNonEmpty(n.Attrs["retry-after-header-name"], apimCRetryAfter), secs)

	if v := n.Attrs["retry-after-variable-name"]; v != "" {
		g.vars[v] = int64(math.Ceil(retry.Seconds()))
	}
}

// rateCheck counts the call now, or only checks it and counts at the end of the pipeline when the increment
// depends on the response.
func (g *gwState) rateCheck(n *polNode, key string, calls int64, per time.Duration) (bool, int, time.Duration) {
	switch {
	case apimIsExprAttr(n, apimCIncCondition) || apimIsExprAttr(n, apimCIncCount):
		allowed, left, retry := g.p.limiter.peek(key, int(calls), per, g.now())

		g.deferred = append(g.deferred, func(int64) {
			cond, cnt := g.boolAttr(n, apimCIncCondition, true), g.intAttr(n, apimCIncCount, 1)
			if cond && cnt > 0 && cnt <= calls && g.evalErr == nil {
				g.p.limiter.add(key, int(cnt), per, g.now())
			}

			g.evalErr = nil
		})

		return allowed, left, retry
	case g.boolAttr(n, apimCIncCondition, true):
		cnt := min(g.intAttr(n, apimCIncCount, 1), calls)

		return g.p.limiter.take(key, int(calls), int(max(cnt, 1)), per, g.now())
	}

	return g.p.limiter.peek(key, int(calls), per, g.now())
}

func (g *gwState) rateHeaders(n *polNode, calls int64, left int) {
	if h := n.Attrs["total-calls-header-name"]; h != "" {
		g.extra.Set(h, strconv.FormatInt(calls, 10))
	}

	if h := n.Attrs["remaining-calls-header-name"]; h != "" {
		g.extra.Set(h, strconv.Itoa(left))
	}

	if v := n.Attrs["remaining-calls-variable-name"]; v != "" {
		g.vars[v] = int64(left)
	}
}

func (g *gwState) quotaDeny(kind string, retry time.Duration) {
	g.failR(http.StatusForbidden, "QuotaExceeded", apimQuotaMessage(kind, retry))

	if retry > 0 {
		g.respHeader.Set(apimCRetryAfter, strconv.Itoa(int(math.Ceil(retry.Seconds()))))
	}
}

func (g *gwState) requestBytes() int64 { return max(g.r.ContentLength, 0) }

func (g *gwState) quotaByKey(n *polNode) {
	key, ok := g.keyAttr(n)
	if !ok {
		return
	}

	calls, kb, period := g.intAttr(
		n,
		apimCCalls,
		0,
	), g.intAttr(
		n,
		apimCBandwidth,
		0,
	), g.intAttr(
		n,
		apimCRenewalPeriod,
		0,
	)
	first := time.Time{}

	if v := n.Attrs[apimCFirstPeriod]; v != "" {
		t, err := time.Parse(apimQuotaTimeFmt, v)
		if err != nil {
			g.evalErr = apimExprFail("first-period-start is not a yyyy-MM-ddTHH:mm:ssZ date")

			return
		}

		first = t
	}

	if g.evalErr != nil || period < 0 || (period > 0 && period < apimMinKeyedRenewal) || calls < 0 || kb < 0 {
		g.evalErr = apimExprFail("renewal-period must be 0 or at least %d seconds", apimMinKeyedRenewal)

		return
	}

	full, per := apimKeyOf(g.plan.svcKey, "qbk", key), time.Duration(period)*time.Second

	if kind, retry := g.p.quotas.exceeded(full, g.now(), first, per, calls, kb); kind != "" {
		g.quotaDeny(kind, retry)

		return
	}

	g.deferred = append(g.deferred, func(written int64) {
		cond, cnt := g.boolAttr(n, apimCIncCondition, true), g.intAttr(n, apimCIncCount, 1)

		defer func() { g.evalErr = nil }()

		if !cond || cnt < 1 || g.evalErr != nil || g.seenKeys[full] {
			return
		}

		g.seenKeys[full] = true
		g.p.quotas.add(full, g.now(), first, per, cnt, written+g.requestBytes())
	})
}

type gwQuotaScope struct {
	n   *polNode
	key string
}

func (g *gwState) quota(n *polNode) {
	if g.plan.subID == "" {
		return
	}

	scopes := []gwQuotaScope{{n: n, key: apimScopeProduct}}

	for _, a := range n.Kids {
		if a.Name != apimCAPIElem || !g.quotaTargets(a, g.plan.apiID, strOf(g.plan.api, apimCDisplayName)) {
			continue
		}

		scopes = append(scopes, gwQuotaScope{n: a, key: "api:" + a.Attrs[apimCID] + a.Attrs[fieldName]})

		for _, o := range a.Kids {
			if o.Name == apimCOperationElem && g.plan.opID != "" &&
				g.quotaTargets(o, g.plan.opID, strOf(g.plan.op, apimCDisplayName)) {
				scopes = append(
					scopes,
					gwQuotaScope{n: o, key: "op:" + g.plan.apiID + "/" + o.Attrs[apimCID] + o.Attrs[fieldName]},
				)
			}
		}
	}

	type counter struct {
		first time.Time
		key   string
		per   time.Duration
		calls int64
		kb    int64
	}

	cs := make([]counter, 0, len(scopes))

	for _, sc := range scopes {
		c := counter{
			key:   apimKeyOf(g.plan.svcKey, "quota", g.plan.subID, sc.key),
			first: g.plan.subStart,
			per:   time.Duration(g.intAttr(sc.n, apimCRenewalPeriod, 0)) * time.Second,
			calls: g.intAttr(sc.n, apimCCalls, 0), kb: g.intAttr(sc.n, apimCBandwidth, 0),
		}

		if kind, retry := g.p.quotas.exceeded(c.key, g.now(), c.first, c.per, c.calls, c.kb); kind != "" {
			g.quotaDeny(kind, retry)

			return
		}

		cs = append(cs, c)
	}

	g.deferred = append(g.deferred, func(written int64) {
		for _, c := range cs {
			g.p.quotas.add(c.key, g.now(), c.first, c.per, 1, written+g.requestBytes())
		}
	})
}

func (g *gwState) quotaTargets(e *polNode, id, name string) bool {
	if v := e.Attrs[apimCID]; v != "" {
		return strings.EqualFold(v, id)
	}

	return e.Attrs[fieldName] != "" && strings.EqualFold(e.Attrs[fieldName], name)
}
