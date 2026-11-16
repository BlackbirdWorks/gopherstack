package azurefile

import (
	"context"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/logger"
)

const (
	hourTable        = "$MetricsHourPrimaryTransactionsFile"
	minuteTable      = "$MetricsMinutePrimaryTransactionsFile"
	hourKeyLayout    = "20060102T1500"
	minuteKeyLayout  = "20060102T1504"
	percent          = 100.0
	msPerSecond      = 1000.0
	statusSuccess    = "Success"
	statusAuthz      = "AuthorizationError"
	statusClientErr  = "ClientOtherError"
	statusServerErr  = "ServerOtherError"
	statusThrottling = "ThrottlingError"
	statusTimeout    = "ServerTimeoutError"
	accessUser       = "user"
	allAPIs          = "All"
)

// MetricsSink stores one Storage Analytics transactions row; the row replaces any earlier row with the same keys.
type MetricsSink interface {
	PutMetricRow(table, partitionKey, rowKey string, props map[string]any) error
}

func metricStatuses() []string {
	return []string{
		"Success", "ThrottlingError", "ClientTimeoutError", "ServerTimeoutError", "ClientOtherError",
		"ServerOtherError", "AuthorizationError", "NetworkError",
	}
}

// metricAgg accumulates the counters of one (bucket, row key).
type metricAgg struct {
	counts      map[string]int64
	ingress     int64
	egress      int64
	latencyMS   float64
	latencyReqs int64
}

type metricKey struct{ table, pk, rk string }

// txn describes one finished request.
type txn struct {
	status  string
	api     string
	acct    string
	ingress int64
	egress  int64
	latency time.Duration
}

// txnStatus maps an HTTP status and authentication mode to a Storage Analytics status message.
func txnStatus(code int, auth string) string {
	base := statusSuccess

	switch {
	case code == http.StatusNotModified || code < http.StatusBadRequest:
	case code == http.StatusUnauthorized || code == http.StatusForbidden:
		base = statusAuthz
	case code == http.StatusTooManyRequests:
		base = statusThrottling
	case code == http.StatusRequestTimeout:
		base = "ClientTimeoutError"
	case code == http.StatusGatewayTimeout:
		base = statusTimeout
	case code >= http.StatusInternalServerError:
		base = statusServerErr
	default:
		base = statusClientErr
	}

	return auth + base
}

func requestAuth(r *http.Request) string {
	switch {
	case r.Header.Get("Authorization") != "":
		return ""
	case r.URL.Query().Get("sig") != "":
		return "SAS"
	}

	return "Anonymous"
}

func (a *metricAgg) add(t txn) {
	a.counts[t.status]++
	a.ingress += t.ingress
	a.egress += t.egress

	if strings.HasSuffix(t.status, statusSuccess) {
		a.latencyMS += float64(t.latency) / float64(time.Millisecond)
		a.latencyReqs++
	}
}

func baseStatus(s string) string {
	return strings.TrimPrefix(strings.TrimPrefix(s, "Anonymous"), "SAS")
}

func (a *metricAgg) row() map[string]any {
	var total, billable, applicable int64

	byBase := map[string]int64{}

	for s, n := range a.counts {
		b := baseStatus(s)
		total += n
		byBase[b] += n

		if b != statusThrottling {
			applicable += n
		}

		if b != statusThrottling && b != statusServerErr && b != statusTimeout {
			billable += n
		}
	}

	row := map[string]any{
		"TotalIngress": a.ingress, "TotalEgress": a.egress, "TotalRequests": total,
		"TotalBillableRequests": billable,
	}

	row["Availability"] = ratio(billable, applicable)
	row["PercentSuccess"] = ratio(byBase[statusSuccess], total)
	row["PercentThrottlingError"] = ratio(byBase[statusThrottling], total)
	row["PercentTimeoutError"] = ratio(byBase["ClientTimeoutError"]+byBase[statusTimeout], total)
	row["PercentServerOtherError"] = ratio(byBase[statusServerErr], total)
	row["PercentClientOtherError"] = ratio(byBase[statusClientErr], total)
	row["PercentAuthorizationError"] = ratio(byBase[statusAuthz], total)
	row["PercentNetworkError"] = ratio(byBase["NetworkError"], total)

	var avg float64
	if a.latencyReqs > 0 {
		avg = a.latencyMS / float64(a.latencyReqs)
	}

	row["AverageE2ELatency"], row["AverageServerLatency"] = avg, avg

	for _, s := range metricStatuses() {
		for _, prefix := range []string{"", "Anonymous", "SAS"} {
			row[prefix+s] = a.counts[prefix+s]
		}
	}

	return row
}

func ratio(n, d int64) float64 {
	if d == 0 {
		return 0
	}

	return float64(n) / float64(d) * percent
}

// metricsBuckets lists the (table, partition key, include-APIs) targets enabled for acct at now.
type metricsTarget struct {
	table, pk string
	apis      bool
}

func (b *InMemoryBackend) metricsTargets(acct string, now time.Time) []metricsTarget {
	b.mu.RLock("MetricsConfig")
	defer b.mu.RUnlock()

	a := b.peek(acct)
	if a == nil {
		return nil
	}

	p := a.props()

	var out []metricsTarget

	if m := p.HourMetrics; m != nil && m.Enabled {
		out = append(out, metricsTarget{hourTable, now.Format(hourKeyLayout), m.IncludeAPIs != nil && *m.IncludeAPIs})
	}

	if m := p.MinuteMetrics; m != nil && m.Enabled {
		out = append(
			out,
			metricsTarget{minuteTable, now.Format(minuteKeyLayout), m.IncludeAPIs != nil && *m.IncludeAPIs},
		)
	}

	return out
}

// recordMetrics folds a finished request into the enabled metrics tables; sink errors are logged and dropped.
func (h *Handler) recordMetrics(ctx context.Context, t txn, now time.Time) {
	if h.Metrics == nil {
		return
	}

	for _, tg := range h.Backend.metricsTargets(t.acct, now) {
		keys := []string{accessUser + ";" + allAPIs}
		if tg.apis {
			keys = append(keys, fmt.Sprintf("%s;%s", accessUser, t.api))
		}

		for _, rk := range keys {
			h.putMetric(ctx, metricKey{tg.table, tg.pk, rk}, t)
		}
	}
}

func (h *Handler) putMetric(ctx context.Context, k metricKey, t txn) {
	h.metricsMu.Lock("PutMetric")
	agg := h.metricAggs[k]

	if agg == nil {
		if h.metricAggs == nil {
			h.metricAggs = map[metricKey]*metricAgg{}
		}

		agg = &metricAgg{counts: map[string]int64{}}
		h.metricAggs[k] = agg
	}

	agg.add(t)
	row := agg.row()
	h.metricsMu.Unlock()

	if err := h.Metrics.PutMetricRow(k.table, k.pk, k.rk, row); err != nil {
		logger.Load(ctx).WarnContext(ctx, "azurefile: metrics row not stored", "table", k.table, "error", err)
	}
}

func requestTxn(rc *reqCtx, api string, rs *resp, took time.Duration) txn {
	egress := int64(len(rs.body))
	if rs.stream != nil {
		if n, err := strconv.ParseInt(rs.hdr.Get("Content-Length"), 10, 64); err == nil {
			egress = n
		}
	}

	return txn{
		status: txnStatus(rs.status, requestAuth(rc.r)), api: api, acct: rc.acct,
		ingress: max(rc.r.ContentLength, 0), egress: egress, latency: took,
	}
}
