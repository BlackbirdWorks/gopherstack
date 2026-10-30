package gcpapigateway

import (
	"slices"
	"time"
)

type quotaCounter struct {
	Window int64
	Used   int64
}

type quotaDenial struct {
	metric string
	limit  string
}

// consume charges a request's metric costs against the per-minute project limits, all or nothing. missing reports
// a cost metric the API's latest config no longer declares.
func (b *InMemoryBackend) consume(sv *served, rt *route) (*quotaDenial, bool) {
	if len(rt.Costs) == 0 {
		return nil, false
	}

	for m := range rt.Costs {
		if !sv.metrics[m] {
			return nil, true
		}
	}

	project := sv.project

	b.mu.Lock("consume")
	defer b.mu.Unlock()

	window := b.now().Truncate(time.Minute).Unix()

	metrics := make([]string, 0, len(rt.Costs))
	for m := range rt.Costs {
		if _, limited := sv.limits[m]; limited {
			metrics = append(metrics, m)
		}
	}

	slices.Sort(metrics)

	counter := func(m string) *quotaCounter {
		key := project + "|" + m

		c, ok := b.quota[key]
		if !ok || c.Window != window {
			c = &quotaCounter{Window: window}
			b.quota[key] = c
		}

		return c
	}

	for _, m := range metrics {
		if counter(m).Used+rt.Costs[m] > sv.limits[m].PerMin {
			return &quotaDenial{metric: m, limit: sv.limits[m].Name}, false
		}
	}

	for _, m := range metrics {
		counter(m).Used += rt.Costs[m]
	}

	return nil, false
}
