package amp

import (
	"context"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/services/amp/promql"
)

type exemplarJSON struct {
	Labels    promql.Labels `json:"labels"`
	Value     string        `json:"value"`
	Timestamp float64       `json:"timestamp"`
}

type exemplarSeriesJSON struct {
	SeriesLabels promql.Labels  `json:"seriesLabels"`
	Exemplars    []exemplarJSON `json:"exemplars"`
}

// exemplarsData answers GET/POST query_exemplars: ingested exemplars of series selected by query in [start, end].
func (h *Handler) exemplarsData(ctx context.Context, wsID string, vals url.Values) (any, error) {
	q := vals.Get("query")
	if q == "" {
		return nil, promParamError("invalid parameter \"query\": query is required")
	}

	expr, err := promql.Parse(q)
	if err != nil {
		return nil, promParamError("invalid parameter \"query\": " + err.Error())
	}

	start, err := parseTime(vals.Get("start"), time.Unix(0, 0).UTC())
	if err != nil {
		return nil, promParamError("invalid parameter \"start\": " + err.Error())
	}

	end, err := parseTime(vals.Get("end"), h.Backend.now())
	if err != nil {
		return nil, promParamError("invalid parameter \"end\": " + err.Error())
	}

	return h.Backend.queryExemplars(ctx, wsID, promql.Selectors(expr), start.UnixMilli(), end.UnixMilli())
}

func (b *InMemoryBackend) queryExemplars(
	ctx context.Context, wsID string, selectors [][]*promql.Matcher, minT, maxT int64,
) ([]exemplarSeriesJSON, error) {
	b.mu.RLock("QueryExemplars")
	defer b.mu.RUnlock()

	if err := b.dataWorkspaceLocked(b.scopeOf(ctx), wsID); err != nil {
		return nil, err
	}

	w, _ := b.workspaces.Get(wsID)
	minT = max(minT, b.retentionCutoffMs(w))
	out := []exemplarSeriesJSON{}
	seen := map[string]bool{}

	for _, rec := range b.seriesByWS.Get(wsID) {
		if seen[rec.Key] || !matchesAny(rec.Labels, selectors) {
			continue
		}

		seen[rec.Key] = true

		var exs []exemplarJSON

		for _, ex := range rec.Exemplars {
			if ex.T >= minT && ex.T <= maxT {
				exs = append(exs, exemplarJSON{Labels: ex.Labels, Value: formatValue(ex.V), Timestamp: seconds(ex.T)})
			}
		}

		if len(exs) > 0 {
			out = append(out, exemplarSeriesJSON{SeriesLabels: rec.Labels, Exemplars: exs})
		}
	}

	sortExemplarSeries(out)

	return out, nil
}

func matchesAny(l promql.Labels, selectors [][]*promql.Matcher) bool {
	for _, ms := range selectors {
		if promql.MatchesAll(l, ms) {
			return true
		}
	}

	return false
}

func sortExemplarSeries(s []exemplarSeriesJSON) {
	slices.SortFunc(s, func(a, b exemplarSeriesJSON) int {
		return strings.Compare(promql.Fingerprint(a.SeriesLabels), promql.Fingerprint(b.SeriesLabels))
	})
}
