package azuresearch

import (
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
)

type facetSpec struct {
	field    string
	sortBy   string
	interval string
	values   []float64
	count    int
}

const defaultFacetCount = 10

func parseFacet(expr string) (facetSpec, error) {
	parts := strings.Split(expr, ",")
	fs := facetSpec{field: strings.TrimSpace(parts[0]), count: defaultFacetCount, sortBy: "-count"}

	for _, p := range parts[1:] {
		k, v, ok := strings.Cut(strings.TrimSpace(p), ":")
		if !ok {
			return fs, badf("Invalid facet expression '%s'.", expr)
		}

		if err := fs.set(k, v); err != nil {
			return fs, err
		}
	}

	return fs, nil
}

func (fs *facetSpec) set(k, v string) error {
	switch k {
	case fieldCount:
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return badf("Invalid facet count '%s'.", v)
		}

		fs.count = n
	case "sort":
		if !containsStr([]string{fieldCount, "-count", fieldValue, "-value"}, v) {
			return badf("Invalid facet sort '%s'.", v)
		}

		fs.sortBy = v
	case "interval":
		fs.interval = v
	case "values":
		for x := range strings.SplitSeq(v, "|") {
			f, err := strconv.ParseFloat(x, 64)
			if err != nil {
				return badf("Invalid facet values '%s'.", v)
			}

			fs.values = append(fs.values, f)
		}

		sort.Float64s(fs.values)
	default:
		return badf("Unsupported facet option '%s'.", k)
	}

	return nil
}

func computeFacets(c *compiled, exprs []string, hits []hit) (map[string][]map[string]any, error) {
	if len(exprs) == 0 {
		return nil, nil //nolint:nilnil // no facets requested
	}

	out := map[string][]map[string]any{}

	for _, e := range exprs {
		fs, err := parseFacet(e)
		if err != nil {
			return nil, err
		}

		fd, ok := c.byPath[fs.field]
		if !ok || !fd.Facetable {
			return nil, badf("The field '%s' is not facetable.", fs.field)
		}

		out[fs.field] = facetField(fd, fs, hits)
	}

	return out, nil
}

func facetField(fd *fieldDef, fs facetSpec, hits []hit) []map[string]any {
	segs := strings.Split(fd.Path, "/")
	buckets := map[string]int{}
	order := map[string]any{}

	for _, h := range hits {
		seen := map[string]bool{}

		for _, v := range valuesAt(h.doc.Fields, segs) {
			key, rep, ok := facetBucket(fd, fs, v)
			if !ok || seen[key] {
				continue
			}

			seen[key] = true
			buckets[key]++
			order[key] = rep
		}
	}

	if fs.interval != "" || len(fs.values) > 0 {
		return rangeFacetResult(buckets, order)
	}

	return valueFacetResult(buckets, order, fs)
}

type rangeKey struct {
	from, to any
}

func facetBucket(fd *fieldDef, fs facetSpec, v any) (string, any, bool) {
	switch {
	case len(fs.values) > 0:
		return valuesBucket(v, fs.values)
	case fs.interval != "" && fd.Base == typeDate:
		return dateBucket(v, fs.interval)
	case fs.interval != "":
		return numberBucket(v, fs.interval)
	}

	return fmt.Sprint(v), v, v != nil
}

func numberBucket(v any, interval string) (string, any, bool) {
	f, ok := asFloat(v)
	step, err := strconv.ParseFloat(interval, 64)

	if !ok || err != nil || step <= 0 {
		return "", nil, false
	}

	from := math.Floor(f/step) * step

	return fmtNum(from), rangeKey{from: from, to: from + step}, true
}

func valuesBucket(v any, bounds []float64) (string, any, bool) {
	f, ok := asFloat(v)
	if !ok {
		return "", nil, false
	}

	i := sort.Search(len(bounds), func(i int) bool { return bounds[i] > f })

	rk := rangeKey{}
	if i > 0 {
		rk.from = bounds[i-1]
	}

	if i < len(bounds) {
		rk.to = bounds[i]
	}

	return strconv.Itoa(i), rk, true
}

func dateBucket(v any, unit string) (string, any, bool) {
	t, ok := parseDate(v)
	if !ok {
		return "", nil, false
	}

	var from, to time.Time

	switch unit {
	case "year":
		from = time.Date(t.Year(), 1, 1, 0, 0, 0, 0, time.UTC)
		to = from.AddDate(1, 0, 0)
	case "quarter":
		from = time.Date(
			t.Year(),
			time.Month((int(t.Month())-1)/monthsPerQuarter*monthsPerQuarter+1),
			1,
			0,
			0,
			0,
			0,
			time.UTC,
		)
		to = from.AddDate(0, monthsPerQuarter, 0)
	case "month":
		from = time.Date(t.Year(), t.Month(), 1, 0, 0, 0, 0, time.UTC)
		to = from.AddDate(0, 1, 0)
	case "week":
		from = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC).AddDate(0, 0, -int(t.Weekday()))
		to = from.AddDate(0, 0, daysPerWeek)
	case "day":
		from = time.Date(t.Year(), t.Month(), t.Day(), 0, 0, 0, 0, time.UTC)
		to = from.AddDate(0, 0, 1)
	case "hour":
		from = t.Truncate(time.Hour)
		to = from.Add(time.Hour)
	case "minute":
		from = t.Truncate(time.Minute)
		to = from.Add(time.Minute)
	case "second":
		from = t.Truncate(time.Second)
		to = from.Add(time.Second)
	default:
		return "", nil, false
	}

	return from.Format(time.RFC3339), rangeKey{from: from.Format(time.RFC3339), to: to.Format(time.RFC3339)}, true
}

func rangeFacetResult(buckets map[string]int, rep map[string]any) []map[string]any {
	type item struct {
		rk    rangeKey
		sort  float64
		count int
	}

	items := make([]item, 0, len(buckets))

	for k, n := range buckets {
		rk, _ := rep[k].(rangeKey)
		sv, _ := asFloat(rk.from)

		if s, ok := rk.from.(string); ok {
			t, _ := parseDate(s)
			sv = float64(t.Unix())
		}

		if rk.from == nil {
			sv = math.Inf(-1)
		}

		items = append(items, item{rk: rk, sort: sv, count: n})
	}

	sort.Slice(items, func(i, j int) bool { return items[i].sort < items[j].sort })

	out := make([]map[string]any, 0, len(items))

	for _, it := range items {
		m := map[string]any{fieldCount: it.count}
		if it.rk.from != nil {
			m["from"] = it.rk.from
		}

		if it.rk.to != nil {
			m["to"] = it.rk.to
		}

		out = append(out, m)
	}

	return out
}

func valueFacetResult(buckets map[string]int, rep map[string]any, fs facetSpec) []map[string]any {
	keys := make([]string, 0, len(buckets))
	for k := range buckets {
		keys = append(keys, k)
	}

	sort.Slice(keys, func(i, j int) bool {
		a, b := keys[i], keys[j]

		switch fs.sortBy {
		case fieldValue:
			return lessValue(rep[a], rep[b])
		case "-value":
			return lessValue(rep[b], rep[a])
		case fieldCount:
			if buckets[a] != buckets[b] {
				return buckets[a] < buckets[b]
			}
		default:
			if buckets[a] != buckets[b] {
				return buckets[a] > buckets[b]
			}
		}

		return lessValue(rep[a], rep[b])
	})

	if fs.count > 0 && len(keys) > fs.count {
		keys = keys[:fs.count]
	}

	out := make([]map[string]any, 0, len(keys))
	for _, k := range keys {
		out = append(out, map[string]any{fieldCount: buckets[k], fieldValue: rep[k]})
	}

	return out
}

func lessValue(a, b any) bool {
	c, ok := orderValues(a, b)
	if !ok {
		return fmt.Sprint(a) < fmt.Sprint(b)
	}

	return c < 0
}
