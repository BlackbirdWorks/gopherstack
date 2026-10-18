package s3vectors

import (
	"cmp"
	"math"
	"slices"
	"sort"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/page"
)

const (
	keyVectors = "vectors"
	opOr       = "$or"
	opAnd      = "$and"
)

type queryIn struct {
	Filter any  `json:"filter"`
	TopK   *int `json:"topK"`
	indexRef
	NextToken      string     `json:"nextToken"`
	QueryMode      string     `json:"queryMode"`
	QueryVector    vectorData `json:"queryVector"`
	ReturnDistance bool       `json:"returnDistance"`
	ReturnMetadata bool       `json:"returnMetadata"`
}

type scored struct {
	vec  *Vector
	dist float64
}

func distance(metric string, a, b []float32) float64 {
	var dot, na, nb, sq float64

	for i := range a {
		x, y := float64(a[i]), float64(b[i])
		dot += x * y
		na += x * x
		nb += y * y
		sq += (x - y) * (x - y)
	}

	if metric == "euclidean" {
		return math.Sqrt(sq)
	}

	return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
}

func (b *InMemoryBackend) queryVectors(sc scope, in *queryIn) (any, error) {
	if in.TopK == nil || *in.TopK < 1 || *in.TopK > maxTopK {
		return nil, invalid("topK must be between 1 and %d", maxTopK)
	}

	if err := page.ValidateToken(in.NextToken); err != nil {
		return nil, invalid("nextToken is not valid")
	}

	b.mu.RLock("queryVectors")
	defer b.mu.RUnlock()

	_, ix, err := b.resolveIndex(sc, in.indexRef)
	if err != nil {
		return nil, err
	}

	match, err := validateQuery(ix, in)
	if err != nil {
		return nil, err
	}

	pg := page.New(b.rank(ix, in, match), in.NextToken, *in.TopK, *in.TopK)
	out := make([]queryVector, 0, len(pg.Data))

	for _, h := range pg.Data {
		q := queryVector{Key: h.vec.Key}
		if in.ReturnDistance {
			d := float32(h.dist)
			q.Distance = &d
		}

		if in.ReturnMetadata && h.vec.Meta != nil {
			q.Metadata = h.vec.Meta
		}

		out = append(out, q)
	}

	resp := map[string]any{keyVectors: out, "distanceMetric": ix.DistanceMetric}
	if pg.Next != "" {
		resp["nextToken"] = pg.Next
	}

	return resp, nil
}

func validateQuery(ix *Index, in *queryIn) (matcher, error) {
	if err := validateVectorData(ix, in.QueryVector.Float32, "queryVector"); err != nil {
		return nil, err
	}

	if err := validateQueryMode(ix, in.QueryMode); err != nil {
		return nil, err
	}

	return compileFilter(in.Filter, ix)
}

// rank returns the vectors passing the filter, nearest first; ties break on key.
func (b *InMemoryBackend) rank(ix *Index, in *queryIn, match matcher) []scored {
	var hits []scored

	for _, v := range b.setOf(ix).Vectors {
		if match(v.Meta) {
			hits = append(hits, scored{vec: v, dist: distance(ix.DistanceMetric, in.QueryVector.Float32, v.Data)})
		}
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].dist != hits[j].dist {
			return hits[i].dist < hits[j].dist
		}

		return hits[i].vec.Key < hits[j].vec.Key
	})

	return hits
}

func validateQueryMode(ix *Index, mode string) error {
	switch mode {
	case "", ix.Mode:
		return nil
	case modeClassic:
		return invalid("CLASSIC cannot be specified for an ENHANCED vector index")
	case modeEnhanced:
		return nil
	}

	return invalid("queryMode must be CLASSIC or ENHANCED")
}

type matcher func(meta map[string]any) bool

func matchAll(map[string]any) bool { return true }

func compileFilter(f any, ix *Index) (matcher, error) {
	if f == nil {
		return matchAll, nil
	}

	obj, ok := f.(map[string]any)
	if !ok {
		return nil, invalid("filter must be a JSON object")
	}

	if len(obj) == 0 {
		return matchAll, nil
	}

	return compileObject(obj, ix)
}

func compileObject(obj map[string]any, ix *Index) (matcher, error) {
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}

	slices.Sort(keys)

	parts := make([]matcher, 0, len(keys))

	for _, k := range keys {
		var (
			m   matcher
			err error
		)

		if strings.HasPrefix(k, "$") {
			m, err = compileLogical(k, obj[k], ix)
		} else {
			m, err = compileField(k, obj[k], ix)
		}

		if err != nil {
			return nil, err
		}

		parts = append(parts, m)
	}

	return func(meta map[string]any) bool {
		for _, p := range parts {
			if !p(meta) {
				return false
			}
		}

		return true
	}, nil
}

func compileLogical(op string, v any, ix *Index) (matcher, error) {
	if op != opAnd && op != opOr {
		return nil, invalid("unsupported top-level filter operator %q", op)
	}

	list, ok := v.([]any)
	if !ok || len(list) == 0 {
		return nil, invalid("%s requires a non-empty array of filter objects", op)
	}

	subs := make([]matcher, 0, len(list))

	for _, e := range list {
		obj, isObj := e.(map[string]any)
		if !isObj || len(obj) == 0 {
			return nil, invalid("%s requires a non-empty array of filter objects", op)
		}

		m, err := compileObject(obj, ix)
		if err != nil {
			return nil, err
		}

		subs = append(subs, m)
	}

	return func(meta map[string]any) bool {
		for _, s := range subs {
			if s(meta) == (op == opOr) {
				return op == opOr
			}
		}

		return op == opAnd
	}, nil
}

func isPrimitive(v any) bool {
	switch v.(type) {
	case string, float64, bool:
		return true
	}

	return false
}

func compileField(key string, v any, ix *Index) (matcher, error) {
	if !ix.filterable(key) {
		return nil, invalid("metadata key %q is non-filterable and cannot be used in a filter", key)
	}

	if isPrimitive(v) {
		return fieldOp(key, "$eq", v)
	}

	ops, ok := v.(map[string]any)
	if !ok || len(ops) == 0 {
		return nil, invalid("filter for %q must be a value or an operator object", key)
	}

	names := make([]string, 0, len(ops))
	for n := range ops {
		names = append(names, n)
	}

	slices.Sort(names)

	parts := make([]matcher, 0, len(names))

	for _, n := range names {
		m, err := fieldOp(key, n, ops[n])
		if err != nil {
			return nil, err
		}

		parts = append(parts, m)
	}

	return func(meta map[string]any) bool {
		for _, p := range parts {
			if !p(meta) {
				return false
			}
		}

		return true
	}, nil
}

func fieldOp(key, op string, arg any) (matcher, error) {
	switch op {
	case "$eq", "$ne":
		if !isPrimitive(arg) {
			return nil, invalid("%s on %q requires a string, number or boolean", op, key)
		}

		return func(meta map[string]any) bool {
			have, present := meta[key]
			if !present {
				return op == "$ne"
			}

			return holds(have, func(e any) bool { return e == arg }) == (op == "$eq")
		}, nil
	case "$gt", "$gte", "$lt", "$lte":
		n, ok := arg.(float64)
		if !ok {
			return nil, invalid("%s on %q requires a number", op, key)
		}

		return func(meta map[string]any) bool {
			have, isNum := meta[key].(float64)

			return isNum && compareOp(op, have, n)
		}, nil
	case "$in", "$nin":
		return compileIn(key, op, arg)
	case "$exists":
		want, ok := arg.(bool)
		if !ok {
			return nil, invalid("$exists on %q requires a boolean", key)
		}

		return func(meta map[string]any) bool {
			_, present := meta[key]

			return present == want
		}, nil
	}

	return nil, invalid("unsupported filter operator %q on %q", op, key)
}

func compileIn(key, op string, arg any) (matcher, error) {
	list, ok := arg.([]any)
	if !ok || len(list) == 0 {
		return nil, invalid("%s on %q requires a non-empty array", op, key)
	}

	for _, e := range list {
		if !isPrimitive(e) {
			return nil, invalid("%s on %q requires an array of strings, numbers or booleans", op, key)
		}
	}

	return func(meta map[string]any) bool {
		have, present := meta[key]
		if !present {
			return op == "$nin"
		}

		in := holds(have, func(e any) bool { return slices.Contains(list, e) })

		return in == (op == "$in")
	}, nil
}

// holds reports whether the stored value, or any element of a stored list, satisfies pred.
func holds(have any, pred func(any) bool) bool {
	if list, ok := have.([]any); ok {
		return slices.ContainsFunc(list, pred)
	}

	return pred(have)
}

func compareOp(op string, have, want float64) bool {
	c := cmp.Compare(have, want)

	switch op {
	case "$gt":
		return c > 0
	case "$gte":
		return c >= 0
	case "$lt":
		return c < 0
	default:
		return c <= 0
	}
}
