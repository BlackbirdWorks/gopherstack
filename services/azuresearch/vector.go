package azuresearch

import (
	"math"
	"sort"
)

const vecPostFilter = "postFilter"

func vecOf(v any) ([]float64, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}

	out := make([]float64, len(arr))

	for i, x := range arr {
		f, ok := asFloat(x)
		if !ok {
			return nil, false
		}

		out[i] = f
	}

	return out, true
}

// similarity converts a metric comparison of two vectors into a search score (higher is better).
func similarity(metric string, a, b []float64) float64 {
	var dot, na, nb, sq float64

	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
		d := a[i] - b[i]
		sq += d * d
	}

	switch metric {
	case "euclidean":
		return 1 / (1 + math.Sqrt(sq))
	case "dotProduct":
		return (1 + dot) / 2
	case "hamming":
		return 1 / (1 + sq)
	}

	if na == 0 || nb == 0 {
		return 0
	}

	return 1 / (2 - dot/math.Sqrt(na*nb))
}

func (pl *searchPlan) vectorField(name string, dims int) (*fieldDef, error) {
	fd, ok := pl.ix.comp.byPath[name]
	if !ok || !fd.isVector() {
		return nil, badf("The field '%s' in the vector query is not a vector field.", name)
	}

	if dims != fd.Dimensions {
		return nil, badf("The vector query has %d dimensions but the field '%s' has %d.", dims, name, fd.Dimensions)
	}

	return fd, nil
}

func (pl *searchPlan) vectorList(vq vectorQuery, docs []*Doc) ([]hit, error) {
	var fields []*fieldDef

	for _, n := range splitCSV(vq.Fields) {
		fd, err := pl.vectorField(n, len(vq.Vector))
		if err != nil {
			return nil, err
		}

		fields = append(fields, fd)
	}

	post := pl.req.VectorFilter == vecPostFilter

	var cands []hit

	for _, d := range docs {
		if !post && !pl.passes(d) {
			continue
		}

		if s, ok := bestSimilarity(pl.ix.comp, fields, d, vq.Vector); ok {
			cands = append(cands, hit{doc: d, key: keyOf(pl.ix, d), score: s})
		}
	}

	sort.SliceStable(cands, func(i, j int) bool { return cands[i].score > cands[j].score })

	if len(cands) > vq.K {
		cands = cands[:vq.K]
	}

	if post {
		kept := cands[:0]

		for _, h := range cands {
			if pl.passes(h.doc) {
				kept = append(kept, h)
			}
		}

		cands = kept
	}

	return cands, nil
}

func bestSimilarity(c *compiled, fields []*fieldDef, d *Doc, q []float64) (float64, bool) {
	best, found := 0.0, false

	for _, fd := range fields {
		v, ok := vecOf(d.Fields[fd.Name])
		if !ok || len(v) != len(q) {
			continue
		}

		if s := similarity(c.vecProfile[fd.Profile].Metric, q, v); !found || s > best {
			best, found = s, true
		}
	}

	return best, found
}

func (pl *searchPlan) vectorHits(docs []*Doc) ([]hit, error) {
	var lists [][]hit

	var weights []float64

	if pl.text {
		lists = append(lists, pl.textHits(docs))
		weights = append(weights, 1)
	}

	for _, vq := range pl.req.Vectors {
		l, err := pl.vectorList(vq, docs)
		if err != nil {
			return nil, err
		}

		lists = append(lists, l)
		weights = append(weights, vq.Weight)
	}

	if len(lists) == 1 {
		return lists[0], nil
	}

	return fuseRRF(lists, weights), nil
}

// fuseRRF merges ranked lists with Reciprocal Rank Fusion.
func fuseRRF(lists [][]hit, weights []float64) []hit {
	scores := map[string]*hit{}

	var order []string

	for li, l := range lists {
		for rank, h := range l {
			cur, ok := scores[h.key]
			if !ok {
				cp := hit{doc: h.doc, key: h.key}
				cur = &cp
				scores[h.key] = cur
				order = append(order, h.key)
			}

			cur.score += weights[li] / float64(rrfK+rank+1)
		}
	}

	out := make([]hit, 0, len(order))
	for _, k := range order {
		out = append(out, *scores[k])
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].score > out[j].score })

	return out
}
