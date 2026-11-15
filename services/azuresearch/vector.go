package azuresearch

import (
	"math"
	"slices"
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
		f, isNum := asFloat(x)
		if !isNum {
			return nil, false
		}

		out[i] = f
	}

	return out, true
}

// metricValue returns the canonical metric (similarity for cosine and dotProduct, distance for
// euclidean and hamming) and the @search.score derived from it.
func metricValue(metric string, a, b []float64) (float64, float64) {
	var dot, na, nb, sq float64

	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
		d := a[i] - b[i]
		sq += d * d
	}

	switch metric {
	case metricEuclidean:
		dist := math.Sqrt(sq)

		return dist, 1 / (1 + dist)
	case "dotProduct":
		return dot, (1 + dot) * half
	case metricHamming:
		return sq, 1 / (1 + sq)
	}

	if na == 0 || nb == 0 {
		return 0, 0
	}

	cos := dot / math.Sqrt(na*nb)

	return cos, 1 / (cosineOffset - cos)
}

func lowerIsCloser(metric string) bool { return metric == metricEuclidean || metric == metricHamming }

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

type vecCand struct {
	doc    *Doc
	fields map[string]vecScore
	key    string
	metric string
	rank   float64
	final  float64
	canon  float64
}

// vecPlan is a vector query resolved against the index.
type vecPlan struct {
	filter  pred
	fields  []*fieldDef
	over    float64
	rescore bool
}

func (pl *searchPlan) resolveVector(vq vectorQuery) (*vecPlan, error) {
	vp := &vecPlan{}

	for _, n := range splitCSV(vq.Fields) {
		fd, err := pl.vectorField(n, len(vq.Vector))
		if err != nil {
			return nil, err
		}

		vp.fields = append(vp.fields, fd)
	}

	if err := pl.checkVectorOptions(vq, vp); err != nil {
		return nil, err
	}

	if vq.FilterOverride != "" {
		f, err := compileFilter(vq.FilterOverride, pl.fctx)
		if err != nil {
			return nil, err
		}

		vp.filter = f
	}

	return vp, nil
}

func (pl *searchPlan) checkVectorOptions(vq vectorQuery, vp *vecPlan) error {
	comp := pl.ix.comp
	vp.over = minOversampling
	compressed := 0

	for i, fd := range vp.fields {
		prof := comp.vecProfile[fd.Profile]
		if prof.Compression != "" {
			compressed++

			if prof.Rescore {
				vp.rescore, vp.over = true, max(vp.over, prof.Oversample)
			}
		}

		if vq.Threshold != nil && vq.Threshold.Kind == thresholdSimilarity &&
			prof.Metric != comp.vecProfile[vp.fields[0].Profile].Metric {
			return badf(
				"The vector query threshold requires all fields to use the same similarity metric (field %d).",
				i,
			)
		}
	}

	if vq.Oversampling == 0 {
		return nil
	}

	if compressed != len(vp.fields) || !vp.rescore {
		return badf(
			"The vector query oversampling is only permitted when every field uses a compression with rescoring enabled.",
		)
	}

	vp.over = vq.Oversampling

	return nil
}

func (vp *vecPlan) passes(pl *searchPlan, d *Doc, post bool) bool {
	switch {
	case vp.filter != nil:
		return vp.filter(&fenv{d: d, doc: d.Fields, vars: map[string]any{}})
	case post:
		return true
	}

	return pl.passes(d)
}

func (vp *vecPlan) candidates(pl *searchPlan, d *Doc, q []float64, perDoc int) []vecCand {
	shared := map[string]vecScore{}
	key := keyOf(pl.ix, d)

	var out []vecCand

	for _, fd := range vp.fields {
		v, ok := vecOf(d.Fields[fd.Name])
		if !ok || len(v) != len(q) {
			continue
		}

		prof := pl.ix.comp.vecProfile[fd.Profile]
		canon, full := metricValue(prof.Metric, q, v)
		rank, final := full, full

		if prof.Compression != "" {
			var qcanon float64

			qcanon, rank = metricValue(prof.Metric, q, quantize(prof.Quantizer, v))
			if !prof.Rescore {
				final, canon = rank, qcanon
			}
		}

		shared[fd.Path] = vecScore{Search: final, Similarity: canon}

		out = append(out, vecCand{
			doc: d, fields: shared, key: key, metric: prof.Metric, rank: rank, final: final, canon: canon,
		})
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].rank > out[j].rank })

	if perDoc > 0 && len(out) > perDoc {
		out = out[:perDoc]
	}

	return out
}

func (vq vectorQuery) keeps(c vecCand) bool {
	t := vq.Threshold
	switch {
	case t == nil:
		return true
	case t.Kind == thresholdScore:
		return c.final >= t.Value
	case lowerIsCloser(c.metric):
		return c.canon <= t.Value
	}

	return c.canon >= t.Value
}

func (pl *searchPlan) vectorList(vq vectorQuery, docs []*Doc) ([]hit, error) {
	vp, err := pl.resolveVector(vq)
	if err != nil {
		return nil, err
	}

	post := pl.req.VectorFilter == vecPostFilter

	var cands []vecCand

	for _, d := range docs {
		if !vp.passes(pl, d, post) {
			continue
		}

		cands = append(cands, vp.candidates(pl, d, vq.Vector, vq.PerDocLimit)...)
	}

	sort.SliceStable(cands, func(i, j int) bool { return cands[i].rank > cands[j].rank })

	if pool := int(math.Ceil(float64(vq.K) * vp.over)); len(cands) > pool {
		cands = cands[:pool]
	}

	kept := cands[:0]

	for _, c := range cands {
		if vq.keeps(c) {
			kept = append(kept, c)
		}
	}

	sort.SliceStable(kept, func(i, j int) bool { return kept[i].final > kept[j].final })

	if len(kept) > vq.K {
		kept = kept[:vq.K]
	}

	kept = distinctDocs(kept)

	return pl.vectorHitsOf(kept, vp, post), nil
}

func (pl *searchPlan) vectorHitsOf(cands []vecCand, vp *vecPlan, post bool) []hit {
	out := make([]hit, 0, len(cands))

	for _, c := range cands {
		if post && vp.filter == nil && !pl.passes(c.doc) {
			continue
		}

		out = append(out, hit{doc: c.doc, key: c.key, score: c.final, vec: c.fields})
	}

	return out
}

func (pl *searchPlan) vectorHits(docs []*Doc) ([]hit, []hit, error) {
	var lists [][]hit

	var weights []float64

	var textAll []hit

	if pl.text {
		textAll = pl.textHits(docs)
		lists = append(lists, textAll[:min(len(textAll), pl.req.textRecall())])
		weights = append(weights, 1)
	}

	for _, vq := range pl.req.Vectors {
		l, verr := pl.vectorList(vq, docs)
		if verr != nil {
			return nil, nil, verr
		}

		lists = append(lists, l)
		weights = append(weights, vq.Weight)
	}

	hits := lists[0]
	if len(lists) > 1 {
		hits = fuseRRF(lists, weights)
	}

	pl.attachDebug(hits, lists)

	return hits, pl.countSet(hits, textAll), nil
}

func (pl *searchPlan) countSet(hits, textAll []hit) []hit {
	if pl.req.CountMode == countRetrievable || len(textAll) == 0 {
		return hits
	}

	seen := make(map[string]struct{}, len(hits))
	for _, h := range hits {
		seen[h.key] = struct{}{}
	}

	all := slices.Clone(hits)

	for _, h := range textAll {
		if _, ok := seen[h.key]; !ok {
			all = append(all, h)
		}
	}

	return all
}

func (r *searchRequest) textRecall() int {
	if r.MaxTextRecall > 0 {
		return r.MaxTextRecall
	}

	return defaultTextRecall
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

func distinctDocs(c []vecCand) []vecCand {
	seen := make(map[string]struct{}, len(c))
	out := c[:0]

	for _, x := range c {
		if _, dup := seen[x.key]; !dup {
			seen[x.key] = struct{}{}

			out = append(out, x)
		}
	}

	return out
}
