package gcpdatastore

import (
	"math"
	"slices"
	"sort"

	"cloud.google.com/go/datastore/apiv1/datastorepb"
	"google.golang.org/protobuf/proto"
)

const (
	maxVectorDims   = 2048
	maxNearestLimit = 100
	vectorTypeKey   = "__type__"
	vectorTypeValue = "__vector__"
	vectorValueKey  = "value"
)

// nearestPlan is a validated FindNearest stage.
type nearestPlan struct {
	prop      string
	distProp  string
	query     []float64
	threshold float64
	limit     int
	measure   datastorepb.FindNearest_DistanceMeasure
	hasThresh bool
}

func withoutOrders(q *datastorepb.Query) *datastorepb.Query {
	out, ok := proto.Clone(q).(*datastorepb.Query)
	if !ok {
		return q
	}

	out.Order = nil

	return out
}

// vectorOf reads a vector value stored as a map {__type__: "__vector__", value: [numbers]}.
func vectorOf(v *datastorepb.Value) ([]float64, bool) {
	m := v.GetEntityValue()
	if m == nil || m.GetProperties()[vectorTypeKey].GetStringValue() != vectorTypeValue {
		return nil, false
	}

	arr := m.GetProperties()[vectorValueKey].GetArrayValue()
	if arr == nil {
		return nil, false
	}

	out := make([]float64, 0, len(arr.GetValues()))

	for _, e := range arr.GetValues() {
		switch e.GetValueType().(type) {
		case *datastorepb.Value_DoubleValue:
			out = append(out, e.GetDoubleValue())
		case *datastorepb.Value_IntegerValue:
			out = append(out, float64(e.GetIntegerValue()))
		default:
			return nil, false
		}
	}

	return out, true
}

func queryVector(fn *datastorepb.FindNearest) ([]float64, error) {
	qv, ok := vectorOf(fn.GetQueryVector())
	if !ok || len(qv) == 0 {
		return nil, errInvalid("find_nearest.query_vector must be a non-empty vector.")
	}

	if len(qv) > maxVectorDims {
		return nil, errInvalid("Vectors may have at most %d dimensions.", maxVectorDims)
	}

	if slices.ContainsFunc(qv, func(f float64) bool { return math.IsNaN(f) || math.IsInf(f, 0) }) {
		return nil, errInvalid("find_nearest.query_vector must hold finite numbers.")
	}

	return qv, nil
}

func planNearest(fn *datastorepb.FindNearest, q *datastorepb.Query) (*nearestPlan, error) {
	if fn.GetVectorProperty().GetName() == "" {
		return nil, errInvalid("find_nearest.vector_property is required.")
	}

	qv, err := queryVector(fn)
	if err != nil {
		return nil, err
	}

	switch fn.GetDistanceMeasure() {
	case datastorepb.FindNearest_EUCLIDEAN, datastorepb.FindNearest_COSINE, datastorepb.FindNearest_DOT_PRODUCT:
	case datastorepb.FindNearest_DISTANCE_MEASURE_UNSPECIFIED:
		return nil, errInvalid("find_nearest.distance_measure is required.")
	}

	if l := fn.GetLimit().GetValue(); l < 1 || l > maxNearestLimit {
		return nil, errInvalid("find_nearest.limit must be between 1 and %d.", maxNearestLimit)
	}

	if len(q.GetStartCursor()) > 0 || len(q.GetEndCursor()) > 0 {
		return nil, errInvalid("Cursors cannot be combined with find_nearest.")
	}

	n := &nearestPlan{
		query: qv, prop: fn.GetVectorProperty().GetName(), distProp: fn.GetDistanceResultProperty(),
		measure: fn.GetDistanceMeasure(), limit: int(fn.GetLimit().GetValue()),
	}

	if fn.GetDistanceThreshold() != nil {
		n.threshold, n.hasThresh = fn.GetDistanceThreshold().GetValue(), true
	}

	return n, nil
}

func (n *nearestPlan) distance(a []float64) (float64, bool) {
	if len(a) != len(n.query) {
		return 0, false
	}

	var dot, na, nb, sq float64

	for i := range a {
		dot += a[i] * n.query[i]
		na += a[i] * a[i]
		nb += n.query[i] * n.query[i]
		sq += (a[i] - n.query[i]) * (a[i] - n.query[i])
	}

	var d float64

	switch n.measure {
	case datastorepb.FindNearest_EUCLIDEAN:
		d = math.Sqrt(sq)
	case datastorepb.FindNearest_COSINE:
		if na == 0 || nb == 0 {
			return 0, false
		}

		d = 1 - dot/(math.Sqrt(na)*math.Sqrt(nb))
	case datastorepb.FindNearest_DOT_PRODUCT:
		d = dot
	case datastorepb.FindNearest_DISTANCE_MEASURE_UNSPECIFIED:
		return 0, false
	}

	if math.IsNaN(d) || math.IsInf(d, 0) {
		return 0, false
	}

	return d, true
}

func (n *nearestPlan) within(d float64) bool {
	if !n.hasThresh {
		return true
	}

	if n.measure == datastorepb.FindNearest_DOT_PRODUCT {
		return d >= n.threshold
	}

	return d <= n.threshold
}

// apply keeps the closest entities with a matching vector, optionally stamping the distance.
func (n *nearestPlan) apply(rows []*row, byKey map[*datastorepb.Key]*cand) []*row {
	type scored struct {
		r *row
		d float64
	}

	var sc []scored

	for _, r := range rows {
		c := byKey[r.key]
		if c == nil {
			continue
		}

		vec, ok := vectorOf(c.ent.GetProperties()[n.prop])
		if !ok {
			continue
		}

		if d, found := n.distance(vec); found && n.within(d) {
			sc = append(sc, scored{r: r, d: d})
		}
	}

	sort.SliceStable(sc, func(i, j int) bool {
		if n.measure == datastorepb.FindNearest_DOT_PRODUCT {
			return sc[i].d > sc[j].d
		}

		return sc[i].d < sc[j].d
	})

	if len(sc) > n.limit {
		sc = sc[:n.limit]
	}

	out := make([]*row, 0, len(sc))

	for _, s := range sc {
		r := *s.r

		if n.distProp != "" {
			cp, ok := proto.Clone(r.ent).(*datastorepb.Entity)
			if ok {
				cp.Properties[n.distProp] = doubleValue(s.d)
				r.ent = cp
			}
		}

		out = append(out, &r)
	}

	return out
}
