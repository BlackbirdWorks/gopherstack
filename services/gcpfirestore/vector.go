package gcpfirestore

import (
	"cmp"
	"math"
	"slices"

	"cloud.google.com/go/firestore/apiv1/firestorepb"
	"google.golang.org/protobuf/proto"
)

const (
	vectorTypeKey   = "__type__"
	vectorTypeValue = "__vector__"
	vectorValueKey  = "value"
	maxVectorDims   = 2048
	maxNearestLimit = 1000
)

// vectorOf reads a Firestore vector value: a map {__type__: "__vector__", value: [numbers]}.
func vectorOf(v *firestorepb.Value) ([]float64, bool) {
	m, ok := v.GetValueType().(*firestorepb.Value_MapValue)
	if !ok || m.MapValue.GetFields()[vectorTypeKey].GetStringValue() != vectorTypeValue {
		return nil, false
	}

	arr, ok := m.MapValue.GetFields()[vectorValueKey].GetValueType().(*firestorepb.Value_ArrayValue)
	if !ok {
		return nil, false
	}

	out := make([]float64, 0, len(arr.ArrayValue.GetValues()))

	for _, e := range arr.ArrayValue.GetValues() {
		switch n := e.GetValueType().(type) {
		case *firestorepb.Value_DoubleValue:
			out = append(out, n.DoubleValue)
		case *firestorepb.Value_IntegerValue:
			out = append(out, float64(n.IntegerValue))
		default:
			return nil, false
		}
	}

	return out, true
}

func compareVectors(a, b []float64) int {
	if c := cmp.Compare(len(a), len(b)); c != 0 {
		return c
	}

	for i := range a {
		if c := compareDoubles(a[i], b[i]); c != 0 {
			return c
		}
	}

	return 0
}

func vectorDistance(m firestorepb.StructuredQuery_FindNearest_DistanceMeasure, a, b []float64) (float64, bool) {
	var dot, na, nb, sq float64

	for i := range a {
		dot += a[i] * b[i]
		na += a[i] * a[i]
		nb += b[i] * b[i]
		sq += (a[i] - b[i]) * (a[i] - b[i])
	}

	switch m {
	case firestorepb.StructuredQuery_FindNearest_EUCLIDEAN:
		return math.Sqrt(sq), true
	case firestorepb.StructuredQuery_FindNearest_COSINE:
		if na == 0 || nb == 0 {
			return 0, false
		}

		return 1 - dot/(math.Sqrt(na)*math.Sqrt(nb)), true
	case firestorepb.StructuredQuery_FindNearest_DOT_PRODUCT:
		return dot, true
	case firestorepb.StructuredQuery_FindNearest_DISTANCE_MEASURE_UNSPECIFIED:
	}

	return 0, false
}

type nearest struct {
	doc  *firestorepb.Document
	dist float64
}

func validateFindNearest(fn *firestorepb.StructuredQuery_FindNearest) ([]float64, error) {
	if fn.GetVectorField().GetFieldPath() == "" {
		return nil, errInvalid("find_nearest.vector_field is required.")
	}

	q, ok := vectorOf(fn.GetQueryVector())
	if !ok || len(q) == 0 {
		return nil, errInvalid("find_nearest.query_vector must be a non-empty vector.")
	}

	if slices.ContainsFunc(q, func(f float64) bool { return math.IsNaN(f) || math.IsInf(f, 0) }) {
		return nil, errInvalid("find_nearest.query_vector must hold finite numbers.")
	}

	if len(q) > maxVectorDims {
		return nil, errInvalid("Vectors may have at most %d dimensions.", maxVectorDims)
	}

	if fn.GetDistanceMeasure() == firestorepb.StructuredQuery_FindNearest_DISTANCE_MEASURE_UNSPECIFIED {
		return nil, errInvalid("find_nearest.distance_measure is required.")
	}

	if l := fn.GetLimit().GetValue(); l < 1 || l > maxNearestLimit {
		return nil, errInvalid("find_nearest.limit must be between 1 and %d.", maxNearestLimit)
	}

	return q, nil
}

func withDistance(d *firestorepb.Document, field string, dist float64) (*firestorepb.Document, error) {
	path, err := parseFieldPath(field)
	if err != nil {
		return nil, err
	}

	cp, ok := proto.Clone(d).(*firestorepb.Document)
	if !ok {
		return d, nil
	}

	if cp.Fields == nil {
		cp.Fields = map[string]*firestorepb.Value{}
	}

	setPath(cp.GetFields(), path, doubleVal(dist))

	return cp, nil
}

func nearestCandidates(
	docs []*firestorepb.Document, fn *firestorepb.StructuredQuery_FindNearest, query []float64, path []string,
) []nearest {
	measure := fn.GetDistanceMeasure()
	higherIsCloser := measure == firestorepb.StructuredQuery_FindNearest_DOT_PRODUCT
	cands := make([]nearest, 0, len(docs))

	for _, d := range docs {
		raw, found := getPath(d.GetFields(), path)
		vec, isVec := vectorOf(raw)

		if !found || !isVec || len(vec) != len(query) {
			continue
		}

		dist, ok := vectorDistance(measure, query, vec)
		if !ok || math.IsNaN(dist) || math.IsInf(dist, 0) || thresholdRejects(fn, dist, higherIsCloser) {
			continue
		}

		cands = append(cands, nearest{doc: d, dist: dist})
	}

	return cands
}

// findNearest keeps the limit documents whose vector field is closest to the query vector.
func findNearest(
	docs []*firestorepb.Document,
	fn *firestorepb.StructuredQuery_FindNearest,
) ([]*firestorepb.Document, error) {
	query, err := validateFindNearest(fn)
	if err != nil {
		return nil, err
	}

	path, err := parseFieldPath(fn.GetVectorField().GetFieldPath())
	if err != nil {
		return nil, err
	}

	higherIsCloser := fn.GetDistanceMeasure() == firestorepb.StructuredQuery_FindNearest_DOT_PRODUCT
	cands := nearestCandidates(docs, fn, query, path)

	slices.SortFunc(cands, func(a, b nearest) int {
		c := cmp.Compare(a.dist, b.dist)
		if higherIsCloser {
			c = -c
		}

		if c != 0 {
			return c
		}

		return cmp.Compare(a.doc.GetName(), b.doc.GetName())
	})

	cands = cands[:min(len(cands), int(fn.GetLimit().GetValue()))]
	out := make([]*firestorepb.Document, 0, len(cands))

	for _, c := range cands {
		d := c.doc

		if f := fn.GetDistanceResultField(); f != "" {
			if d, err = withDistance(c.doc, f, c.dist); err != nil {
				return nil, err
			}
		}

		out = append(out, d)
	}

	return out, nil
}

func thresholdRejects(fn *firestorepb.StructuredQuery_FindNearest, dist float64, higherIsCloser bool) bool {
	if fn.GetDistanceThreshold() == nil {
		return false
	}

	th := fn.GetDistanceThreshold().GetValue()
	if higherIsCloser {
		return dist < th
	}

	return dist > th
}
