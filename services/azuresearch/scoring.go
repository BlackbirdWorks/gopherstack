package azuresearch

import (
	"math"
	"slices"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
)

const (
	fnMagnitude     = "magnitude"
	fnFreshness     = "freshness"
	fnDistance      = "distance"
	fnTag           = "tag"
	interpLog       = "logarithmic"
	interpQuadratic = "quadratic"
	aggSum          = "sum"
	aggProduct      = "product"
)

type scoreFunc struct {
	Type          string
	Field         string
	Interpolation string
	RefParam      string
	TagsParam     string
	Boost         float64
	RangeStart    float64
	RangeEnd      float64
	Distance      float64
	Duration      time.Duration
	ConstBeyond   bool
}

type scoringProfile struct {
	Weights     map[string]float64
	Name        string
	Aggregation string
	Funcs       []scoreFunc
}

func (c *compiler) scoring(def map[string]any) error {
	for _, raw := range listOf(def, "scoringProfiles") {
		sp, err := c.scoringProfile(objOf(raw))
		if err != nil {
			return err
		}

		c.comp.profiles[sp.Name] = sp
	}

	if dp := strOf(def, "defaultScoringProfile"); dp != "" {
		if _, ok := c.comp.profiles[dp]; !ok {
			return badf("The request is invalid. Details: defaultScoringProfile '%s' is not defined.", dp)
		}

		c.comp.defProfile = dp
	}

	return nil
}

func (c *compiler) scoringProfile(p map[string]any) (*scoringProfile, error) {
	sp := &scoringProfile{
		Name: strOf(p, "name"), Weights: map[string]float64{}, Aggregation: strOf(p, "functionAggregation"),
	}

	if sp.Aggregation == "" {
		sp.Aggregation = aggSum
	}

	switch sp.Aggregation {
	case aggSum, "average", "minimum", "maximum", "firstMatching", aggProduct:
	default:
		return nil, badf(
			"The request is invalid. Details: scoring profile '%s' has unsupported functionAggregation '%s'.",
			sp.Name,
			sp.Aggregation,
		)
	}

	if err := c.profileWeights(sp, objOf(objOf(p["text"])["weights"])); err != nil {
		return nil, err
	}

	for _, fr := range listOf(p, "functions") {
		sf, err := c.scoreFunc(sp.Name, objOf(fr))
		if err != nil {
			return nil, err
		}

		sp.Funcs = append(sp.Funcs, sf)
	}

	return sp, nil
}

func (c *compiler) profileWeights(sp *scoringProfile, weights map[string]any) error {
	for field, w := range weights {
		fd, ok := c.comp.byPath[field]
		if !ok || !fd.Searchable {
			return badf(
				"The request is invalid. Details: scoring profile '%s' weights unknown or non-searchable field '%s'.",
				sp.Name,
				field,
			)
		}

		wt, isNum := w.(float64)
		if !isNum || wt <= 0 {
			return badf("The request is invalid. Details: scoring profile '%s' weight for '%s' must be positive.",
				sp.Name, field)
		}

		sp.Weights[field] = wt
	}

	return nil
}

func (c *compiler) scoreFunc(profile string, f map[string]any) (scoreFunc, error) {
	sf := scoreFunc{
		Type: strOf(f, "type"), Field: strOf(f, "fieldName"), Interpolation: strOf(f, "interpolation"),
	}
	sf.Boost, _ = f["boost"].(float64)

	if sf.Interpolation == "" {
		sf.Interpolation = "linear"
	}

	fd, ok := c.comp.byPath[sf.Field]
	if !ok {
		return sf, badf(
			"The request is invalid. Details: scoring profile '%s' function references unknown field '%s'.",
			profile,
			sf.Field,
		)
	}

	if err := checkScoreFunc(profile, sf, fd); err != nil {
		return sf, err
	}

	switch sf.Type {
	case fnMagnitude:
		m := objOf(f["magnitude"])
		sf.RangeStart, _ = m["boostingRangeStart"].(float64)
		sf.RangeEnd, _ = m["boostingRangeEnd"].(float64)
		sf.ConstBeyond = boolOf(m, "constantBoostBeyondRange", false)
	case fnFreshness:
		d, err := iso8601.Parse(strOf(objOf(f["freshness"]), "boostingDuration"))
		if err != nil {
			return sf, badf("The request is invalid. Details: invalid freshness boostingDuration.")
		}

		sf.Duration = d
	case fnDistance:
		d := objOf(f["distance"])
		sf.RefParam = strOf(d, "referencePointParameter")
		sf.Distance, _ = d["boostingDistance"].(float64)
	case fnTag:
		sf.TagsParam = strOf(objOf(f["tag"]), "tagsParameter")
	default:
		return sf, badf("The request is invalid. Details: unsupported scoring function type '%s'.", sf.Type)
	}

	return sf, nil
}

func scoreFuncFieldTypes(kind string) []string {
	switch kind {
	case fnMagnitude:
		return []string{typeInt32, typeInt64, typeDouble}
	case fnFreshness:
		return []string{typeDate}
	case fnDistance:
		return []string{typeGeo}
	case fnTag:
		return []string{typeString}
	}

	return nil
}

// checkScoreFunc applies the documented function rules: filterable field, a field type that fits the
// function, a known interpolation, and no quadratic or logarithmic interpolation on tag.
func checkScoreFunc(profile string, sf scoreFunc, fd *fieldDef) error {
	if types := scoreFuncFieldTypes(sf.Type); types != nil && !slices.Contains(types, fd.Base) {
		return badf(
			"The request is invalid. Details: scoring profile '%s' %s function cannot use field '%s' of type %s.",
			profile,
			sf.Type,
			fd.Name,
			fd.Type,
		)
	}

	if !fd.Filterable {
		return badf("The request is invalid. Details: scoring profile '%s' function field '%s' must be filterable.",
			profile, fd.Name)
	}

	if !slices.Contains([]string{"constant", "linear", interpQuadratic, interpLog}, sf.Interpolation) ||
		(sf.Type == fnTag && (sf.Interpolation == interpQuadratic || sf.Interpolation == interpLog)) {
		return badf("The request is invalid. Details: scoring profile '%s' interpolation '%s' is not allowed for %s.",
			profile, sf.Interpolation, sf.Type)
	}

	return nil
}

// interpolate maps a 0..1 position (1 = full boost) through the interpolation curve.
func interpolate(kind string, x float64) float64 {
	x = math.Max(0, math.Min(1, x))

	switch kind {
	case "constant":
		return 1
	case interpQuadratic:
		return x * x
	case interpLog:
		return math.Log1p(x*(math.E-1)) / 1
	}

	return x
}

type scoreParams map[string][]string

// functionBoost returns the multiplier the profile's functions apply to a document's text score.
func (sp *scoringProfile) functionBoost(doc *Doc, params scoreParams, now time.Time) float64 {
	if len(sp.Funcs) == 0 {
		return 1
	}

	vals := make([]float64, 0, len(sp.Funcs))

	for _, f := range sp.Funcs {
		pos, ok := f.position(doc, params, now)
		if !ok {
			vals = append(vals, 1)

			continue
		}

		vals = append(vals, 1+(f.Boost-1)*interpolate(f.Interpolation, pos))
	}

	switch sp.Aggregation {
	case "average":
		return sum(vals) / float64(len(vals))
	case "minimum":
		return minOf(vals)
	case "maximum":
		return maxOf(vals)
	case "firstMatching":
		for _, v := range vals {
			if v != 1 {
				return v
			}
		}

		return 1
	}

	if sp.Aggregation == aggProduct {
		return product(vals)
	}

	total := 1.0
	for _, v := range vals {
		total += v - 1
	}

	return total
}

func product(v []float64) float64 {
	t := 1.0
	for _, x := range v {
		t *= x
	}

	return t
}

func sum(v []float64) float64 {
	t := 0.0
	for _, x := range v {
		t += x
	}

	return t
}

func minOf(v []float64) float64 {
	m := v[0]
	for _, x := range v {
		m = math.Min(m, x)
	}

	return m
}

func maxOf(v []float64) float64 {
	m := v[0]
	for _, x := range v {
		m = math.Max(m, x)
	}

	return m
}

func (f scoreFunc) position(doc *Doc, params scoreParams, now time.Time) (float64, bool) {
	val := lookupPath(doc.Fields, f.Field)

	switch f.Type {
	case fnMagnitude:
		return f.magnitudePosition(val)
	case fnFreshness:
		return f.freshnessPosition(val, now)
	case fnDistance:
		return f.distancePosition(val, params)
	case fnTag:
		return tagPosition(val, params[f.TagsParam])
	}

	return 0, false
}

func (f scoreFunc) magnitudePosition(val any) (float64, bool) {
	n, ok := asFloat(val)
	if !ok || f.RangeEnd == f.RangeStart {
		return 0, false
	}

	pos := (n - f.RangeStart) / (f.RangeEnd - f.RangeStart)
	if (pos < 0 || pos > 1) && !f.ConstBeyond {
		return 0, false
	}

	return pos, true
}

func (f scoreFunc) freshnessPosition(val any, now time.Time) (float64, bool) {
	t, ok := parseDate(val)
	if !ok || f.Duration <= 0 {
		return 0, false
	}

	age := now.Sub(t)
	if age < 0 {
		age = -age
	}

	if age > f.Duration {
		return 0, false
	}

	return 1 - float64(age)/float64(f.Duration), true
}

func (f scoreFunc) distancePosition(val any, params scoreParams) (float64, bool) {
	pt, ok := parseGeoPoint(val)
	ref, rok := paramPoint(params[f.RefParam])

	if !ok || !rok || f.Distance <= 0 {
		return 0, false
	}

	d := haversineKM(pt, ref)
	if d > f.Distance {
		return 0, false
	}

	return 1 - d/f.Distance, true
}

func tagPosition(val any, tags []string) (float64, bool) {
	var have []string

	switch t := val.(type) {
	case string:
		have = []string{t}
	case []any:
		for _, x := range t {
			if s, ok := x.(string); ok {
				have = append(have, s)
			}
		}
	}

	for _, h := range have {
		for _, want := range tags {
			if strings.EqualFold(strings.TrimSpace(h), strings.TrimSpace(want)) {
				return 1, true
			}
		}
	}

	return 0, false
}
