package azuresearch

import (
	"math"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/iso8601"
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
		p := objOf(raw)
		sp := &scoringProfile{
			Name: strOf(p, "name"), Weights: map[string]float64{}, Aggregation: strOf(p, "functionAggregation"),
		}

		if sp.Aggregation == "" {
			sp.Aggregation = "sum"
		}

		for field, w := range objOf(objOf(p["text"])["weights"]) {
			fd, ok := c.comp.byPath[field]
			if !ok || !fd.Searchable {
				return badf("The request is invalid. Details: scoring profile '%s' weights unknown or non-searchable field '%s'.",
					sp.Name, field)
			}

			sp.Weights[field], _ = w.(float64)
		}

		for _, fr := range listOf(p, "functions") {
			sf, err := c.scoreFunc(sp.Name, objOf(fr))
			if err != nil {
				return err
			}

			sp.Funcs = append(sp.Funcs, sf)
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

func (c *compiler) scoreFunc(profile string, f map[string]any) (scoreFunc, error) {
	sf := scoreFunc{
		Type: strOf(f, "type"), Field: strOf(f, "fieldName"), Interpolation: strOf(f, "interpolation"),
	}
	sf.Boost, _ = f["boost"].(float64)

	if sf.Interpolation == "" {
		sf.Interpolation = "linear"
	}

	if _, ok := c.comp.byPath[sf.Field]; !ok {
		return sf, badf("The request is invalid. Details: scoring profile '%s' function references unknown field '%s'.", profile, sf.Field)
	}

	switch sf.Type {
	case "magnitude":
		m := objOf(f["magnitude"])
		sf.RangeStart, _ = m["boostingRangeStart"].(float64)
		sf.RangeEnd, _ = m["boostingRangeEnd"].(float64)
		sf.ConstBeyond = boolOf(m, "constantBoostBeyondRange", false)
	case "freshness":
		d, err := iso8601.Parse(strOf(objOf(f["freshness"]), "boostingDuration"))
		if err != nil {
			return sf, badf("The request is invalid. Details: invalid freshness boostingDuration.")
		}

		sf.Duration = d
	case "distance":
		d := objOf(f["distance"])
		sf.RefParam = strOf(d, "referencePointParameter")
		sf.Distance, _ = d["boostingDistance"].(float64)
	case "tag":
		sf.TagsParam = strOf(objOf(f["tag"]), "tagsParameter")
	default:
		return sf, badf("The request is invalid. Details: unsupported scoring function type '%s'.", sf.Type)
	}

	return sf, nil
}

// interpolate maps a 0..1 position (1 = full boost) through the interpolation curve.
func interpolate(kind string, x float64) float64 {
	x = math.Max(0, math.Min(1, x))

	switch kind {
	case "constant":
		return 1
	case "quadratic":
		return x * x
	case "logarithmic":
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

	total := 1.0
	for _, v := range vals {
		total += v - 1
	}

	return total
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
	case "magnitude":
		n, ok := asFloat(val)
		if !ok || f.RangeEnd == f.RangeStart {
			return 0, false
		}

		pos := (n - f.RangeStart) / (f.RangeEnd - f.RangeStart)
		if (pos < 0 || pos > 1) && !f.ConstBeyond {
			return 0, false
		}

		return pos, true
	case "freshness":
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
	case "distance":
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
	case "tag":
		return tagPosition(val, params[f.TagsParam])
	}

	return 0, false
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
