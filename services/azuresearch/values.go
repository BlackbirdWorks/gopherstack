package azuresearch

import (
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const earthRadiusKM = 6371.0088

type geoPoint struct{ Lon, Lat float64 }

func asFloat(v any) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case int64:
		return float64(n), true
	case int:
		return float64(n), true
	case jsonx.Number:
		f, err := n.Float64()

		return f, err == nil
	case string:
		switch n {
		case litNaN:
			return math.NaN(), true
		case litINF:
			return math.Inf(1), true
		case "-INF":
			return math.Inf(-1), true
		}
	}

	return 0, false
}

func parseDate(v any) (time.Time, bool) {
	s, ok := v.(string)
	if !ok {
		return time.Time{}, false
	}

	for _, layout := range []string{time.RFC3339Nano, "2006-01-02T15:04:05", "2006-01-02"} {
		if t, err := time.Parse(layout, s); err == nil {
			return t.UTC(), true
		}
	}

	return time.Time{}, false
}

func parseGeoPoint(v any) (geoPoint, bool) {
	m, ok := v.(map[string]any)
	if !ok {
		return geoPoint{}, false
	}

	coords, _ := m["coordinates"].([]any)
	if len(coords) != coordPair {
		return geoPoint{}, false
	}

	lon, ok1 := asFloat(coords[0])
	lat, ok2 := asFloat(coords[1])

	return geoPoint{Lon: lon, Lat: lat}, ok1 && ok2 && lon >= -180 && lon <= 180 && lat >= -90 && lat <= 90
}

func paramPoint(vals []string) (geoPoint, bool) {
	if len(vals) != coordPair {
		return geoPoint{}, false
	}

	lon, e1 := strconv.ParseFloat(strings.TrimSpace(vals[0]), 64)
	lat, e2 := strconv.ParseFloat(strings.TrimSpace(vals[1]), 64)

	return geoPoint{Lon: lon, Lat: lat}, e1 == nil && e2 == nil
}

func haversineKM(a, b geoPoint) float64 {
	rad := degToRad
	dLat, dLon := (b.Lat-a.Lat)*rad, (b.Lon-a.Lon)*rad
	h := math.Sin(dLat*half)*math.Sin(dLat*half) +
		math.Cos(a.Lat*rad)*math.Cos(b.Lat*rad)*math.Sin(dLon*half)*math.Sin(dLon*half)

	return 2 * earthRadiusKM * math.Asin(math.Min(1, math.Sqrt(h)))
}

// lookupPath reads a (possibly nested, slash-separated) field from a document.
func lookupPath(fields map[string]any, path string) any {
	var cur any = fields

	for seg := range strings.SplitSeq(path, "/") {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}

		cur = m[seg]
	}

	return cur
}

func typeErr(base string) error {
	return badf(
		"Cannot convert a primitive value to the expected type '%s'. See the inner exception for more information.",
		base,
	)
}

// normalizeDoc validates raw against the schema and returns the document as it is stored.
func normalizeDoc(c *compiled, raw map[string]any, version string) (map[string]any, error) {
	return normalizeObject(c.fields, raw, "search.documentFields", version)
}

func normalizeObject(defs []*fieldDef, raw map[string]any, typeName, version string) (map[string]any, error) {
	byName := make(map[string]*fieldDef, len(defs))
	for _, d := range defs {
		byName[d.Name] = d
	}

	out := make(map[string]any, len(raw))

	for k, v := range raw {
		if strings.HasPrefix(k, "@search.") || strings.HasPrefix(k, "@odata.") {
			continue
		}

		d, ok := byName[k]
		if !ok {
			return nil, badf("The property '%s' does not exist on type '%s' or is not present in the API version '%s'.",
				k, typeName, version)
		}

		nv, err := normalizeValue(d, v, version)
		if err != nil {
			return nil, err
		}

		out[k] = nv
	}

	return out, nil
}

func normalizeValue(d *fieldDef, v any, version string) (any, error) {
	if v == nil {
		return nil, nil //nolint:nilnil // JSON null is a legitimate stored value
	}

	if !d.Collection {
		return normalizeScalar(d, v, version)
	}

	arr, ok := v.([]any)
	if !ok {
		return nil, typeErr(d.Type)
	}

	if d.isVector() {
		return normalizeVector(d, arr)
	}

	out := make([]any, 0, len(arr))

	for _, el := range arr {
		nv, err := normalizeScalar(d, el, version)
		if err != nil {
			return nil, err
		}

		out = append(out, nv)
	}

	return out, nil
}

func normalizeVector(d *fieldDef, arr []any) (any, error) {
	if len(arr) != d.Dimensions {
		return nil, badf(
			"A vector field '%s' must have %d dimensions, but %d were provided.",
			d.Name,
			d.Dimensions,
			len(arr),
		)
	}

	out := make([]any, len(arr))

	for i, el := range arr {
		f, ok := el.(jsonx.Number)
		if !ok {
			if ff, isF := el.(float64); isF {
				out[i] = ff

				continue
			}

			return nil, typeErr(d.Base)
		}

		ff, err := f.Float64()
		if err != nil {
			return nil, typeErr(d.Base)
		}

		out[i] = ff
	}

	return out, nil
}

func normalizeScalar(d *fieldDef, v any, version string) (any, error) {
	switch d.Base {
	case typeGeo:
		if _, ok := parseGeoPoint(v); !ok {
			return nil, typeErr(d.Base)
		}

		return v, nil
	case typeComplex:
		m, ok := v.(map[string]any)
		if !ok {
			return nil, typeErr(d.Type)
		}

		return normalizeObject(d.Children, m, "search.complex."+d.Name, version)
	}

	return normalizePrimitive(d, v)
}

func normalizePrimitive(d *fieldDef, v any) (any, error) {
	switch d.Base {
	case typeString:
		if s, ok := v.(string); ok {
			return s, nil
		}
	case typeBool:
		if b, ok := v.(bool); ok {
			return b, nil
		}
	case typeInt32, typeInt64:
		return normalizeInt(d, v)
	case typeDouble, typeSingle, typeHalf:
		return normalizeFloat(d, v)
	case typeDate:
		if t, ok := parseDate(v); ok {
			return t.Format(time.RFC3339Nano), nil
		}
	}

	return nil, typeErr(d.Base)
}

func normalizeFloat(d *fieldDef, v any) (any, error) {
	f, ok := numberOf(v)
	if s, isS := v.(string); isS && !ok {
		f, ok = asFloat(s)
	}

	if !ok {
		return nil, typeErr(d.Base)
	}

	if math.IsNaN(f) || math.IsInf(f, 0) {
		return nonFinite(f), nil
	}

	return f, nil
}

func nonFinite(f float64) string {
	switch {
	case math.IsNaN(f):
		return litNaN
	case f > 0:
		return litINF
	}

	return "-INF"
}

func numberOf(v any) (float64, bool) {
	switch n := v.(type) {
	case jsonx.Number:
		f, err := n.Float64()

		return f, err == nil
	case float64:
		return n, true
	}

	return 0, false
}

func normalizeInt(d *fieldDef, v any) (any, error) {
	var i int64

	switch n := v.(type) {
	case jsonx.Number:
		parsed, err := n.Int64()
		if err != nil {
			return nil, typeErr(d.Base)
		}

		i = parsed
	case float64:
		if n != math.Trunc(n) {
			return nil, typeErr(d.Base)
		}

		i = int64(n)
	default:
		return nil, typeErr(d.Base)
	}

	if d.Base == typeInt32 && (i < math.MinInt32 || i > math.MaxInt32) {
		return nil, typeErr(d.Base)
	}

	return i, nil
}

func fmtNum(f float64) string { return strconv.FormatFloat(f, 'g', -1, 64) }
