package bqsql

import (
	"database/sql/driver"
	"encoding/json/v2"
	"slices"
	"strings"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

const geoJSONPosLen = 2

// addGeoUDFs registers the ST_* functions of
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions.
func addGeoUDFs(d map[string]udfDef) {
	d["st_geogpoint"] = udfDef{n: argc2, fn: udfGeogPoint}
	d["st_geogfromtext"] = udfDef{n: -1, fn: udfGeogFromText}
	d["st_astext"] = geoUnary(func(g *geo) (driver.Value, error) { return g.wkt(), nil })
	d["st_geogfromgeojson"] = udfDef{n: -1, fn: udfGeogFromGeoJSON}
	d["st_asgeojson"] = geoUnary(func(g *geo) (driver.Value, error) { return g.geoJSON(), nil })
	d["st_x"] = geoUnary(func(g *geo) (driver.Value, error) { return pointCoord(g, "ST_X", true) })
	d["st_y"] = geoUnary(func(g *geo) (driver.Value, error) { return pointCoord(g, "ST_Y", false) })
	d["st_distance"] = geoMeasure(0, func(a, b *geo, _ []driver.Value) (driver.Value, error) {
		if a.isEmpty() || b.isEmpty() {
			return nil, nil //nolint:nilnil // empty inputs give NULL
		}

		return distance(a, b) * earthRadiusMeters, nil
	})
	d["st_dwithin"] = geoMeasure(1, func(a, b *geo, rest []driver.Value) (driver.Value, error) {
		limit, err := toFloat(rest[0])
		if err != nil {
			return nil, err
		}

		if a.isEmpty() || b.isEmpty() {
			return boolVal(false), nil
		}

		return boolVal(distance(a, b)*earthRadiusMeters <= limit), nil
	})
	d["st_length"] = udfDef{n: -1, fn: geoScalar(func(g *geo) (driver.Value, error) { return g.length(), nil })}
	d["st_area"] = udfDef{n: -1, fn: geoScalar(func(g *geo) (driver.Value, error) { return g.area(), nil })}
	d["st_contains"] = geoPredicate(contains)
	d["st_within"] = geoPredicate(func(a, b *geo) bool { return contains(b, a) })
	d["st_intersects"] = geoPredicate(intersects)
	d["st_makeline"] = udfDef{n: -1, fn: udfMakeLine}
	d["st_makepolygon"] = udfDef{n: -1, fn: udfMakePolygon}
	d["st_centroid"] = geoUnary(func(g *geo) (driver.Value, error) { return g.centroid().wkt(), nil })
}

func loadGeo(v driver.Value) (*geo, error) { return parseWKT(toStr(v)) }

func geoUnary(f func(*geo) (driver.Value, error)) udfDef {
	return udfDef{n: 1, fn: geoScalar(f)}
}

// geoScalar runs f on the first argument; trailing arguments are use_spheroid flags, of which only FALSE is supported.
func geoScalar(f func(*geo) (driver.Value, error)) udfFn {
	return func(a []driver.Value) (driver.Value, error) {
		if a[0] == nil || len(a) > 1 && a[1] == nil {
			return nil, nil
		}

		if err := noSpheroid(a[1:]); err != nil {
			return nil, err
		}

		g, err := loadGeo(a[0])
		if err != nil {
			return nil, err
		}

		return f(g)
	}
}

func noSpheroid(rest []driver.Value) error {
	for _, v := range rest {
		if n, err := toInt(v); err == nil && n != 0 {
			return errBQ("use_spheroid => TRUE is not supported: only spherical calculations are available")
		}
	}

	return nil
}

func geoMeasure(extra int, f func(a, b *geo, rest []driver.Value) (driver.Value, error)) udfDef {
	return udfDef{n: -1, fn: func(a []driver.Value) (driver.Value, error) {
		fixedArgs := argc2 + extra
		if len(a) < fixedArgs {
			return nil, errBQ("not enough arguments")
		}

		for _, v := range a[:fixedArgs] {
			if v == nil {
				return nil, nil
			}
		}

		if err := noSpheroid(a[fixedArgs:]); err != nil {
			return nil, err
		}

		g1, err := loadGeo(a[0])
		if err != nil {
			return nil, err
		}

		g2, err := loadGeo(a[1])
		if err != nil {
			return nil, err
		}

		return f(g1, g2, a[argc2:fixedArgs])
	}}
}

func geoPredicate(f func(a, b *geo) bool) udfDef {
	return udfDef{n: argc2, fn: func(a []driver.Value) (driver.Value, error) {
		if anyNull(a) {
			return nil, nil
		}

		g1, err := loadGeo(a[0])
		if err != nil {
			return nil, err
		}

		g2, err := loadGeo(a[1])
		if err != nil {
			return nil, err
		}

		return boolVal(f(g1, g2)), nil
	}}
}

func pointCoord(g *geo, fn string, lon bool) (driver.Value, error) {
	p, err := g.singlePoint(fn)
	if err != nil {
		return nil, err
	}

	if lon {
		return p.lon, nil
	}

	return p.lat, nil
}

func udfGeogPoint(a []driver.Value) (driver.Value, error) {
	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	lon, err := toFloat(a[0])
	if err != nil {
		return nil, err
	}

	lat, err := toFloat(a[1])
	if err != nil {
		return nil, err
	}

	g, err := makePoint(lon, lat)
	if err != nil {
		return nil, err
	}

	return g.wkt(), nil
}

// udfGeogFromText takes (wkt, oriented, planar, make_valid); omitted flags arrive as FALSE.
func udfGeogFromText(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	flag := func(i int) bool {
		if i >= len(a) || a[i] == nil {
			return false
		}

		n, err := toInt(a[i])

		return err == nil && n != 0
	}

	oriented, planar, makeValid := flag(1), flag(argc2), flag(argc3)

	switch {
	case oriented && planar:
		return nil, errBQ("ST_GEOGFROMTEXT: oriented and planar cannot both be TRUE")
	case oriented && makeValid:
		return nil, errBQ("ST_GEOGFROMTEXT: oriented and make_valid cannot both be TRUE")
	case makeValid:
		return nil, errBQ("ST_GEOGFROMTEXT: make_valid => TRUE is not supported")
	}

	g, err := parseWKT(toStr(a[0]))
	if err != nil {
		return nil, err
	}

	return g.wkt(), nil
}

func udfGeogFromGeoJSON(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	if len(a) > 1 && a[1] != nil {
		if n, err := toInt(a[1]); err == nil && n != 0 {
			return nil, errBQ("ST_GEOGFROMGEOJSON: make_valid => TRUE is not supported")
		}
	}

	var doc any
	if err := json.Unmarshal([]byte(toStr(a[0])), &doc, jsonx.V1()); err != nil {
		return nil, errBQ("Invalid GeoJSON: %v", err)
	}

	g, err := geoFromJSON(doc)
	if err != nil {
		return nil, err
	}

	if err = g.validate(); err != nil {
		return nil, err
	}

	return g.wkt(), nil
}

func geoFromJSON(v any) (*geo, error) {
	m, ok := v.(map[string]any)
	if !ok {
		return nil, errBQ("Invalid GeoJSON: expected an object")
	}

	typ, _ := m["type"].(string)

	if typ == "Feature" || typ == "FeatureCollection" {
		return nil, errBQ("Invalid GeoJSON: %s is not supported, pass a geometry", typ)
	}

	if typ == "GeometryCollection" {
		list, _ := m["geometries"].([]any)
		g := &geo{kind: gCollection}

		for _, e := range list {
			sub, err := geoFromJSON(e)
			if err != nil {
				return nil, err
			}

			g.subs = append(g.subs, sub)
		}

		return g, nil
	}

	return geoFromCoords(typ, m["coordinates"])
}

func jsonPos(v any) (lonLat, error) {
	arr, _ := v.([]any)
	if len(arr) != geoJSONPosLen {
		return lonLat{}, errBQ("Invalid GeoJSON: a position must have exactly two elements")
	}

	x, err1 := toFloat(jsonNum(arr[0]))
	y, err2 := toFloat(jsonNum(arr[1]))

	if err1 != nil || err2 != nil {
		return lonLat{}, errBQ("Invalid GeoJSON: positions must be numbers")
	}

	return lonLat{x, y}, nil
}

func jsonNum(v any) driver.Value {
	switch t := v.(type) {
	case float64:
		return t
	case jsonx.Number:
		f, err := t.Float64()
		if err != nil {
			return nil
		}

		return f
	}

	return nil
}

func jsonPositions(v any) ([]lonLat, error) {
	arr, _ := v.([]any)
	out := make([]lonLat, len(arr))

	for i, e := range arr {
		p, err := jsonPos(e)
		if err != nil {
			return nil, err
		}

		out[i] = p
	}

	return out, nil
}

func jsonRings(v any) ([][]lonLat, error) {
	arr, _ := v.([]any)
	out := make([][]lonLat, len(arr))

	for i, e := range arr {
		r, err := jsonPositions(e)
		if err != nil {
			return nil, err
		}

		out[i] = r
	}

	return out, nil
}

func geoFromCoords(typ string, c any) (*geo, error) {
	switch typ {
	case "Point":
		p, err := jsonPos(c)

		return &geo{kind: gPoint, pts: []lonLat{p}}, err
	case "MultiPoint":
		p, err := jsonPositions(c)

		return &geo{kind: gMultiPoint, pts: p}, err
	case "LineString":
		p, err := jsonPositions(c)

		return &geo{kind: gLine, lines: [][]lonLat{p}}, err
	case "MultiLineString":
		l, err := jsonRings(c)

		return &geo{kind: gMultiLine, lines: l}, err
	case "Polygon":
		r, err := jsonRings(c)

		return &geo{kind: gPolygon, polys: [][][]lonLat{r}}, err
	case "MultiPolygon":
		arr, _ := c.([]any)
		g := &geo{kind: gMultiPolygon}

		for _, e := range arr {
			r, err := jsonRings(e)
			if err != nil {
				return nil, err
			}

			g.polys = append(g.polys, r)
		}

		return g, nil
	}

	return nil, errBQ("Invalid GeoJSON: unsupported type %q", typ)
}

func jsonCoord(p lonLat) string { return "[" + fmtCoord(p.lon) + ", " + fmtCoord(p.lat) + "]" }

func jsonCoordList(pts []lonLat) string {
	parts := make([]string, len(pts))
	for i, p := range pts {
		parts[i] = jsonCoord(p)
	}

	return "[" + strings.Join(parts, ", ") + "]"
}

func jsonRingList(rings [][]lonLat) string {
	parts := make([]string, len(rings))
	for i, r := range rings {
		parts[i] = jsonCoordList(r)
	}

	return "[" + strings.Join(parts, ", ") + "]"
}

func (g *geo) geoJSON() string {
	wrap := func(typ, coords string) string {
		return `{ "type": "` + typ + `", "coordinates": ` + coords + ` }`
	}

	switch g.kind {
	case gPoint:
		return wrap("Point", jsonCoord(g.pts[0]))
	case gMultiPoint:
		return wrap("MultiPoint", jsonCoordList(g.pts))
	case gLine:
		return wrap("LineString", jsonCoordList(g.lines[0]))
	case gMultiLine:
		return wrap("MultiLineString", jsonRingList(g.lines))
	case gPolygon:
		return wrap("Polygon", jsonRingList(g.polys[0]))
	case gMultiPolygon:
		parts := make([]string, len(g.polys))
		for i, p := range g.polys {
			parts[i] = jsonRingList(p)
		}

		return wrap("MultiPolygon", "["+strings.Join(parts, ", ")+"]")
	case gCollection:
		parts := make([]string, len(g.subs))
		for i, s := range g.subs {
			parts[i] = s.geoJSON()
		}

		return `{ "type": "GeometryCollection", "geometries": [` + strings.Join(parts, ", ") + `] }`
	case gEmpty:
	}

	return `{ "type": "GeometryCollection", "geometries": [] }`
}

// udfMakeLine concatenates the vertices of its point or linestring arguments, dropping consecutive repeats.
func udfMakeLine(a []driver.Value) (driver.Value, error) {
	if len(a) == 1 && a[0] != nil {
		if arr, err := decodeJSONArray(a[0]); err == nil {
			a = make([]driver.Value, len(arr))
			for i, e := range arr {
				if s, ok := e.(string); ok {
					a[i] = s
				}
			}
		}
	}

	if anyNull(a) {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	var pts []lonLat

	for _, v := range a {
		g, err := loadGeo(v)
		if err != nil {
			return nil, err
		}

		switch g.kind {
		case gPoint:
			pts = appendDistinct(pts, g.pts...)
		case gLine:
			pts = appendDistinct(pts, g.lines[0]...)
		case gEmpty, gMultiPoint, gMultiLine, gPolygon, gMultiPolygon, gCollection:
			return nil, errBQ("ST_MAKELINE: each input must be a single point or linestring")
		}
	}

	if len(pts) == 1 {
		return (&geo{kind: gPoint, pts: pts}).wkt(), nil
	}

	if len(pts) == 0 {
		return (&geo{kind: gEmpty}).wkt(), nil
	}

	return (&geo{kind: gLine, lines: [][]lonLat{pts}}).wkt(), nil
}

func appendDistinct(dst []lonLat, add ...lonLat) []lonLat {
	for _, p := range add {
		if len(dst) == 0 || dst[len(dst)-1] != p {
			dst = append(dst, p)
		}
	}

	return dst
}

// udfMakePolygon builds a polygon from a closed linestring shell and an optional array of closed linestring holes.
func udfMakePolygon(a []driver.Value) (driver.Value, error) {
	if a[0] == nil {
		return nil, nil //nolint:nilnil // NULL in, NULL out
	}

	rings := make([][]lonLat, 0, argc2)

	shell, err := closedLine(a[0])
	if err != nil {
		return nil, err
	}

	rings = append(rings, shell)

	if len(a) > 1 && a[1] != nil {
		holes, herr := decodeJSONArray(a[1])
		if herr != nil {
			return nil, herr
		}

		for _, h := range holes {
			s, _ := h.(string)

			ring, rerr := closedLine(s)
			if rerr != nil {
				return nil, rerr
			}

			rings = append(rings, ring)
		}
	}

	if err = validatePolygon(rings); err != nil {
		return nil, err
	}

	return (&geo{kind: gPolygon, polys: [][][]lonLat{rings}}).wkt(), nil
}

func closedLine(v driver.Value) ([]lonLat, error) {
	g, err := loadGeo(v)
	if err != nil {
		return nil, err
	}

	if g.kind != gLine {
		return nil, errBQ("ST_MAKEPOLYGON: the shell and every hole must be a linestring")
	}

	l := g.lines[0]
	if l[0] != l[len(l)-1] {
		return nil, errBQ("ST_MAKEPOLYGON: Loop is not closed")
	}

	return l, nil
}

func hasNamedArgs(n *node) bool {
	for _, s := range n.names {
		if s != "" {
			return true
		}
	}

	return false
}

// namedArgs orders call arguments by params: positional ones first, then those given as name => value.
func namedArgs(n *node, params []string) ([]*node, error) {
	out := make([]*node, len(params))
	pos := 0

	for i, a := range n.args {
		name := ""
		if i < len(n.names) {
			name = n.names[i]
		}

		if name == "" {
			if pos >= len(params) {
				return nil, invalid("Too many arguments for function %s", n.name)
			}

			out[pos] = a
			pos++

			continue
		}

		idx := slices.Index(params, name)
		if idx < 0 || out[idx] != nil {
			return nil, invalid("Named argument %s is not valid for function %s", name, n.name)
		}

		out[idx] = a
	}

	return out, nil
}

// genNamed compiles a call whose parameters may be named; missing optional ones become the given defaults.
func genNamed(
	udf string, params, defaults []string, ret Type,
) func(c *compiler, n *node, sc *scope) (*gx, error) {
	return func(c *compiler, n *node, sc *scope) (*gx, error) {
		ordered, err := namedArgs(n, params)
		if err != nil {
			return nil, err
		}

		sqls := make([]string, len(params))

		for i, a := range ordered {
			if a == nil {
				if defaults[i] == "" {
					return nil, invalid("Missing argument %s for function %s", params[i], n.name)
				}

				sqls[i] = defaults[i]

				continue
			}

			g, gerr := c.expr(a, sc)
			if gerr != nil {
				return nil, gerr
			}

			sqls[i] = g.sql
		}

		name := "bq_" + udf
		if n.safe {
			name = "bq_safe_" + udf
		}

		return &gx{sql: name + "(" + strings.Join(sqls, ", ") + ")", typ: ret}, nil
	}
}

func addGeoFuncs(t map[string]*fnDef) {
	t["ST_GEOGPOINT"] = def("st_geogpoint", arityTwo, arityTwo, fixed(KindGeography))
	t["ST_GEOGFROMTEXT"] = custom(1, arityFour, genNamed("st_geogfromtext",
		[]string{"wkt", "oriented", "planar", "make_valid"}, []string{"", "0", "0", "0"}, scalar(KindGeography)))
	t["ST_GEOGFROMGEOJSON"] = custom(1, arityTwo, genNamed("st_geogfromgeojson",
		[]string{"geojson", "make_valid"}, []string{"", "0"}, scalar(KindGeography)))
	t["ST_ASTEXT"] = def("st_astext", 1, 1, fixed(KindString))
	t["ST_ASGEOJSON"] = def("st_asgeojson", 1, 1, fixed(KindString))
	t["ST_X"] = def("st_x", 1, 1, fixed(KindFloat))
	t["ST_Y"] = def("st_y", 1, 1, fixed(KindFloat))
	t["ST_DISTANCE"] = def("st_distance", arityTwo, arityThree, fixed(KindFloat))
	t["ST_DWITHIN"] = def("st_dwithin", arityThree, arityFour, fixed(KindBool))
	t["ST_LENGTH"] = def("st_length", 1, arityTwo, fixed(KindFloat))
	t["ST_AREA"] = def("st_area", 1, arityTwo, fixed(KindFloat))
	t["ST_CONTAINS"] = def("st_contains", arityTwo, arityTwo, fixed(KindBool))
	t["ST_WITHIN"] = def("st_within", arityTwo, arityTwo, fixed(KindBool))
	t["ST_INTERSECTS"] = def("st_intersects", arityTwo, arityTwo, fixed(KindBool))
	t["ST_MAKELINE"] = def("st_makeline", 1, arityTwo, fixed(KindGeography))
	t["ST_MAKEPOLYGON"] = def("st_makepolygon", 1, arityTwo, fixed(KindGeography))
	t["ST_CENTROID"] = def("st_centroid", 1, 1, fixed(KindGeography))
}

// castGeography is CAST(string AS GEOGRAPHY): WKT or a GeoJSON geometry.
func castGeography(s string) (driver.Value, error) {
	if strings.HasPrefix(strings.TrimSpace(s), "{") {
		return udfGeogFromGeoJSON([]driver.Value{s})
	}

	g, err := parseWKT(s)
	if err != nil {
		return nil, err
	}

	return g.wkt(), nil
}
