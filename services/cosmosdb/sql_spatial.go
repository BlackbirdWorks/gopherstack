package cosmosdb

import (
	"fmt"
	"math"
	"slices"
	"strings"
)

const (
	geoPoint        = "Point"
	geoLineString   = "LineString"
	geoPolygon      = "Polygon"
	geoMultiPolygon = "MultiPolygon"

	// WGS 84, the reference system the service uses for geography (Learn, "Geospatial data").
	wgsA          = 6378137.0
	wgsF          = 1 / 298.257223563
	degToRad      = math.Pi / 180
	minLineVertex = 2
	minRingVertex = 4
	maxLatitude   = 90
	maxLongitude  = 180
	geoEpsilon    = 1e-9
	arcEpsilon    = 1e-12
	halfDivisor   = 2
	vincentyIters = 200
	vincentyTol   = 1e-12
	meanRadius    = 6371008.8
)

type geoPos [2]float64

// geoShape is a parsed GeoJSON Point, LineString, Polygon or MultiPolygon. A polygon is a list of rings: the
// exterior first, then holes.
type geoShape struct {
	kind  string
	line  []geoPos
	polys [][][]geoPos
	point geoPos
}

func (g geoShape) polygonal() bool { return g.kind == geoPolygon || g.kind == geoMultiPolygon }

func (g geoShape) vertices() []geoPos {
	switch g.kind {
	case geoPoint:
		return []geoPos{g.point}
	case geoLineString:
		return g.line
	}

	var out []geoPos

	for _, poly := range g.polys {
		for _, r := range poly {
			out = append(out, r...)
		}
	}

	return out
}

// segments returns the edges of a line or polygon rings; a Point has none.
func (g geoShape) segments() [][2]geoPos {
	var out [][2]geoPos

	edges := func(pts []geoPos) {
		for i := 1; i < len(pts); i++ {
			out = append(out, [2]geoPos{pts[i-1], pts[i]})
		}
	}

	if g.kind == geoLineString {
		edges(g.line)
	}

	for _, poly := range g.polys {
		for _, r := range poly {
			edges(r)
		}
	}

	return out
}

func geoPosOf(v any) (geoPos, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) < minLineVertex {
		return geoPos{}, false
	}

	x, xok := toNumber(arr[0])
	y, yok := toNumber(arr[1])

	if !xok || !yok {
		return geoPos{}, false
	}

	return geoPos{numFloat(x), numFloat(y)}, true
}

func geoPosList(v any) ([]geoPos, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}

	out := make([]geoPos, len(arr))

	for i, e := range arr {
		if out[i], ok = geoPosOf(e); !ok {
			return nil, false
		}
	}

	return out, true
}

func geoRings(v any) ([][]geoPos, bool) {
	arr, ok := v.([]any)
	if !ok {
		return nil, false
	}

	rings := make([][]geoPos, len(arr))

	for i, r := range arr {
		if rings[i], ok = geoPosList(r); !ok {
			return nil, false
		}
	}

	return rings, true
}

// parseGeoShape decodes the supported GeoJSON geometry types; ok is false for anything else or malformed input.
func parseGeoShape(v any) (geoShape, bool) {
	obj, ok := v.(map[string]any)
	if !ok {
		return geoShape{}, false
	}

	kind, _ := obj["type"].(string)
	coords := obj["coordinates"]

	switch kind {
	case geoPoint:
		p, pok := geoPosOf(coords)

		return geoShape{kind: kind, point: p}, pok
	case geoLineString:
		l, lok := geoPosList(coords)

		return geoShape{kind: kind, line: l}, lok
	case geoPolygon:
		rings, rok := geoRings(coords)

		return geoShape{kind: kind, polys: [][][]geoPos{rings}}, rok
	case geoMultiPolygon:
		arr, aok := coords.([]any)
		if !aok {
			return geoShape{}, false
		}

		polys := make([][][]geoPos, len(arr))

		for i, p := range arr {
			if polys[i], ok = geoRings(p); !ok {
				return geoShape{}, false
			}
		}

		return geoShape{kind: kind, polys: polys}, true
	default:
		return geoShape{}, false
	}
}

// geoSpace is the geometry a query runs in: geography (WGS 84 sphere and ellipsoid) or the planar coordinate
// system of a Geometry container.
type geoSpace interface {
	dist(a, b geoPos) float64
	segDist(p, a, b geoPos) float64
	segsIntersect(s, t [2]geoPos) bool
	segsCross(s, t [2]geoPos) bool
	onSegment(p, a, b geoPos) bool
	inPolygon(p geoPos, rings [][]geoPos) bool
	area(rings [][]geoPos) float64
}

func spaceFor(geometry bool) geoSpace {
	if geometry {
		return planarSpace{}
	}

	return geographySpace{}
}

func inAnyPolygon(sp geoSpace, p geoPos, g geoShape) bool {
	return slices.ContainsFunc(g.polys, func(poly [][]geoPos) bool { return sp.inPolygon(p, poly) })
}

func geoIntersects(sp geoSpace, a, b geoShape) bool {
	if b.polygonal() {
		for _, v := range a.vertices() {
			if inAnyPolygon(sp, v, b) {
				return true
			}
		}
	}

	if a.polygonal() {
		for _, v := range b.vertices() {
			if inAnyPolygon(sp, v, a) {
				return true
			}
		}
	}

	return anySegmentsMeet(sp, a, b)
}

func anySegmentsMeet(sp geoSpace, a, b geoShape) bool {
	bs := b.segments()

	for _, s := range a.segments() {
		if slices.ContainsFunc(bs, func(t [2]geoPos) bool { return sp.segsIntersect(s, t) }) {
			return true
		}
	}

	for _, s := range a.segments() {
		if slices.ContainsFunc(b.vertices(), func(v geoPos) bool { return sp.onSegment(v, s[0], s[1]) }) {
			return true
		}
	}

	for _, t := range bs {
		if slices.ContainsFunc(a.vertices(), func(v geoPos) bool { return sp.onSegment(v, t[0], t[1]) }) {
			return true
		}
	}

	return a.kind == geoPoint && b.kind == geoPoint && sp.dist(a.point, b.point) == 0
}

func geoWithin(sp geoSpace, a, b geoShape) bool {
	if !b.polygonal() {
		return a.kind == geoPoint && b.kind == geoPoint && a.point == b.point
	}

	for _, v := range a.vertices() {
		if !inAnyPolygon(sp, v, b) {
			return false
		}
	}

	for _, s := range a.segments() {
		for _, t := range b.segments() {
			if sp.segsCross(s, t) {
				return false
			}
		}
	}

	return true
}

func geoDistance(sp geoSpace, a, b geoShape) float64 {
	if a.kind == geoPoint && b.kind == geoPoint {
		return sp.dist(a.point, b.point)
	}

	if geoIntersects(sp, a, b) {
		return 0
	}

	return math.Min(minVertexToShape(sp, a, b), minVertexToShape(sp, b, a))
}

// minVertexToShape is the least distance from any vertex of from to the vertices and edges of to.
func minVertexToShape(sp geoSpace, from, to geoShape) float64 {
	best := math.Inf(1)
	segs := to.segments()

	for _, v := range from.vertices() {
		for _, w := range to.vertices() {
			best = math.Min(best, sp.dist(v, w))
		}

		for _, s := range segs {
			best = math.Min(best, sp.segDist(v, s[0], s[1]))
		}
	}

	return best
}

// geoArea is the total area of a Polygon or MultiPolygon, zero for points and lines.
func geoArea(sp geoSpace, g geoShape) float64 {
	var total float64

	for _, poly := range g.polys {
		total += sp.area(poly)
	}

	return total
}

// --- planar (Geometry containers) ---

type planarSpace struct{}

func (planarSpace) dist(a, b geoPos) float64 { return math.Hypot(b[0]-a[0], b[1]-a[1]) }

func (planarSpace) segDist(p, a, b geoPos) float64 {
	dx, dy := b[0]-a[0], b[1]-a[1]

	t := 0.0
	if l2 := dx*dx + dy*dy; l2 > 0 {
		t = math.Max(0, math.Min(1, ((p[0]-a[0])*dx+(p[1]-a[1])*dy)/l2))
	}

	return math.Hypot(p[0]-(a[0]+t*dx), p[1]-(a[1]+t*dy))
}

func cross(o, a, b geoPos) float64 {
	return (a[0]-o[0])*(b[1]-o[1]) - (a[1]-o[1])*(b[0]-o[0])
}

func (planarSpace) onSegment(p, a, b geoPos) bool {
	if math.Abs(cross(a, b, p)) > geoEpsilon {
		return false
	}

	return p[0] >= math.Min(a[0], b[0])-geoEpsilon && p[0] <= math.Max(a[0], b[0])+geoEpsilon &&
		p[1] >= math.Min(a[1], b[1])-geoEpsilon && p[1] <= math.Max(a[1], b[1])+geoEpsilon
}

func (sp planarSpace) segsIntersect(s, t [2]geoPos) bool {
	if sp.segsCross(s, t) {
		return true
	}

	return sp.onSegment(s[0], t[0], t[1]) || sp.onSegment(s[1], t[0], t[1]) ||
		sp.onSegment(t[0], s[0], s[1]) || sp.onSegment(t[1], s[0], s[1])
}

// segsCross reports a proper crossing (an interior point shared), which tangent touching is not.
func (planarSpace) segsCross(s, t [2]geoPos) bool {
	d1, d2 := cross(t[0], t[1], s[0]), cross(t[0], t[1], s[1])
	d3, d4 := cross(s[0], s[1], t[0]), cross(s[0], s[1], t[1])

	return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))
}

func (sp planarSpace) inRing(p geoPos, ring []geoPos) bool {
	inside := false

	for i, j := 0, len(ring)-1; i < len(ring); j, i = i, i+1 {
		if sp.onSegment(p, ring[j], ring[i]) {
			return true
		}

		if (ring[i][1] > p[1]) != (ring[j][1] > p[1]) &&
			p[0] < (ring[j][0]-ring[i][0])*(p[1]-ring[i][1])/(ring[j][1]-ring[i][1])+ring[i][0] {
			inside = !inside
		}
	}

	return inside
}

// inPolygon is true for points inside the exterior ring and outside every hole (boundaries count as inside).
func (sp planarSpace) inPolygon(p geoPos, rings [][]geoPos) bool {
	if len(rings) == 0 || !sp.inRing(p, rings[0]) {
		return false
	}

	for _, hole := range rings[1:] {
		if sp.inRing(p, hole) && !onBoundary(sp, p, hole) {
			return false
		}
	}

	return true
}

func onBoundary(sp geoSpace, p geoPos, ring []geoPos) bool {
	for i := 1; i < len(ring); i++ {
		if sp.onSegment(p, ring[i-1], ring[i]) {
			return true
		}
	}

	return false
}

// area is the exterior's shoelace area less the holes', in squared coordinate units.
func (planarSpace) area(rings [][]geoPos) float64 {
	shoelace := func(r []geoPos) float64 {
		var s float64

		for i := 1; i < len(r); i++ {
			s += r[i-1][0]*r[i][1] - r[i][0]*r[i-1][1]
		}

		return math.Abs(s) / halfDivisor
	}

	var total float64

	for i, r := range rings {
		if i == 0 {
			total += shoelace(r)
		} else {
			total -= shoelace(r)
		}
	}

	return math.Max(total, 0)
}

// --- validity ---

// geoValidity returns "" for a valid GeoJSON geometry, otherwise the reason it is not. Geography coordinates must
// be longitude -180..180 and latitude -90..90; a Geometry container accepts any finite coordinate.
func geoValidity(v any, geometry bool) (string, bool) {
	obj, ok := v.(map[string]any)
	if !ok {
		return "", false
	}

	kind, _ := obj["type"].(string)

	switch kind {
	case geoPoint, geoLineString, geoPolygon, geoMultiPolygon:
	default:
		return "The GeoJSON type of the input is not supported or is missing.", true
	}

	g, parsed := parseGeoShape(obj)
	if !parsed {
		return "The coordinates of the GeoJSON input are malformed.", true
	}

	if !geometry {
		for _, p := range g.vertices() {
			if reason := rangeReason(p); reason != "" {
				return reason, true
			}
		}
	}

	return structureReason(g), true
}

// rangeReason names each coordinate axis that is out of range, in the service's wording.
func rangeReason(p geoPos) string {
	var parts []string

	if math.Abs(p[1]) > maxLatitude {
		parts = append(parts, "Latitude values must be between -90 and 90 degrees.")
	}

	if math.Abs(p[0]) > maxLongitude {
		parts = append(parts, "Longitude values must be between -180 and 180 degrees.")
	}

	return strings.Join(parts, " ")
}

func structureReason(g geoShape) string {
	if g.kind == geoLineString && len(g.line) < minLineVertex {
		return "The LineString input is not valid because it has fewer than two points."
	}

	for _, poly := range g.polys {
		for i, r := range poly {
			if len(r) < minRingVertex {
				return fmt.Sprintf(
					"The Polygon input is not valid because ring number %d has fewer than four points.",
					i+1,
				)
			}

			if r[0] != r[len(r)-1] {
				return fmt.Sprintf("The Polygon input is not valid because the start and end points of the ring "+
					"number %d are not the same. Each ring of a polygon must start and end at the same point.", i+1)
			}
		}
	}

	return ""
}

func callSpatial(name string, args []any, geometry bool) any {
	switch name {
	case biStIsValid, biStIsValidDetailed:
		reason, ok := geoValidity(args[0], geometry)
		if !ok {
			return undef{}
		}

		if name == biStIsValid {
			return reason == ""
		}

		if reason == "" {
			return map[string]any{"valid": true}
		}

		return map[string]any{"valid": false, "reason": reason}
	default:
		a, aok := parseGeoShape(args[0])
		b, bok := parseGeoShape(args[1])

		if !aok || !bok {
			return undef{}
		}

		sp := spaceFor(geometry)

		switch name {
		case biStDistance:
			return floatResult(geoDistance(sp, a, b))
		case biStWithin:
			return geoWithin(sp, a, b)
		default:
			return geoIntersects(sp, a, b)
		}
	}
}

// callSpatialArea is ST_AREA: square metres for geography, squared coordinate units for geometry; points and
// lines have no area.
func callSpatialArea(arg any, geometry bool) any {
	g, ok := parseGeoShape(arg)
	if !ok {
		return undef{}
	}

	return floatResult(geoArea(spaceFor(geometry), g))
}
