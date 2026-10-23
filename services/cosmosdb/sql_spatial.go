package cosmosdb

import (
	"fmt"
	"math"
)

const (
	geoPoint      = "Point"
	geoLineString = "LineString"
	geoPolygon    = "Polygon"

	earthRadiusMeters = 6371008.8
	degToRad          = math.Pi / 180
	minLineVertices   = 2
	minRingVertices   = 4
	maxLatitude       = 90
	maxLongitude      = 180
	geoEpsilon        = 1e-9
	halfDivisor       = 2
)

type geoPos [2]float64

// geoShape is a parsed GeoJSON Point, LineString or Polygon.
type geoShape struct {
	kind  string
	rings [][]geoPos // Point: one ring of one vertex; LineString: one ring; Polygon: exterior then holes
}

func (g geoShape) vertices() []geoPos {
	var out []geoPos
	for _, r := range g.rings {
		out = append(out, r...)
	}

	return out
}

// segments returns the edges of a line or polygon rings; a Point has none.
func (g geoShape) segments() [][2]geoPos {
	var out [][2]geoPos

	for _, r := range g.rings {
		for i := 1; i < len(r); i++ {
			out = append(out, [2]geoPos{r[i-1], r[i]})
		}
	}

	return out
}

func geoPosOf(v any) (geoPos, bool) {
	arr, ok := v.([]any)
	if !ok || len(arr) < minLineVertices {
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

		return geoShape{kind: kind, rings: [][]geoPos{{p}}}, pok
	case geoLineString:
		l, lok := geoPosList(coords)

		return geoShape{kind: kind, rings: [][]geoPos{l}}, lok
	case geoPolygon:
		arr, aok := coords.([]any)
		if !aok {
			return geoShape{}, false
		}

		rings := make([][]geoPos, len(arr))

		for i, r := range arr {
			if rings[i], ok = geoPosList(r); !ok {
				return geoShape{}, false
			}
		}

		return geoShape{kind: kind, rings: rings}, true
	default:
		return geoShape{}, false
	}
}

func haversine(a, b geoPos) float64 {
	dLat := (b[1] - a[1]) * degToRad
	dLon := (b[0] - a[0]) * degToRad
	h := math.Sin(dLat/halfDivisor)*math.Sin(dLat/halfDivisor) +
		math.Cos(a[1]*degToRad)*math.Cos(b[1]*degToRad)*math.Sin(dLon/halfDivisor)*math.Sin(dLon/halfDivisor)

	return halfDivisor * earthRadiusMeters * math.Asin(math.Min(1, math.Sqrt(h)))
}

// pointSegmentMeters is the distance from p to segment ab on a local tangent plane around p.
func pointSegmentMeters(p, a, b geoPos) float64 {
	cosLat := math.Cos(p[1] * degToRad)
	proj := func(q geoPos) (float64, float64) {
		return (q[0] - p[0]) * degToRad * earthRadiusMeters * cosLat, (q[1] - p[1]) * degToRad * earthRadiusMeters
	}

	ax, ay := proj(a)
	bx, by := proj(b)
	dx, dy := bx-ax, by-ay

	t := 0.0
	if l2 := dx*dx + dy*dy; l2 > 0 {
		t = math.Max(0, math.Min(1, -(ax*dx+ay*dy)/l2))
	}

	return math.Hypot(ax+t*dx, ay+t*dy)
}

func cross(o, a, b geoPos) float64 {
	return (a[0]-o[0])*(b[1]-o[1]) - (a[1]-o[1])*(b[0]-o[0])
}

func onSegment(p, a, b geoPos) bool {
	if math.Abs(cross(a, b, p)) > geoEpsilon {
		return false
	}

	return p[0] >= math.Min(a[0], b[0])-geoEpsilon && p[0] <= math.Max(a[0], b[0])+geoEpsilon &&
		p[1] >= math.Min(a[1], b[1])-geoEpsilon && p[1] <= math.Max(a[1], b[1])+geoEpsilon
}

func segmentsIntersect(s, t [2]geoPos) bool {
	d1, d2 := cross(t[0], t[1], s[0]), cross(t[0], t[1], s[1])
	d3, d4 := cross(s[0], s[1], t[0]), cross(s[0], s[1], t[1])

	if ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0)) {
		return true
	}

	return onSegment(s[0], t[0], t[1]) || onSegment(s[1], t[0], t[1]) ||
		onSegment(t[0], s[0], s[1]) || onSegment(t[1], s[0], s[1])
}

// segmentsCross reports a proper crossing (an interior point shared), which tangent touching is not.
func segmentsCross(s, t [2]geoPos) bool {
	d1, d2 := cross(t[0], t[1], s[0]), cross(t[0], t[1], s[1])
	d3, d4 := cross(s[0], s[1], t[0]), cross(s[0], s[1], t[1])

	return ((d1 > 0 && d2 < 0) || (d1 < 0 && d2 > 0)) && ((d3 > 0 && d4 < 0) || (d3 < 0 && d4 > 0))
}

func inRing(p geoPos, ring []geoPos) bool {
	inside := false

	for i, j := 0, len(ring)-1; i < len(ring); j, i = i, i+1 {
		if onSegment(p, ring[j], ring[i]) {
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
func inPolygon(p geoPos, poly geoShape) bool {
	if len(poly.rings) == 0 || !inRing(p, poly.rings[0]) {
		return false
	}

	for _, hole := range poly.rings[1:] {
		if inRing(p, hole) && !onRingBoundary(p, hole) {
			return false
		}
	}

	return true
}

func onRingBoundary(p geoPos, ring []geoPos) bool {
	for i := 1; i < len(ring); i++ {
		if onSegment(p, ring[i-1], ring[i]) {
			return true
		}
	}

	return false
}

func geoIntersects(a, b geoShape) bool {
	if a.kind == geoPolygon {
		for _, v := range b.vertices() {
			if inPolygon(v, a) {
				return true
			}
		}
	}

	if b.kind == geoPolygon {
		for _, v := range a.vertices() {
			if inPolygon(v, b) {
				return true
			}
		}
	}

	return anySegmentsMeet(a, b)
}

func anySegmentsMeet(a, b geoShape) bool {
	as, bs := a.segments(), b.segments()

	for _, s := range as {
		for _, t := range bs {
			if segmentsIntersect(s, t) {
				return true
			}
		}

		for _, v := range b.vertices() {
			if onSegment(v, s[0], s[1]) {
				return true
			}
		}
	}

	for _, t := range bs {
		for _, v := range a.vertices() {
			if onSegment(v, t[0], t[1]) {
				return true
			}
		}
	}

	return a.kind == geoPoint && b.kind == geoPoint && haversine(a.rings[0][0], b.rings[0][0]) == 0
}

func geoWithin(a, b geoShape) bool {
	if b.kind != geoPolygon {
		return a.kind == geoPoint && b.kind == geoPoint && a.rings[0][0] == b.rings[0][0]
	}

	for _, v := range a.vertices() {
		if !inPolygon(v, b) {
			return false
		}
	}

	for _, s := range a.segments() {
		for _, t := range b.segments() {
			if segmentsCross(s, t) {
				return false
			}
		}
	}

	return true
}

func geoDistance(a, b geoShape) float64 {
	if a.kind == geoPoint && b.kind == geoPoint {
		return haversine(a.rings[0][0], b.rings[0][0])
	}

	if geoIntersects(a, b) {
		return 0
	}

	best := math.Inf(1)
	best = math.Min(best, minVertexToShape(a, b))

	return math.Min(best, minVertexToShape(b, a))
}

// minVertexToShape is the least distance from any vertex of from to the vertices and edges of to.
func minVertexToShape(from, to geoShape) float64 {
	best := math.Inf(1)
	segs := to.segments()

	for _, v := range from.vertices() {
		for _, w := range to.vertices() {
			best = math.Min(best, haversine(v, w))
		}

		for _, s := range segs {
			best = math.Min(best, pointSegmentMeters(v, s[0], s[1]))
		}
	}

	return best
}

// geoValidity returns "" for a valid GeoJSON geometry, otherwise the reason it is not.
func geoValidity(v any) (string, bool) {
	obj, ok := v.(map[string]any)
	if !ok {
		return "", false
	}

	kind, _ := obj["type"].(string)

	switch kind {
	case geoPoint, geoLineString, geoPolygon:
	default:
		return "The GeoJSON type of the input is not supported or is missing.", true
	}

	g, parsed := parseGeoShape(obj)
	if !parsed {
		return "The coordinates of the GeoJSON input are malformed.", true
	}

	for _, p := range g.vertices() {
		if math.Abs(p[1]) > maxLatitude || math.Abs(p[0]) > maxLongitude {
			return fmt.Sprintf("Latitude values must be between -90 and 90 degrees. "+
				"Longitude values must be between -180 and 180 degrees. "+
				"Found longitude value '%v' and latitude value '%v'.", p[0], p[1]), true
		}
	}

	return structureReason(g), true
}

func structureReason(g geoShape) string {
	switch g.kind {
	case geoLineString:
		if len(g.rings[0]) < minLineVertices {
			return "The LineString input is not valid because it has fewer than two points."
		}
	case geoPolygon:
		for i, r := range g.rings {
			if len(r) < minRingVertices {
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

func callSpatial(name string, args []any) any {
	switch name {
	case biStIsValid, biStIsValidDetailed:
		reason, ok := geoValidity(args[0])
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

		switch name {
		case biStDistance:
			return floatResult(geoDistance(a, b))
		case biStWithin:
			return geoWithin(a, b)
		default:
			return geoIntersects(a, b)
		}
	}
}
