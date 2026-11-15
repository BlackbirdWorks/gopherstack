package bqsql

import (
	"fmt"
	"math"
	"strconv"
	"strings"
)

// GEOGRAPHY values are stored as WKT. Semantics follow
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions on a spherical Earth.

const (
	earthRadiusMeters = 6371010.0
	geoEps            = 1e-10
	maxRingCheck      = 2000
	minRingPoints     = 4
	fullSphere        = 4 * math.Pi
	triAreaScale      = 2
	parityMod         = 2
	halfTurn          = 180
	fullTurn          = 360
	degToRad          = math.Pi / halfTurn
	radToDeg          = halfTurn / math.Pi
	wktPoint          = "POINT"
	wktMultiPoint     = "MULTIPOINT"
	wktLine           = "LINESTRING"
	wktMultiLine      = "MULTILINESTRING"
	wktPolygon        = "POLYGON"
	wktMultiPolygon   = "MULTIPOLYGON"
	wktCollection     = "GEOMETRYCOLLECTION"
	wktEmpty          = "EMPTY"
)

type geoKind uint8

const (
	gEmpty geoKind = iota
	gPoint
	gMultiPoint
	gLine
	gMultiLine
	gPolygon
	gMultiPolygon
	gCollection
)

type lonLat struct{ lon, lat float64 }

type vec3 struct{ x, y, z float64 }

type geo struct {
	pts   []lonLat
	lines [][]lonLat
	polys [][][]lonLat
	subs  []*geo
	kind  geoKind
}

func (v vec3) dot(o vec3) float64 { return v.x*o.x + v.y*o.y + v.z*o.z }

func (v vec3) cross(o vec3) vec3 {
	return vec3{v.y*o.z - v.z*o.y, v.z*o.x - v.x*o.z, v.x*o.y - v.y*o.x}
}

func (v vec3) scale(k float64) vec3 { return vec3{v.x * k, v.y * k, v.z * k} }
func (v vec3) add(o vec3) vec3      { return vec3{v.x + o.x, v.y + o.y, v.z + o.z} }
func (v vec3) sub(o vec3) vec3      { return vec3{v.x - o.x, v.y - o.y, v.z - o.z} }
func (v vec3) norm() float64        { return math.Sqrt(v.dot(v)) }

func (v vec3) unit() vec3 {
	n := v.norm()
	if n == 0 {
		return v
	}

	return v.scale(1 / n)
}

func (p lonLat) vec() vec3 {
	lon, lat := p.lon*degToRad, p.lat*degToRad

	return vec3{math.Cos(lat) * math.Cos(lon), math.Cos(lat) * math.Sin(lon), math.Sin(lat)}
}

func (v vec3) lonLat() lonLat {
	return lonLat{math.Atan2(v.y, v.x) * radToDeg, math.Atan2(v.z, math.Hypot(v.x, v.y)) * radToDeg}
}

func angle(a, b vec3) float64 { return math.Atan2(a.cross(b).norm(), a.dot(b)) }

func (p lonLat) valid() error {
	if math.IsNaN(p.lon) || math.IsNaN(p.lat) || math.IsInf(p.lon, 0) || math.IsInf(p.lat, 0) {
		return errBQ("Invalid coordinate: %v %v", p.lon, p.lat)
	}

	if p.lat < -90 || p.lat > 90 {
		return errBQ("Invalid latitude %v: must be in [-90, 90]", p.lat)
	}

	if p.lon < -180 || p.lon > 180 {
		return errBQ("Invalid longitude %v: must be in [-180, 180]", p.lon)
	}

	return nil
}

func fmtCoord(x float64) string {
	r, err := strconv.ParseFloat(strconv.FormatFloat(x, 'g', 15, 64), 64)
	if err != nil {
		r = x
	}

	if r == 0 {
		r = 0
	}

	return strconv.FormatFloat(r, 'f', -1, 64)
}

func (p lonLat) wkt() string { return fmtCoord(p.lon) + " " + fmtCoord(p.lat) }

func wktList(pts []lonLat) string {
	parts := make([]string, len(pts))
	for i, p := range pts {
		parts[i] = p.wkt()
	}

	return strings.Join(parts, ", ")
}

func wktRings(rings [][]lonLat) string {
	parts := make([]string, len(rings))
	for i, r := range rings {
		parts[i] = "(" + wktList(r) + ")"
	}

	return strings.Join(parts, ", ")
}

func (g *geo) wkt() string {
	switch g.kind {
	case gPoint:
		return wktPoint + "(" + g.pts[0].wkt() + ")"
	case gMultiPoint:
		return wktMultiPoint + "(" + wktList(g.pts) + ")"
	case gLine:
		return wktLine + "(" + wktList(g.lines[0]) + ")"
	case gMultiLine:
		return wktMultiLine + "(" + wktRings(g.lines) + ")"
	case gPolygon:
		return wktPolygon + "(" + wktRings(g.polys[0]) + ")"
	case gMultiPolygon:
		parts := make([]string, len(g.polys))
		for i, p := range g.polys {
			parts[i] = "(" + wktRings(p) + ")"
		}

		return wktMultiPolygon + "(" + strings.Join(parts, ", ") + ")"
	case gCollection:
		parts := make([]string, len(g.subs))
		for i, s := range g.subs {
			parts[i] = s.wkt()
		}

		return wktCollection + "(" + strings.Join(parts, ", ") + ")"
	case gEmpty:
	}

	return wktCollection + " " + wktEmpty
}

type wktParser struct {
	src string
	pos int
}

func (p *wktParser) skip() {
	for p.pos < len(p.src) && strings.ContainsRune(" \t\r\n", rune(p.src[p.pos])) {
		p.pos++
	}
}

func (p *wktParser) peek() byte {
	p.skip()

	if p.pos >= len(p.src) {
		return 0
	}

	return p.src[p.pos]
}

func (p *wktParser) accept(c byte) bool {
	if p.peek() == c {
		p.pos++

		return true
	}

	return false
}

func (p *wktParser) word() string {
	p.skip()

	start := p.pos
	for p.pos < len(p.src) && isLetter(p.src[p.pos]) {
		p.pos++
	}

	return strings.ToUpper(p.src[start:p.pos])
}

func (p *wktParser) number() (float64, error) {
	p.skip()

	start := p.pos
	for p.pos < len(p.src) && strings.ContainsRune("+-.0123456789eE", rune(p.src[p.pos])) {
		p.pos++
	}

	v, err := strconv.ParseFloat(p.src[start:p.pos], 64)
	if err != nil {
		return 0, errBQ("Invalid WKT: expected a number at offset %d", start)
	}

	return v, nil
}

func (p *wktParser) coord() (lonLat, error) {
	x, err := p.number()
	if err != nil {
		return lonLat{}, err
	}

	y, err := p.number()
	if err != nil {
		return lonLat{}, err
	}

	return lonLat{x, y}, nil
}

func (p *wktParser) expect(c byte) error {
	if !p.accept(c) {
		return errBQ("Invalid WKT: expected %q at offset %d", c, p.pos)
	}

	return nil
}

func (p *wktParser) coordList() ([]lonLat, error) {
	if err := p.expect('('); err != nil {
		return nil, err
	}

	var out []lonLat

	for {
		wrapped := p.accept('(')

		c, err := p.coord()
		if err != nil {
			return nil, err
		}

		if wrapped {
			if err = p.expect(')'); err != nil {
				return nil, err
			}
		}

		out = append(out, c)

		if p.accept(')') {
			return out, nil
		}

		if err = p.expect(','); err != nil {
			return nil, err
		}
	}
}

func (p *wktParser) listOf(each func() error) error {
	if err := p.expect('('); err != nil {
		return err
	}

	for {
		if err := each(); err != nil {
			return err
		}

		if p.accept(')') {
			return nil
		}

		if err := p.expect(','); err != nil {
			return err
		}
	}
}

func (p *wktParser) geometry() (*geo, error) {
	kw := p.word()
	if kw == "" {
		return nil, errBQ("Invalid WKT: expected a geometry type at offset %d", p.pos)
	}

	save := p.pos

	if p.word() == wktEmpty {
		return &geo{kind: gEmpty}, nil
	}

	p.pos = save

	if p.peek() == 'Z' || p.peek() == 'M' {
		return nil, errBQ("Invalid WKT: Z and M coordinates are not supported")
	}

	switch kw {
	case wktPoint:
		c, err := p.coordList()
		if err != nil || len(c) != 1 {
			return nil, errBQ("Invalid WKT: a POINT needs exactly one coordinate")
		}

		return &geo{kind: gPoint, pts: c}, nil
	case wktMultiPoint:
		c, err := p.coordList()

		return &geo{kind: gMultiPoint, pts: c}, err
	case wktLine:
		c, err := p.coordList()

		return &geo{kind: gLine, lines: [][]lonLat{c}}, err
	case wktMultiLine:
		g := &geo{kind: gMultiLine}
		err := p.listOf(func() error {
			c, cerr := p.coordList()
			g.lines = append(g.lines, c)

			return cerr
		})

		return g, err
	case wktPolygon:
		rings, err := p.rings()

		return &geo{kind: gPolygon, polys: [][][]lonLat{rings}}, err
	case wktMultiPolygon:
		g := &geo{kind: gMultiPolygon}
		err := p.listOf(func() error {
			rings, rerr := p.rings()
			g.polys = append(g.polys, rings)

			return rerr
		})

		return g, err
	case wktCollection:
		g := &geo{kind: gCollection}
		err := p.listOf(func() error {
			sub, serr := p.geometry()
			g.subs = append(g.subs, sub)

			return serr
		})

		return g, err
	}

	return nil, errBQ("Invalid WKT: unknown geometry type %s", kw)
}

func (p *wktParser) rings() ([][]lonLat, error) {
	var out [][]lonLat

	err := p.listOf(func() error {
		c, cerr := p.coordList()
		out = append(out, c)

		return cerr
	})

	return out, err
}

func parseWKT(s string) (*geo, error) {
	p := &wktParser{src: s}

	g, err := p.geometry()
	if err != nil {
		return nil, err
	}

	if p.peek() != 0 {
		return nil, errBQ("Invalid WKT: unexpected input at offset %d", p.pos)
	}

	return g, g.validate()
}

func visitAll(pts []lonLat, visit func(lonLat) error) error {
	for _, p := range pts {
		if err := visit(p); err != nil {
			return err
		}
	}

	return nil
}

func (g *geo) allPoints(visit func(lonLat) error) error {
	if err := visitAll(g.pts, visit); err != nil {
		return err
	}

	for _, l := range g.lines {
		if err := visitAll(l, visit); err != nil {
			return err
		}
	}

	for _, poly := range g.polys {
		for _, r := range poly {
			if err := visitAll(r, visit); err != nil {
				return err
			}
		}
	}

	for _, s := range g.subs {
		if err := s.allPoints(visit); err != nil {
			return err
		}
	}

	return nil
}

func (g *geo) validate() error {
	if err := g.allPoints(lonLat.valid); err != nil {
		return err
	}

	for _, l := range g.lines {
		if len(l) < 2 { //nolint:mnd // a line needs two vertices
			return errBQ("Invalid geography: a LINESTRING needs at least two vertices")
		}
	}

	for _, poly := range g.polys {
		if err := validatePolygon(poly); err != nil {
			return err
		}
	}

	for _, s := range g.subs {
		if err := s.validate(); err != nil {
			return err
		}
	}

	return nil
}

func validatePolygon(rings [][]lonLat) error {
	if len(rings) == 0 {
		return errBQ("Invalid geography: a POLYGON needs a shell")
	}

	for _, r := range rings {
		if len(r) < minRingPoints || r[0] != r[len(r)-1] {
			return errBQ("Invalid geography: a polygon ring needs at least 4 vertices and must be closed")
		}

		if err := checkRingSimple(r); err != nil {
			return err
		}
	}

	return nil
}

func checkRingSimple(r []lonLat) error {
	n := len(r) - 1
	if n > maxRingCheck {
		return nil
	}

	for i := range n {
		if r[i] == r[i+1] {
			return errBQ("Invalid geography: duplicate vertices in a polygon ring")
		}
	}

	for i := range n {
		for j := i + 1; j < n; j++ {
			if j == i+1 || i == 0 && j == n-1 {
				continue
			}

			if arcsIntersect(r[i].vec(), r[i+1].vec(), r[j].vec(), r[j+1].vec()) {
				return errBQ("Invalid geography: polygon ring has crossing edges (loop %d intersects loop %d)", i, j)
			}
		}
	}

	return nil
}

// arcOn reports whether x, a point on the great circle of a-b, lies on the minor arc a-b.
func arcOn(a, b, x vec3) bool {
	n := a.cross(b)

	return a.cross(x).dot(n) >= -geoEps && x.cross(b).dot(n) >= -geoEps
}

func arcsIntersect(a, b, c, d vec3) bool {
	n1, n2 := a.cross(b), c.cross(d)
	x := n1.cross(n2)

	if x.norm() < geoEps {
		return onArc(a, c, d) || onArc(b, c, d) || onArc(c, a, b) || onArc(d, a, b)
	}

	x = x.unit()

	for _, cand := range []vec3{x, x.scale(-1)} {
		if arcOn(a, b, cand) && arcOn(c, d, cand) {
			return true
		}
	}

	return false
}

func onArc(p, a, b vec3) bool { return arcPointDist(p, a, b) < geoEps }

// arcPointDist is the angular distance from p to the minor arc a-b.
func arcPointDist(p, a, b vec3) float64 {
	n := a.cross(b)
	if n.norm() < geoEps {
		return angle(p, a)
	}

	n = n.unit()
	q := p.sub(n.scale(p.dot(n)))

	if q.norm() > geoEps && arcOn(a, b, q.unit()) {
		return math.Abs(math.Asin(math.Max(-1, math.Min(1, p.dot(n)))))
	}

	return math.Min(angle(p, a), angle(p, b))
}

func arcArcDist(a, b, c, d vec3) float64 {
	if arcsIntersect(a, b, c, d) {
		return 0
	}

	return math.Min(math.Min(arcPointDist(a, c, d), arcPointDist(b, c, d)),
		math.Min(arcPointDist(c, a, b), arcPointDist(d, a, b)))
}

func vecs(pts []lonLat) []vec3 {
	out := make([]vec3, len(pts))
	for i, p := range pts {
		out[i] = p.vec()
	}

	return out
}

// signedRingArea is the area (steradians) to the left of the ring, in [0, 4pi).
func signedRingArea(ring []lonLat) float64 {
	v := vecs(ring)
	total := 0.0

	for i := 1; i+1 < len(v)-1; i++ {
		total += triArea(v[0], v[i], v[i+1])
	}

	for total < 0 {
		total += fullSphere
	}

	return math.Mod(total, fullSphere)
}

// triArea is the signed spherical area of triangle abc (Van Oosterom and Strackee).
func triArea(a, b, c vec3) float64 {
	num := a.dot(b.cross(c))
	den := 1 + a.dot(b) + b.dot(c) + c.dot(a)

	return triAreaScale * math.Atan2(num, den)
}

// ringInterior returns the ring oriented so its interior (left side) is the smaller region, and that area.
func ringInterior(ring []lonLat) ([]lonLat, float64) {
	a := signedRingArea(ring)
	if a <= fullSphere/2 {
		return ring, a
	}

	out := make([]lonLat, len(ring))
	for i, p := range ring {
		out[len(ring)-1-i] = p
	}

	return out, fullSphere - a
}

type polyShape struct {
	shell []vec3
	holes [][]vec3
	out   vec3
}

func newPolyShape(rings [][]lonLat) *polyShape {
	shellPts, _ := ringInterior(rings[0])
	ps := &polyShape{shell: vecs(shellPts)}

	sum := vec3{}
	for _, v := range ps.shell {
		sum = sum.add(v)
	}

	if sum.norm() < geoEps {
		sum = vec3{0, 0, 1}.cross(ps.shell[0])
	}

	ps.out = sum.unit().scale(-1)

	for _, h := range rings[1:] {
		hp, _ := ringInterior(h)
		ps.holes = append(ps.holes, vecs(hp))
	}

	return ps
}

func ringCrossings(p, q vec3, ring []vec3) (int, bool) {
	count := 0

	for i := 0; i+1 < len(ring); i++ {
		if onArc(p, ring[i], ring[i+1]) {
			return 0, true
		}

		if arcsIntersect(p, q, ring[i], ring[i+1]) {
			count++
		}
	}

	return count, false
}

// locate classifies p against the polygon: 1 inside, 0 on the boundary, -1 outside.
func (ps *polyShape) locate(p vec3) int {
	n, onB := ringCrossings(p, ps.out, ps.shell)
	if onB {
		return 0
	}

	inside := n%parityMod == 1

	for _, h := range ps.holes {
		hn, hb := ringCrossings(p, h[0].scale(-1), h)
		if hb {
			return 0
		}

		if hn%2 == 1 {
			inside = false
		}
	}

	if inside {
		return 1
	}

	return -1
}

func (ps *polyShape) edges() [][2]vec3 {
	var out [][2]vec3

	for _, r := range append([][]vec3{ps.shell}, ps.holes...) {
		for i := 0; i+1 < len(r); i++ {
			out = append(out, [2]vec3{r[i], r[i+1]})
		}
	}

	return out
}

func (g *geo) flatten() ([]lonLat, [][]lonLat, []*polyShape) {
	var (
		pts   []lonLat
		lines [][]lonLat
		polys []*polyShape
	)

	switch g.kind {
	case gPoint, gMultiPoint:
		pts = g.pts
	case gLine, gMultiLine:
		lines = g.lines
	case gPolygon, gMultiPolygon:
		for _, p := range g.polys {
			polys = append(polys, newPolyShape(p))
		}
	case gCollection:
		for _, s := range g.subs {
			p, l, pg := s.flatten()
			pts, lines, polys = append(pts, p...), append(lines, l...), append(polys, pg...)
		}
	case gEmpty:
	}

	return pts, lines, polys
}

func (g *geo) isEmpty() bool {
	pts, lines, polys := g.flatten()

	return len(pts)+len(lines)+len(polys) == 0
}

// dim is the highest dimension present: 0 points, 1 lines, 2 polygons, -1 empty.
func (g *geo) dim() int {
	pts, lines, polys := g.flatten()

	switch {
	case len(polys) > 0:
		return 2 //nolint:mnd // polygon dimension
	case len(lines) > 0:
		return 1
	case len(pts) > 0:
		return 0
	}

	return -1
}

func distance(a, b *geo) float64 {
	ap, al, apg := a.flatten()
	bp, bl, bpg := b.flatten()
	best := math.Inf(1)

	upd := func(d float64) { best = math.Min(best, d) }

	for _, p := range ap {
		upd(pointToShapes(p.vec(), bp, bl, bpg))
	}

	for _, p := range bp {
		upd(pointToShapes(p.vec(), ap, al, apg))
	}

	for _, l := range al {
		for _, m := range bl {
			upd(linesDist(l, m))
		}

		for _, pg := range bpg {
			upd(lineToPoly(l, pg))
		}
	}

	for _, l := range bl {
		for _, pg := range apg {
			upd(lineToPoly(l, pg))
		}
	}

	for _, p := range apg {
		for _, q := range bpg {
			upd(polyToPoly(p, q))
		}
	}

	return best
}

func pointToShapes(p vec3, pts []lonLat, lines [][]lonLat, polys []*polyShape) float64 {
	best := math.Inf(1)

	for _, q := range pts {
		best = math.Min(best, angle(p, q.vec()))
	}

	for _, l := range lines {
		v := vecs(l)
		for i := 0; i+1 < len(v); i++ {
			best = math.Min(best, arcPointDist(p, v[i], v[i+1]))
		}
	}

	for _, pg := range polys {
		if pg.locate(p) >= 0 {
			return 0
		}

		for _, e := range pg.edges() {
			best = math.Min(best, arcPointDist(p, e[0], e[1]))
		}
	}

	return best
}

func linesDist(l, m []lonLat) float64 {
	a, b := vecs(l), vecs(m)
	best := math.Inf(1)

	for i := 0; i+1 < len(a); i++ {
		for j := 0; j+1 < len(b); j++ {
			best = math.Min(best, arcArcDist(a[i], a[i+1], b[j], b[j+1]))
		}
	}

	return best
}

func lineToPoly(l []lonLat, pg *polyShape) float64 {
	v := vecs(l)
	best := math.Inf(1)

	for _, p := range v {
		if pg.locate(p) >= 0 {
			return 0
		}
	}

	for i := 0; i+1 < len(v); i++ {
		for _, e := range pg.edges() {
			best = math.Min(best, arcArcDist(v[i], v[i+1], e[0], e[1]))
		}
	}

	return best
}

func polyToPoly(p, q *polyShape) float64 {
	pIn, qIn := q.locate(p.shell[0]) >= 0, p.locate(q.shell[0]) >= 0

	if pIn && p.locate(p.shell[0]) >= 0 || qIn && q.locate(q.shell[0]) >= 0 {
		return 0
	}

	best := math.Inf(1)

	for _, e := range p.edges() {
		for _, f := range q.edges() {
			best = math.Min(best, arcArcDist(e[0], e[1], f[0], f[1]))
		}
	}

	return best
}

func (g *geo) length() float64 {
	_, lines, _ := g.flatten()
	total := 0.0

	for _, l := range lines {
		v := vecs(l)
		for i := 0; i+1 < len(v); i++ {
			total += angle(v[i], v[i+1])
		}
	}

	return total * earthRadiusMeters
}

func (g *geo) area() float64 {
	total := 0.0

	for _, poly := range g.polygonRings() {
		_, a := ringInterior(poly[0])
		for _, h := range poly[1:] {
			_, ha := ringInterior(h)
			a -= ha
		}

		total += a
	}

	return total * earthRadiusMeters * earthRadiusMeters
}

func (g *geo) polygonRings() [][][]lonLat {
	out := append([][][]lonLat{}, g.polys...)
	for _, s := range g.subs {
		out = append(out, s.polygonRings()...)
	}

	return out
}

func intersects(a, b *geo) bool {
	if a.isEmpty() || b.isEmpty() {
		return false
	}

	return distance(a, b) < geoEps
}

// contains: no point of b lies outside a and the interiors intersect.
func contains(a, b *geo) bool {
	if a.isEmpty() || b.isEmpty() {
		return false
	}

	ap, al, apg := a.flatten()
	samples, interior := b.samples()

	anyInterior := false

	for _, p := range samples {
		loc := locateIn(p, ap, al, apg)
		if loc < 0 {
			return false
		}

		if loc > 0 && interior(p) {
			anyInterior = true
		}
	}

	return anyInterior && !boundaryCrossing(a, b)
}

// locateIn is 1 when p is in the interior of the shapes, 0 on a boundary, -1 outside.
func locateIn(p vec3, pts []lonLat, lines [][]lonLat, polys []*polyShape) int {
	best := -1

	for _, pg := range polys {
		switch pg.locate(p) {
		case 1:
			return 1
		case 0:
			best = 0
		}
	}

	for _, l := range lines {
		v := vecs(l)

		for i := 0; i+1 < len(v); i++ {
			if onArc(p, v[i], v[i+1]) {
				if angle(p, v[0]) < geoEps || angle(p, v[len(v)-1]) < geoEps {
					best = max(best, 0)
				} else {
					return 1
				}
			}
		}
	}

	for _, q := range pts {
		if angle(p, q.vec()) < geoEps {
			return 1
		}
	}

	return best
}

// samples returns vertices plus edge midpoints of b, and a predicate that is true for points of b's interior.
func (g *geo) samples() ([]vec3, func(vec3) bool) {
	pts, lines, polys := g.flatten()

	var out []vec3

	for _, p := range pts {
		out = append(out, p.vec())
	}

	for _, l := range lines {
		v := vecs(l)

		out = append(out, v...)
		for i := 0; i+1 < len(v); i++ {
			out = append(out, v[i].add(v[i+1]).unit())
		}
	}

	for _, pg := range polys {
		for _, e := range pg.edges() {
			out = append(out, e[0], e[0].add(e[1]).unit())
		}

		out = append(out, polyInteriorPoint(pg))
	}

	isInterior := func(vec3) bool { return true }

	return out, isInterior
}

// polyInteriorPoint finds a point strictly inside the polygon by scanning edge midpoints nudged inward.
func polyInteriorPoint(pg *polyShape) vec3 {
	for _, e := range pg.edges() {
		mid := e[0].add(e[1]).unit()
		left := e[0].cross(e[1]).unit()

		for _, step := range []float64{1e-3, 1e-5, 1e-7} {
			p := mid.add(left.scale(step)).unit()
			if pg.locate(p) == 1 {
				return p
			}
		}
	}

	return pg.shell[0]
}

// boundaryCrossing reports whether an edge of b properly crosses the boundary of a polygon in a.
func boundaryCrossing(a, b *geo) bool {
	_, _, apg := a.flatten()
	_, bl, bpg := b.flatten()

	var bEdges [][2]vec3

	for _, l := range bl {
		v := vecs(l)
		for i := 0; i+1 < len(v); i++ {
			bEdges = append(bEdges, [2]vec3{v[i], v[i+1]})
		}
	}

	for _, pg := range bpg {
		bEdges = append(bEdges, pg.edges()...)
	}

	for _, pg := range apg {
		for _, e := range pg.edges() {
			for _, f := range bEdges {
				if properCross(e[0], e[1], f[0], f[1]) {
					return true
				}
			}
		}
	}

	return false
}

func properCross(a, b, c, d vec3) bool {
	if !arcsIntersect(a, b, c, d) {
		return false
	}

	for _, p := range []vec3{a, b} {
		if onArc(p, c, d) {
			return false
		}
	}

	for _, p := range []vec3{c, d} {
		if onArc(p, a, b) {
			return false
		}
	}

	return true
}

func (g *geo) centroid() *geo {
	pts, lines, polys := g.flatten()

	switch {
	case len(polys) > 0:
		return polyCentroid(g.polygonRings())
	case len(lines) > 0:
		sum := vec3{}

		for _, l := range lines {
			v := vecs(l)
			for i := 0; i+1 < len(v); i++ {
				sum = sum.add(v[i].add(v[i+1]).unit().scale(angle(v[i], v[i+1])))
			}
		}

		return pointGeo(sum)
	case len(pts) > 0:
		sum := vec3{}
		for _, p := range pts {
			sum = sum.add(p.vec())
		}

		return pointGeo(sum)
	}

	return &geo{kind: gEmpty}
}

func polyCentroid(polys [][][]lonLat) *geo {
	sum := vec3{}

	for _, poly := range polys {
		for i, ring := range poly {
			sign := 1.0
			if i > 0 {
				sign = -1
			}

			oriented, _ := ringInterior(ring)
			v := vecs(oriented)

			for j := 1; j+1 < len(v)-1; j++ {
				a := triArea(v[0], v[j], v[j+1])
				sum = sum.add(v[0].add(v[j]).add(v[j+1]).scale(sign * a))
			}
		}
	}

	return pointGeo(sum)
}

func pointGeo(v vec3) *geo {
	if v.norm() < geoEps {
		return &geo{kind: gEmpty}
	}

	return &geo{kind: gPoint, pts: []lonLat{v.unit().lonLat()}}
}

func makePoint(lon, lat float64) (*geo, error) {
	if math.IsNaN(lon) || math.IsInf(lon, 0) || math.IsNaN(lat) || math.IsInf(lat, 0) {
		return nil, errBQ("ST_GEOGPOINT: coordinates must be finite")
	}

	if lat < -90 || lat > 90 {
		return nil, errBQ("ST_GEOGPOINT: latitude %v is out of range [-90, 90]", lat)
	}

	lon = math.Mod(lon, fullTurn)
	if lon > halfTurn {
		lon -= fullTurn
	} else if lon < -halfTurn {
		lon += fullTurn
	}

	return &geo{kind: gPoint, pts: []lonLat{{lon, lat}}}, nil
}

func (g *geo) singlePoint(fn string) (lonLat, error) {
	if g.kind != gPoint {
		return lonLat{}, errBQ("%s can only be called on a single point geography, but got %s", fn, g.describe())
	}

	return g.pts[0], nil
}

func (g *geo) describe() string {
	if g.isEmpty() {
		return "an empty geography"
	}

	return fmt.Sprintf("a geography of dimension %d", g.dim())
}

func isLetter(c byte) bool { return c >= 'A' && c <= 'Z' || c >= 'a' && c <= 'z' }
