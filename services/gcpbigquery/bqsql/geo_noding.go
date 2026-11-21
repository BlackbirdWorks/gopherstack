package bqsql

import (
	"math"
	"sort"
)

// ST_MAKELINE returns a MULTILINESTRING when the snapped input has self-overlapping lines:
// https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/geography_functions
const (
	nodingMaxEdges  = 1024
	collinearBound  = 1e-15
	equatorLat      = 0
	nodingOddDegree = 1
	minLinePoints   = 3
)

type edgeKey struct{ a, b lonLat }

func orderedKey(a, b lonLat) edgeKey {
	if a.lon > b.lon || a.lon == b.lon && a.lat > b.lat {
		a, b = b, a
	}

	return edgeKey{a, b}
}

func onMeridian(pts ...lonLat) bool {
	for _, p := range pts[1:] {
		if p.lon != pts[0].lon {
			return false
		}
	}

	return true
}

func onEquator(pts ...lonLat) bool {
	for _, p := range pts {
		if p.lat != equatorLat {
			return false
		}
	}

	return true
}

func collinear3(a, b, c lonLat) bool {
	if onMeridian(a, b, c) || onEquator(a, b, c) {
		return true
	}

	va, vb, vc := a.vec(), b.vec(), c.vec()

	return math.Abs(va.dot(vb.cross(vc))) <= collinearBound
}

func between(x, a, b float64) bool { return x > math.Min(a, b) && x < math.Max(a, b) }

// strictlyInside reports whether x lies in the open arc a-b; x must be on the great circle of a-b.
func strictlyInside(x, a, b lonLat) bool {
	switch {
	case x == a || x == b:
		return false
	case onMeridian(a, b, x) && math.Abs(a.lat) != 90 && math.Abs(b.lat) != 90:
		return between(x.lat, a.lat, b.lat)
	case onEquator(a, b, x) && math.Abs(a.lon-b.lon) < halfTurn:
		return between(x.lon, a.lon, b.lon)
	}

	va, vb, vx := a.vec(), b.vec(), x.vec()
	n := va.cross(vb)

	return n.dot(va.cross(vx)) > 0 && n.dot(vx.cross(vb)) > 0
}

type lineEdge struct {
	splits []lonLat
	a, b   lonLat
}

// overlaps reports whether two edges lie on one great circle and share more than a single point.
func overlaps(e, f *lineEdge) bool {
	if orderedKey(e.a, e.b) == orderedKey(f.a, f.b) {
		return true
	}

	if !collinear3(e.a, e.b, f.a) || !collinear3(e.a, e.b, f.b) {
		return false
	}

	return strictlyInside(f.a, e.a, e.b) || strictlyInside(f.b, e.a, e.b) ||
		strictlyInside(e.a, f.a, f.b) || strictlyInside(e.b, f.a, f.b)
}

func (e *lineEdge) noteSplits(f *lineEdge) {
	for _, p := range []lonLat{f.a, f.b} {
		if strictlyInside(p, e.a, e.b) {
			e.splits = append(e.splits, p)
		}
	}
}

func (e *lineEdge) pieces() []edgeKey {
	if len(e.splits) == 0 {
		return []edgeKey{{e.a, e.b}}
	}

	va := e.a.vec()
	sort.Slice(e.splits, func(i, j int) bool { return angle(va, e.splits[i].vec()) < angle(va, e.splits[j].vec()) })

	out := make([]edgeKey, 0, len(e.splits)+1)
	prev := e.a

	for _, s := range e.splits {
		if s == prev {
			continue
		}

		out = append(out, edgeKey{prev, s})
		prev = s
	}

	return append(out, edgeKey{prev, e.b})
}

type nodeGraph struct {
	index map[lonLat]int
	order []lonLat
	adj   [][]int
}

func newNodeGraph(pts []lonLat) *nodeGraph {
	g := &nodeGraph{index: map[lonLat]int{}}

	for _, p := range pts {
		if _, ok := g.index[p]; !ok {
			g.index[p] = len(g.order)
			g.order = append(g.order, p)
		}
	}

	g.adj = make([][]int, len(g.order))

	return g
}

func (g *nodeGraph) link(a, b lonLat) {
	i, j := g.index[a], g.index[b]
	g.adj[i] = append(g.adj[i], j)
	g.adj[j] = append(g.adj[j], i)
}

// startNode prefers the lowest-numbered node of odd remaining degree so that a retraced path stays one line.
func (g *nodeGraph) startNode() int {
	first := -1

	for i, nb := range g.adj {
		if len(nb) == 0 {
			continue
		}

		if len(nb)%parityMod == nodingOddDegree {
			return i
		}

		if first < 0 {
			first = i
		}
	}

	return first
}

func (g *nodeGraph) removeEdge(i, j int) {
	g.adj[i] = dropFirst(g.adj[i], j)
	g.adj[j] = dropFirst(g.adj[j], i)
}

func dropFirst(s []int, v int) []int {
	for k, x := range s {
		if x == v {
			return append(s[:k], s[k+1:]...)
		}
	}

	return s
}

func (g *nodeGraph) walks() [][]lonLat {
	for _, nb := range g.adj {
		sort.Ints(nb)
	}

	var out [][]lonLat

	for cur := g.startNode(); cur >= 0; cur = g.startNode() {
		line := []lonLat{g.order[cur]}

		for len(g.adj[cur]) > 0 {
			next := g.adj[cur][0]
			g.removeEdge(cur, next)
			line = append(line, g.order[next])
			cur = next
		}

		out = append(out, line)
	}

	return out
}

// nodeLines collapses retraced and overlapping edges of the vertex chain pts. With no overlap it returns pts unchanged.
func nodeLines(pts []lonLat) [][]lonLat {
	if len(pts) < minLinePoints {
		return [][]lonLat{pts}
	}

	edges := make([]*lineEdge, len(pts)-1)
	for i := range edges {
		edges[i] = &lineEdge{a: pts[i], b: pts[i+1]}
	}

	overlap := markOverlaps(edges)

	if !overlap {
		return [][]lonLat{pts}
	}

	g := newNodeGraph(pts)
	done := make(map[edgeKey]bool, len(edges))

	for _, e := range edges {
		for _, p := range e.pieces() {
			k := orderedKey(p.a, p.b)
			if !done[k] {
				done[k] = true

				g.link(p.a, p.b)
			}
		}
	}

	return g.walks()
}

// markOverlaps records split points on overlapping edges and reports whether any edge repeats or overlaps another.
func markOverlaps(edges []*lineEdge) bool {
	seen := make(map[edgeKey]bool, len(edges))
	overlap := false

	for _, e := range edges {
		k := orderedKey(e.a, e.b)
		overlap = overlap || seen[k]
		seen[k] = true
	}

	if len(edges) > nodingMaxEdges {
		return overlap
	}

	for i, e := range edges {
		for _, f := range edges[i+1:] {
			if overlaps(e, f) {
				overlap = true

				e.noteSplits(f)
				f.noteSplits(e)
			}
		}
	}

	return overlap
}
