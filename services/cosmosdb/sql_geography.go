package cosmosdb

import (
	"math"
)

// geographySpace computes on the WGS 84 earth: geodesic distances on the ellipsoid (Vincenty), edges as great-circle
// arcs, polygon membership by the interior-on-the-left rule (a clockwise ring is the complement of the region it
// encloses, per the geospatial page) and areas on the authalic sphere. Against the Learn examples this gives
// 3.34526974 km for the documented 3.34526982 km distance and 735,970,283.11 m2 for the documented 735,970,283.05.
type geographySpace struct{}

type vec3 [3]float64

func (a vec3) dot(b vec3) float64 { return a[0]*b[0] + a[1]*b[1] + a[2]*b[2] }

func (a vec3) cross(b vec3) vec3 {
	return vec3{a[1]*b[2] - a[2]*b[1], a[2]*b[0] - a[0]*b[2], a[0]*b[1] - a[1]*b[0]}
}

func (a vec3) scale(k float64) vec3 { return vec3{a[0] * k, a[1] * k, a[2] * k} }

func (a vec3) sub(b vec3) vec3 { return vec3{a[0] - b[0], a[1] - b[1], a[2] - b[2]} }

func (a vec3) norm() float64 { return math.Sqrt(a.dot(a)) }

func (a vec3) unit() vec3 {
	n := a.norm()
	if n == 0 {
		return a
	}

	return a.scale(1 / n)
}

func vecOf(p geoPos) vec3 {
	lon, lat := p[0]*degToRad, p[1]*degToRad

	return vec3{math.Cos(lat) * math.Cos(lon), math.Cos(lat) * math.Sin(lon), math.Sin(lat)}
}

func posOf(v vec3) geoPos {
	return geoPos{math.Atan2(v[1], v[0]) / degToRad, math.Asin(math.Max(-1, math.Min(1, v[2]))) / degToRad}
}

// dist is the geodesic distance in metres on the WGS 84 ellipsoid.
func (geographySpace) dist(a, b geoPos) float64 {
	if a == b {
		return 0
	}

	if d, ok := vincenty(a, b); ok {
		return d
	}

	return haversine(a, b)
}

func haversine(a, b geoPos) float64 {
	dLat := (b[1] - a[1]) * degToRad
	dLon := (b[0] - a[0]) * degToRad
	h := math.Sin(dLat/halfDivisor)*math.Sin(dLat/halfDivisor) +
		math.Cos(a[1]*degToRad)*math.Cos(b[1]*degToRad)*math.Sin(dLon/halfDivisor)*math.Sin(dLon/halfDivisor)

	return halfDivisor * meanRadius * math.Asin(math.Min(1, math.Sqrt(h)))
}

// Coefficients of Vincenty's inverse formulae (Vincenty 1975).
const (
	vincLambdaDiv = 16.0
	vincLambdaK   = 4.0
	vincLambdaK2  = 3.0
	vincADiv      = 16384.0
	vincAK0       = 4096.0
	vincAK1       = -768.0
	vincAK2       = 320.0
	vincAK3       = -175.0
	vincBDiv      = 1024.0
	vincBK0       = 256.0
	vincBK1       = -128.0
	vincBK2       = 74.0
	vincBK3       = -47.0
	vincBQuarter  = 4.0
	vincBSixth    = 6.0
	vincBSq       = 3.0
	vincBSq2      = 4.0
	doubleAngle   = 2.0
	minRingPoints = 3
)

// vincenty is the inverse geodesic problem on the ellipsoid; ok is false for near-antipodal points where the
// iteration does not converge.
func vincenty(p1, p2 geoPos) (float64, bool) {
	b := (1 - wgsF) * wgsA
	l := (p2[0] - p1[0]) * degToRad
	u1 := math.Atan((1 - wgsF) * math.Tan(p1[1]*degToRad))
	u2 := math.Atan((1 - wgsF) * math.Tan(p2[1]*degToRad))
	sU1, cU1, sU2, cU2 := math.Sin(u1), math.Cos(u1), math.Sin(u2), math.Cos(u2)
	lam := l

	for range vincentyIters {
		sl, cl := math.Sin(lam), math.Cos(lam)
		ss := math.Hypot(cU2*sl, cU1*sU2-sU1*cU2*cl)

		if ss == 0 {
			return 0, true
		}

		cs := sU1*sU2 + cU1*cU2*cl
		sig := math.Atan2(ss, cs)
		sa := cU1 * cU2 * sl / ss
		c2a := 1 - sa*sa

		c2sm := 0.0
		if c2a != 0 {
			c2sm = cs - doubleAngle*sU1*sU2/c2a
		}

		c := wgsF / vincLambdaDiv * c2a * (vincLambdaK + wgsF*(vincLambdaK-vincLambdaK2*c2a))
		prev := lam
		lam = l + (1-c)*wgsF*sa*(sig+c*ss*(c2sm+c*cs*(-1+doubleAngle*c2sm*c2sm)))

		if math.Abs(lam-prev) < vincentyTol {
			u2sq := c2a * (wgsA*wgsA - b*b) / (b * b)
			a := 1 + u2sq/vincADiv*(vincAK0+u2sq*(vincAK1+u2sq*(vincAK2+vincAK3*u2sq)))
			bb := u2sq / vincBDiv * (vincBK0 + u2sq*(vincBK1+u2sq*(vincBK2+vincBK3*u2sq)))
			inner := cs*(-1+doubleAngle*c2sm*c2sm) -
				bb/vincBSixth*c2sm*(-vincBSq+vincBSq2*ss*ss)*(-vincBSq+vincBSq2*c2sm*c2sm)
			ds := bb * ss * (c2sm + bb/vincBQuarter*inner)

			return b * a * (sig - ds), true
		}
	}

	return 0, false
}

func (geographySpace) onSegment(p, a, b geoPos) bool {
	return onArc(vecOf(p), vecOf(a), vecOf(b))
}

// onArc reports whether p lies on the great-circle arc a-b.
func onArc(p, a, b vec3) bool {
	n := a.cross(b)
	if n.norm() < arcEpsilon {
		return p.sub(a).norm() < geoEpsilon
	}

	n = n.unit()
	if math.Abs(p.dot(n)) > geoEpsilon {
		return false
	}

	return a.cross(p).dot(n) >= -geoEpsilon && p.cross(b).dot(n) >= -geoEpsilon
}

func (sp geographySpace) segsIntersect(s, t [2]geoPos) bool {
	a1, a2, b1, b2 := vecOf(s[0]), vecOf(s[1]), vecOf(t[0]), vecOf(t[1])

	if onArc(a1, b1, b2) || onArc(a2, b1, b2) || onArc(b1, a1, a2) || onArc(b2, a1, a2) {
		return true
	}

	d := a1.cross(a2).cross(b1.cross(b2))
	if d.norm() < arcEpsilon {
		return false
	}

	d = d.unit()

	return (onArc(d, a1, a2) && onArc(d, b1, b2)) || (onArc(d.scale(-1), a1, a2) && onArc(d.scale(-1), b1, b2))
}

// segsCross reports a proper crossing: the arcs meet at a point interior to both.
func (sp geographySpace) segsCross(s, t [2]geoPos) bool {
	a1, a2, b1, b2 := vecOf(s[0]), vecOf(s[1]), vecOf(t[0]), vecOf(t[1])

	d := a1.cross(a2).cross(b1.cross(b2))
	if d.norm() < arcEpsilon {
		return false
	}

	d = d.unit()

	for _, c := range []vec3{d, d.scale(-1)} {
		if onArc(c, a1, a2) && onArc(c, b1, b2) && !endpoint(c, a1, a2, b1, b2) {
			return true
		}
	}

	return false
}

func endpoint(c vec3, ends ...vec3) bool {
	for _, e := range ends {
		if c.sub(e).norm() < geoEpsilon {
			return true
		}
	}

	return false
}

// segDist is the distance from p to the great-circle arc a-b: the geodesic distance to the arc's closest point.
func (sp geographySpace) segDist(p, a, b geoPos) float64 {
	pv, av, bv := vecOf(p), vecOf(a), vecOf(b)

	n := av.cross(bv)
	if n.norm() < arcEpsilon {
		return sp.dist(p, a)
	}

	n = n.unit()
	q := pv.sub(n.scale(pv.dot(n)))

	if q.norm() > arcEpsilon {
		q = q.unit()
		if onArc(q, av, bv) {
			return sp.dist(p, posOf(q))
		}
	}

	return math.Min(sp.dist(p, a), sp.dist(p, b))
}

// ringSignedArea is the signed spherical area of the ring in steradians, normalised to (-2pi, 2pi]: positive for a
// ring that runs counter-clockwise around the smaller region it bounds.
func ringSignedArea(ring []geoPos, lat func(float64) float64) float64 {
	n := len(ring)
	if n > 1 && ring[0] == ring[n-1] {
		n--
	}

	if n < minRingPoints {
		return 0
	}

	vs := make([]vec3, n)
	for i := range n {
		vs[i] = vecOf(geoPos{ring[i][0], lat(ring[i][1])})
	}

	var sum float64

	for i := 1; i < n-1; i++ {
		num := vs[0].dot(vs[i].cross(vs[i+1]))
		den := 1 + vs[0].dot(vs[i]) + vs[i].dot(vs[i+1]) + vs[i+1].dot(vs[0])
		sum += doubleAngle * math.Atan2(num, den)
	}

	const fourPi = 4 * math.Pi

	sum = math.Mod(sum, fourPi)

	switch {
	case sum > doubleAngle*math.Pi:
		sum -= fourPi
	case sum <= -doubleAngle*math.Pi:
		sum += fourPi
	}

	return sum
}

func identityLat(l float64) float64 { return l }

// windingAround sums the signed azimuth turns of the ring seen from p.
func windingAround(p vec3, ring []geoPos) float64 {
	var sum float64

	tangent := func(v vec3) vec3 { return v.sub(p.scale(v.dot(p))) }

	for i := 1; i < len(ring); i++ {
		a, b := tangent(vecOf(ring[i-1])), tangent(vecOf(ring[i]))
		if a.norm() < arcEpsilon || b.norm() < arcEpsilon {
			continue
		}

		sum += math.Atan2(a.cross(b).dot(p), a.dot(b))
	}

	return sum
}

// leftOf reports whether p is in the region to the left of the ring: the smaller bounded region for a
// counter-clockwise ring, everything else for a clockwise one.
func leftOf(p vec3, ring []geoPos) bool {
	turns := math.Round(windingAround(p, ring) / (doubleAngle * math.Pi))

	if ringSignedArea(ring, identityLat) >= 0 {
		return turns == 1
	}

	return turns != -1
}

// inPolygon: on a ring boundary, or to the left of every ring (holes run clockwise).
func (sp geographySpace) inPolygon(p geoPos, rings [][]geoPos) bool {
	if len(rings) == 0 {
		return false
	}

	for _, r := range rings {
		if onBoundary(sp, p, r) {
			return true
		}
	}

	pv := vecOf(p)

	for _, r := range rings {
		if !leftOf(pv, r) {
			return false
		}
	}

	return true
}

// area is the polygon's area in square metres: each ring's smaller bounded region on the authalic sphere, the
// exterior less the holes.
func (geographySpace) area(rings [][]geoPos) float64 {
	rq := authalicRadius()

	var total float64

	for i, r := range rings {
		a := math.Abs(ringSignedArea(r, authalicLatitude)) * rq * rq
		if i == 0 {
			total += a
		} else {
			total -= a
		}
	}

	return math.Max(total, 0)
}

const wgsE2 = wgsF * (doubleAngle - wgsF)

func authalicQ(phi float64) float64 {
	e := math.Sqrt(wgsE2)
	s := math.Sin(phi)

	return (1 - wgsE2) * (s/(1-wgsE2*s*s) - (1/(doubleAngle*e))*math.Log((1-e*s)/(1+e*s)))
}

func authalicRadius() float64 { return wgsA * math.Sqrt(authalicQ(math.Pi/doubleAngle)/doubleAngle) }

// authalicLatitude maps a geodetic latitude in degrees to the latitude on the equal-area sphere.
func authalicLatitude(deg float64) float64 {
	return math.Asin(authalicQ(deg*degToRad)/authalicQ(math.Pi/doubleAngle)) / degToRad
}
