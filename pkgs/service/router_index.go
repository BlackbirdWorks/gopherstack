package service

import (
	"math"
	"strings"
)

// routeIndex buckets service positions (priority order) by the first byte a gate prefix
// requires, so a request only visits services whose gates could pass. Immutable once built.
type routeIndex struct {
	byCBOR   map[string][]int
	byPath   [256][]int
	byPathPf [256][][]string
	byTarget [256][]int
	always   []int
}

// validGate returns prefixes unchanged if each is at least minLen bytes (and, for
// path gates, starts with "/"); otherwise nil, leaving the service ungated.
func validGate(prefixes []string, minLen int) []string {
	for _, p := range prefixes {
		if len(p) < minLen || (minLen == minPathGateLen && !strings.HasPrefix(p, "/")) {
			return nil
		}
	}

	return prefixes
}

func buildRouteIndex(targetGates, pathGates [][]string, cborGates []string) routeIndex {
	idx := routeIndex{byCBOR: map[string][]int{}}

	for i := range targetGates {
		switch {
		case len(pathGates[i]) > 0:
			addKeyedPrefixes(&idx, i, pathGates[i])
		case len(targetGates[i]) > 0:
			addKeyed(&idx.byTarget, i, targetGates[i], 0)

			if cborGates[i] != "" {
				idx.byCBOR[cborGates[i]] = append(idx.byCBOR[cborGates[i]], i)
			}
		default:
			idx.always = append(idx.always, i)
		}
	}

	return idx
}

func addKeyed(buckets *[256][]int, pos int, prefixes []string, keyAt int) {
	var seen [256]bool

	for _, p := range prefixes {
		if k := p[keyAt]; !seen[k] {
			seen[k] = true
			buckets[k] = append(buckets[k], pos)
		}
	}
}

// addKeyedPrefixes buckets pos under the second byte of each path prefix, recording per
// bucket only the prefixes sharing that byte: they are all a gate check needs there.
func addKeyedPrefixes(idx *routeIndex, pos int, prefixes []string) {
	var seen [256]bool

	for _, p := range prefixes {
		k := p[1]
		if seen[k] {
			continue
		}

		seen[k] = true

		same := make([]string, 0, len(prefixes))

		for _, q := range prefixes {
			if q[1] == k {
				same = append(same, q)
			}
		}

		idx.byPath[k] = append(idx.byPath[k], pos)
		idx.byPathPf[k] = append(idx.byPathPf[k], same)
	}
}

func head(s []int, i int) int {
	if i < len(s) {
		return s[i]
	}

	return math.MaxInt
}

const (
	fromAlways = iota
	fromPath
	fromTarget
)

// nextCandidate pops the smallest position across the three ascending lists, or -1, and
// reports which list it came from.
func nextCandidate(a, b, c []int, ia, ib, ic *int) (int, int) {
	ha, hb, hc := head(a, *ia), head(b, *ib), head(c, *ic)

	m := min(ha, hb, hc)

	switch m {
	case math.MaxInt:
		return -1, fromAlways
	case ha:
		*ia++

		return m, fromAlways
	case hb:
		*ib++

		return m, fromPath
	default:
		*ic++

		return m, fromTarget
	}
}

// rpcv2CBORServiceName returns <name> from /service/<name>/operation/<Op>, else "".
func rpcv2CBORServiceName(path string) string {
	rest, ok := strings.CutPrefix(path, rpcv2PathPrefix)
	if !ok {
		return ""
	}

	name, op, ok := strings.Cut(rest, rpcv2PathInfix)
	if !ok || op == "" {
		return ""
	}

	return name
}

// mergeAscending merges two ascending position lists, dropping duplicates.
func mergeAscending(a, b []int) []int {
	out := make([]int, 0, len(a)+len(b))

	for len(a) > 0 || len(b) > 0 {
		switch {
		case len(b) == 0 || (len(a) > 0 && a[0] < b[0]):
			out, a = append(out, a[0]), a[1:]
		case len(a) == 0 || b[0] < a[0]:
			out, b = append(out, b[0]), b[1:]
		default:
			out, a, b = append(out, a[0]), a[1:], b[1:]
		}
	}

	return out
}
