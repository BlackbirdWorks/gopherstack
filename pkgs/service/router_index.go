package service

import (
	"math"
	"strings"
)

// routeIndex buckets service positions (priority order) by the first byte a gate prefix
// requires, so a request only visits services whose gates could pass. Immutable once built.
type routeIndex struct {
	always   []int
	byPath   [256][]int
	byTarget [256][]int
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

func buildRouteIndex(targetGates, pathGates [][]string) routeIndex {
	var idx routeIndex

	for i := range targetGates {
		switch {
		case len(pathGates[i]) > 0:
			addKeyed(&idx.byPath, i, pathGates[i], 1)
		case len(targetGates[i]) > 0:
			addKeyed(&idx.byTarget, i, targetGates[i], 0)
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

func head(s []int, i int) int {
	if i < len(s) {
		return s[i]
	}

	return math.MaxInt
}

// nextCandidate pops the smallest position across the three ascending lists, or -1.
func nextCandidate(a, b, c []int, ia, ib, ic *int) int {
	ha, hb, hc := head(a, *ia), head(b, *ib), head(c, *ic)

	m := min(ha, hb, hc)

	switch m {
	case math.MaxInt:
		return -1
	case ha:
		*ia++

		return m
	case hb:
		*ib++

		return m
	default:
		*ic++

		return m
	}
}
