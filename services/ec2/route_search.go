package ec2

import (
	"net/netip"
	"slices"
)

const (
	filterRouteExact     = "route-search.exact-match"
	filterRouteLongest   = "route-search.longest-prefix-match"
	filterRouteSubnetOf  = "route-search.subnet-of-match"
	filterRouteSupernet  = "route-search.supernet-of-match"
	routeSearchNoContain = -1
)

func parseRoutePrefix(s string) (netip.Prefix, bool) {
	p, err := netip.ParsePrefix(s)
	if err != nil {
		return netip.Prefix{}, false
	}

	return p.Masked(), true
}

// routeSearchItemMatch applies the per-route route-search.* filters; other filter names pass.
func routeSearchItemMatch(routeCIDR, name string, values []string) bool {
	route, ok := parseRoutePrefix(routeCIDR)
	if !ok {
		return false
	}

	var match func(route, want netip.Prefix) bool

	switch name {
	case filterRouteExact:
		match = func(route, want netip.Prefix) bool { return route == want }
	case filterRouteSubnetOf:
		match = func(route, want netip.Prefix) bool { return want.Bits() <= route.Bits() && want.Contains(route.Addr()) }
	case filterRouteSupernet:
		match = func(route, want netip.Prefix) bool { return route.Bits() <= want.Bits() && route.Contains(want.Addr()) }
	default:
		return true
	}

	return slices.ContainsFunc(values, func(v string) bool {
		want, valid := parseRoutePrefix(v)

		return valid && match(route, want)
	})
}

// routeSearchLongestPrefix keeps, for each requested CIDR, the routes with the longest prefix containing it.
func routeSearchLongestPrefix[T any](items []T, cidrOf func(T) string, values []string) []T {
	keep := map[int]bool{}

	for _, v := range values {
		want, ok := parseRoutePrefix(v)
		if !ok {
			continue
		}

		best := routeSearchNoContain

		for _, it := range items {
			if p, valid := parseRoutePrefix(cidrOf(it)); valid && p.Bits() <= want.Bits() && p.Contains(want.Addr()) {
				best = max(best, p.Bits())
			}
		}

		for i, it := range items {
			if p, valid := parseRoutePrefix(cidrOf(it)); valid && p.Bits() == best && p.Bits() <= want.Bits() &&
				p.Contains(want.Addr()) {
				keep[i] = true
			}
		}
	}

	out := make([]T, 0, len(keep))

	for i, it := range items {
		if keep[i] {
			out = append(out, it)
		}
	}

	return out
}

// applyRouteSearchFilters narrows items by every route-search.* filter in filters.
func applyRouteSearchFilters[T any](items []T, filters map[string][]string, cidrOf func(T) string) []T {
	out := items

	for _, name := range []string{filterRouteExact, filterRouteSubnetOf, filterRouteSupernet} {
		values, ok := filters[name]
		if !ok {
			continue
		}

		out = slices.DeleteFunc(
			slices.Clone(out),
			func(it T) bool { return !routeSearchItemMatch(cidrOf(it), name, values) },
		)
	}

	if values, ok := filters[filterRouteLongest]; ok {
		out = routeSearchLongestPrefix(out, cidrOf, values)
	}

	return out
}
