package cosmosdb

import (
	"encoding/json"
	"fmt"
	"strings"
)

// Computed property definition rules (Learn, "Computed properties"): top-level name, unique, not a system property;
// a SELECT VALUE query over the root item with no JOIN, WHERE, GROUP BY, ORDER BY, TOP, DISTINCT or OFFSET LIMIT, no
// subquery, aggregate, spatial function, UDF or nondeterministic function.

// exprChildren lists the direct sub-expressions of x.
func exprChildren(x sqlExpr) []sqlExpr {
	switch n := x.(type) {
	case *exprProp:
		return []sqlExpr{n.base}
	case *exprIndex:
		return []sqlExpr{n.base, n.idx}
	case *exprUnary:
		return []sqlExpr{n.x}
	case *exprBinary:
		return []sqlExpr{n.l, n.r}
	case *exprIsNull:
		return []sqlExpr{n.x}
	case *exprIn:
		return append([]sqlExpr{n.x}, n.list...)
	case *exprBetween:
		return []sqlExpr{n.x, n.lo, n.hi}
	case *exprLike:
		return []sqlExpr{n.x, n.pattern}
	case *exprTernary:
		return []sqlExpr{n.cond, n.a, n.b}
	case *exprCall:
		return n.args
	case *exprArrayLit:
		return n.items
	case *exprObjectLit:
		return n.vals
	}

	return nil
}

// forbiddenComputedUse names the construct of x a computed property query may not contain, or "".
func forbiddenComputedUse(x sqlExpr) string {
	switch n := x.(type) {
	case *exprSubquery:
		return "a subquery"
	case *exprCall:
		if why := forbiddenComputedCall(n); why != "" {
			return why
		}
	}

	for _, child := range exprChildren(x) {
		if why := forbiddenComputedUse(child); why != "" {
			return why
		}
	}

	return ""
}

func forbiddenComputedCall(n *exprCall) string {
	if n.kind == fnAggregate {
		return "an aggregate function"
	}

	if n.kind == fnSpatial {
		return "a spatial function"
	}

	if n.kind == fnUDF {
		return "a user-defined function"
	}

	if n.name == biRand || strings.HasPrefix(n.name, "GETCURRENT") {
		return "the nondeterministic function " + n.name
	}

	if n.name == biStArea {
		return "a spatial function"
	}

	return ""
}

// firstSegment is the top-level property of an index path such as "/cp/a/?".
func firstSegment(path string) string {
	segs := splitIndexPath(path)
	if len(segs) == 0 {
		return ""
	}

	return segs[0]
}

// computedIndexed reports whether the policy indexes a computed property path: only an explicit path under the
// property's own name counts, the root wildcard does not cover computed properties.
func (p indexingPolicy) computedIndexed(path string) bool {
	name := firstSegment(path)
	segs := splitIndexPath(path)
	best, bestLen := false, -1

	for _, inc := range p.IncludedPaths {
		if firstSegment(inc.Path) != name {
			continue
		}

		if l, ok := matchIndexPattern(inc.Path, segs); ok && l >= bestLen {
			best, bestLen = true, l
		}
	}

	for _, exc := range p.ExcludedPaths {
		if firstSegment(exc.Path) != name {
			continue
		}

		if l, ok := matchIndexPattern(exc.Path, segs); ok && l > bestLen {
			best, bestLen = false, l
		}
	}

	return best
}

func (p indexingPolicy) isComputed(path string) bool {
	_, ok := p.computed[firstSegment(path)]

	return ok
}

// checkComputedIndexes rejects a spatial index on a computed property.
func checkComputedIndexes(pol indexingPolicy, raw json.RawMessage) error {
	props, err := parseComputedProperties(raw)
	if err != nil || len(props) == 0 {
		return err
	}

	for _, idx := range pol.SpatialIndexes {
		for _, cp := range props {
			if firstSegment(idx.Path) == cp.Name {
				return fmt.Errorf(
					"%w: computed property %q cannot have a spatial index",
					ErrInvalidContainerPolicy,
					cp.Name,
				)
			}
		}
	}

	return nil
}
