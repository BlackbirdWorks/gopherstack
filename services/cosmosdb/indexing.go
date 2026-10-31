package cosmosdb

import (
	"encoding/json"
	"slices"
	"strings"
)

const (
	indexModeNone  = "none"
	indexModeLazy  = "lazy"
	orderAscending = "ascending"

	msgNoCompositeIndex = "The order by query does not have a corresponding composite index that it can be served from."
	msgOrderByExcluded  = "The index path corresponding to the specified order-by item is excluded."
	msgLazyIndexing     = "The lazy indexing mode is not available for new containers."
	msgScanRequired     = "An invalid query has been specified with filters against path(s) excluded from " +
		"indexing. Consider adding allow scan header in the request."
)

// QueryOptions are the request-level switches of a query.
type QueryOptions struct {
	// AllowScan permits queries the indexing policy cannot serve (x-ms-documentdb-query-enable-scan).
	AllowScan bool
}

type indexingPolicy struct {
	IndexingMode  string `json:"indexingMode"`
	IncludedPaths []struct {
		Path string `json:"path"`
	} `json:"includedPaths"`
	ExcludedPaths []struct {
		Path string `json:"path"`
	} `json:"excludedPaths"`
	VectorIndexes []struct {
		Path string `json:"path"`
		Type string `json:"type"`
	} `json:"vectorIndexes"`
	FullTextIndexes []struct {
		Path string `json:"path"`
	} `json:"fullTextIndexes"`
	SpatialIndexes   []spatialIndex `json:"spatialIndexes"`
	computed         map[string]struct{}
	CompositeIndexes [][]struct {
		Path  string `json:"path"`
		Order string `json:"order"`
	} `json:"compositeIndexes"`
}

// parseIndexingPolicy decodes the stored policy; an unset or undecodable one yields the default (index all).
func parseIndexingPolicy(raw json.RawMessage) indexingPolicy {
	if len(raw) == 0 {
		raw = json.RawMessage(defaultIndexingPolicy)
	}

	var pol indexingPolicy
	if err := json.Unmarshal(raw, &pol); err != nil {
		_ = json.Unmarshal([]byte(defaultIndexingPolicy), &pol)
	}

	return pol
}

// checkQueryIndexing applies the indexing policy to stmt: multi-property ORDER BY needs a composite index, and a
// query whose filter cannot use the index needs the scan switch.
func checkQueryIndexing(
	stmt *sqlSelectStmt, raw json.RawMessage, opts QueryOptions, computed map[string]sqlExpr,
) error {
	pol := parseIndexingPolicy(raw)
	pol.computed = make(map[string]struct{}, len(computed))

	for name := range computed {
		pol.computed[name] = struct{}{}
	}

	if !pol.hasCompositeFor(stmt.orderBy) {
		return &queryIndexError{msg: msgNoCompositeIndex}
	}

	if pol.orderByExcluded(stmt.orderBy) {
		return &queryIndexError{msg: msgOrderByExcluded}
	}

	if !opts.AllowScan && stmt.where != nil && pol.needsScan(stmt.where) {
		return &queryIndexError{msg: msgScanRequired}
	}

	return nil
}

type queryIndexError struct{ msg string }

func (e *queryIndexError) Error() string { return e.msg }

// hasCompositeFor reports whether a multi-key ORDER BY over plain properties is covered by a composite index whose
// leading paths match in order, with directions all equal or all opposite. Other ORDER BYs are not restricted.
func (p indexingPolicy) hasCompositeFor(keys []sqlOrderKey) bool {
	if len(keys) < arityPair {
		return true
	}

	paths := make([]string, len(keys))

	for i, k := range keys {
		path, ok := rootPath(k.expr)
		if !ok {
			return true
		}

		paths[i] = path
	}

	for _, idx := range p.CompositeIndexes {
		if len(idx) < len(keys) {
			continue
		}

		same, opposite := true, true

		for i, k := range keys {
			if normalizeIndexPath(idx[i].Path) != paths[i] {
				same, opposite = false, false

				break
			}

			desc := strings.EqualFold(idx[i].Order, "descending")
			same = same && desc == k.desc
			opposite = opposite && desc != k.desc
		}

		if same || opposite {
			return true
		}
	}

	return false
}

// orderByExcluded reports whether a single-key ORDER BY sorts on a property path the policy does not index. A
// multi-key ORDER BY is served by a composite index instead (hasCompositeFor).
func (p indexingPolicy) orderByExcluded(keys []sqlOrderKey) bool {
	if len(keys) != 1 {
		return false
	}

	path, ok := rootPath(keys[0].expr)

	return ok && path != "" && !p.indexed(path)
}

// rootPath renders a property chain on the container row as "/a/b" (array positions as "[]").
func rootPath(x sqlExpr) (string, bool) {
	switch n := x.(type) {
	case *exprVar:
		return "", n.idx == 0
	case *exprProp:
		base, ok := rootPath(n.base)

		return base + "/" + n.key, ok
	case *exprIndex:
		base, ok := rootPath(n.base)
		if !ok {
			return "", false
		}

		if lit, isLit := n.idx.(*exprLit); isLit {
			if s, isStr := lit.v.(string); isStr {
				return base + "/" + s, true
			}
		}

		return base + "/[]", true
	default:
		return "", false
	}
}

func normalizeIndexPath(p string) string {
	return strings.ReplaceAll(strings.TrimSuffix(strings.TrimSuffix(p, "/?"), "/*"), `"`, "")
}

// indexed reports whether the policy indexes the filter path; the longest matching include or exclude wins.
func (p indexingPolicy) indexed(path string) bool {
	if p.isComputed(path) {
		return !strings.EqualFold(p.IndexingMode, indexModeNone) && p.computedIndexed(path)
	}

	if strings.EqualFold(p.IndexingMode, indexModeNone) {
		return path == "/id" || path == "/"+sysPropRID
	}

	if path == "/id" || path == "/"+sysPropTS {
		return true
	}

	segs := splitIndexPath(path)
	best, bestLen := true, -1

	for _, inc := range p.IncludedPaths {
		if l, ok := matchIndexPattern(inc.Path, segs); ok && l >= bestLen {
			best, bestLen = true, l
		}
	}

	for _, exc := range p.ExcludedPaths {
		if l, ok := matchIndexPattern(exc.Path, segs); ok && l > bestLen {
			best, bestLen = false, l
		}
	}

	return best
}

func splitIndexPath(p string) []string {
	p = strings.ReplaceAll(strings.Trim(p, "/"), `"`, "")
	if p == "" {
		return nil
	}

	return strings.Split(p, "/")
}

// matchIndexPattern matches a policy path ("/a/b/?", "/a/*", "/*") against segs, returning its specificity.
func matchIndexPattern(pattern string, segs []string) (int, bool) {
	exact := strings.HasSuffix(pattern, "/?")
	psegs := splitIndexPath(strings.TrimSuffix(strings.TrimSuffix(pattern, "/?"), "/*"))

	if len(psegs) > len(segs) || (exact && len(psegs) != len(segs)) {
		return 0, false
	}

	for i, s := range psegs {
		if s != segs[i] {
			return 0, false
		}
	}

	return len(psegs), true
}

// needsScan reports whether the filter cannot be served by the index: an AND is served when either side is, an OR
// and any single predicate only when everything it touches is indexed.
func (p indexingPolicy) needsScan(x sqlExpr) bool {
	switch n := x.(type) {
	case *exprBinary:
		if n.op == sqlTokAnd {
			return p.needsScan(n.l) && p.needsScan(n.r)
		}

		if n.op == sqlTokOr {
			return p.needsScan(n.l) || p.needsScan(n.r)
		}
	case *exprUnary:
		if n.op == sqlTokNot {
			return p.needsScan(n.x)
		}
	}

	return p.touchesUnindexed(x)
}

func (p indexingPolicy) touchesUnindexed(x sqlExpr) bool {
	if path, ok := rootPath(x); ok {
		return path != "" && !p.isComputed(path) && !p.indexed(path)
	}

	switch n := x.(type) {
	case *exprBinary:
		return p.touchesUnindexed(n.l) || p.touchesUnindexed(n.r)
	case *exprUnary:
		return p.touchesUnindexed(n.x)
	case *exprIsNull:
		return p.touchesUnindexed(n.x)
	case *exprIn:
		return p.touchesUnindexed(n.x)
	case *exprBetween:
		return p.touchesUnindexed(n.x)
	case *exprLike:
		return p.touchesUnindexed(n.x)
	case *exprCall:
		if slices.ContainsFunc(n.args, p.touchesUnindexed) {
			return true
		}
	}

	return false
}
