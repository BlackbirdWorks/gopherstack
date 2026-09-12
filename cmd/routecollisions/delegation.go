package main

import (
	"go/ast"
	"go/token"
	"maps"
	"sort"
	"strconv"
)

// chaseDepthLimit bounds how many hops chaseClaims will follow from a
// RouteMatcher body through calls/indexes into other named bodies before it
// stops recursing (it still extracts claims from the body at the limit,
// just doesn't chase further from it). Sized for the deepest real chain in
// the repo today -- resiliencehub/mgn/networkmanager's route-table builders
// nest three hops deep (RouteMatcher -> routes()/routeTable() ->
// per-family builder -> per-sub-family builder) -- with headroom for one
// more hop of indirection.
const chaseDepthLimit = 4

// chaseClaims extracts claims from body/node, then -- unless depth has hit
// chaseDepthLimit -- recursively extracts claims from every named body
// referencedNames finds called or indexed within it, so long as that name
// hasn't already been visited in this chase (cycle safety; also avoids
// reprocessing a shared helper like mergeRoutes/concatRoutes reached from
// multiple branches). The returned slice is not deduplicated -- callers
// combine it across every RouteMatcher chase and dedupe once at the end.
func chaseClaims(
	body string,
	node ast.Node,
	pd *pkgData,
	consts map[string]string,
	sliceConsts map[string][]string,
	depth int,
	visited map[string]bool,
) []claim {
	claims := extractClaimsForNode(body, node, pd.fset, consts, sliceConsts)

	if depth >= chaseDepthLimit {
		return claims
	}

	for _, name := range referencedNames(node) {
		if visited[name] {
			continue
		}

		childNode, ok := pd.namedBodies[name]
		if !ok {
			continue
		}

		visited[name] = true

		fp := pd.fset.Position(childNode.Pos()).Filename
		childBody := bodyText(pd.fset, pd.srcByFile[fp], childNode)

		claims = append(claims, chaseClaims(childBody, childNode, pd, consts, sliceConsts, depth+1, visited)...)
	}

	return claims
}

// extractClaimsForNode runs the ordinary text-based claim extraction on
// body, plus an AST-based scan of node for map composite literals (map
// keys are never a prefix check, so they're reported as exact claims). When
// node itself is a map composite literal (a package-level var like
// account's operationNames or resourcegroups' rgRESTPathOps, resolved
// directly by chaseClaims), the text-based scan is skipped -- everything
// meaningful in a bare map literal's source text is a key or a value, and
// the map-key scan already covers the keys precisely.
//
// Any const declared local to node (a helper function's own "const suffix =
// \"/tags\"", as opposed to a package-level const) is folded into consts for
// this call only, and its declaration text excluded from the raw
// quoted-literal scan -- see localConstTable's doc comment for why: without
// this, resourcegroups' isResourceTagsPath produced a bogus standalone
// "/tags" claim merely from declaring a HasSuffix comparison const, never
// from an actual prefix check.
func extractClaimsForNode(
	body string,
	node ast.Node,
	fset *token.FileSet,
	consts map[string]string,
	sliceConsts map[string][]string,
) []claim {
	if lit, ok := node.(*ast.CompositeLit); ok {
		if _, isMap := lit.Type.(*ast.MapType); isMap {
			cc := newClaimCollector(body)
			scanMapKeyClaims(cc, lit, consts)

			return cc.claims()
		}
	}

	localConsts, declRanges := localConstTable(fset, node)

	merged := consts
	if len(localConsts) > 0 {
		merged = make(map[string]string, len(consts)+len(localConsts))

		maps.Copy(merged, consts)
		maps.Copy(merged, localConsts)
	}

	claims := extractClaims(body, merged, sliceConsts, declRanges)

	cc := newClaimCollector(body)
	scanMapKeyClaims(cc, node, merged)
	scanSliceLiteralIdentClaims(cc, node, merged)
	scanSwitchCaseIdentClaims(cc, node, merged)

	return append(claims, cc.claims()...)
}

// scanSwitchCaseIdentClaims walks node for every switch statement's case
// clause and adds an exact claim for each case value that's a bare
// identifier resolving against consts. detective's RouteMatcher classifies
// its entire operation set this way -- "switch path { case pathGraph,
// pathGraphRemoval, pathGraphsList, ...: return true }" -- a multi-value
// case clause where no individual value is preceded by "==", "HasPrefix(
// path,", "CutPrefix(path," or "range " (bareIdentRe's only recognized
// contexts), and it isn't a map literal either, so without this the whole
// claim set silently collapses to whatever else the RouteMatcher happens
// to check (here, just its "/tags/" ARN guard).
func scanSwitchCaseIdentClaims(cc *claimCollector, node ast.Node, consts map[string]string) {
	ast.Inspect(node, func(n ast.Node) bool {
		clause, ok := n.(*ast.CaseClause)
		if !ok {
			return true
		}

		for _, expr := range clause.List {
			id, isIdent := expr.(*ast.Ident)
			if !isIdent {
				continue
			}

			if val, resolved := consts[id.Name]; resolved {
				cc.addExact(val)
			}
		}

		return true
	})
}

// scanSliceLiteralIdentClaims walks node for every "[]string{...}" composite
// literal it contains -- local (backup's matchesBackupPath and omics'
// isOmicsPath both build "prefixes"/"exacts" this way, a shape the
// package-wide sliceConsts regex table in main.go never sees since it only
// matches a top-level "IDENT = []string{...}" var, not a local ":="
// literal) or otherwise -- and, for each element that's a bare identifier
// resolving against consts, adds both an exact claim for the value and a
// prefix claim for value+"/". Both readings are recorded because this
// repo's common idiom for such a table is "path == p ||
// strings.HasPrefix(path, p+\"/\")" (see omics' isOmicsPath): the loop
// variable itself isn't const-resolvable, so scanConcatLiterals can't
// already see the "+\"/\"" half, and recording only the bare value would
// silently miss a same-tier collision that only matches the "p+\"/\""
// prefix (inspector2's own real claim is "/configuration/", not
// "/configuration" -- reproducing that pair needs the "/" variant too).
func scanSliceLiteralIdentClaims(cc *claimCollector, node ast.Node, consts map[string]string) {
	ast.Inspect(node, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}

		arr, isArray := lit.Type.(*ast.ArrayType)
		if !isArray {
			return true
		}

		if elt, isStringElt := arr.Elt.(*ast.Ident); !isStringElt || elt.Name != "string" {
			return true
		}

		for _, el := range lit.Elts {
			id, isIdent := el.(*ast.Ident)
			if !isIdent {
				continue
			}

			val, resolved := consts[id.Name]
			if !resolved {
				continue
			}

			cc.addRaw(val, kindExact)
			cc.addRaw(val+"/", kindPrefix)
		}

		return true
	})
}

// localConstTable walks node for const declarations local to it (inside a
// func/method body, not the package-level consts collectConsts already
// gathers) and returns their name->value map, plus the body-relative byte
// ranges of each literal's own declaration text within node's source range.
// extractClaimsForNode excludes those ranges from the raw quoted-literal
// scan, so a local const contributes a claim only through an actual
// comparison/concatenation usage resolved against this table -- never
// redundantly (and inaccurately, ignoring the surrounding condition) from
// its own "const x = \"...\"" line.
func localConstTable(fset *token.FileSet, node ast.Node) (map[string]string, [][2]int) {
	nodeStart := fset.Position(node.Pos()).Offset

	consts := map[string]string{}

	var ranges [][2]int

	ast.Inspect(node, func(n ast.Node) bool {
		gd, ok := n.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			return true
		}

		for _, spec := range gd.Specs {
			collectLocalConstSpec(spec, fset, nodeStart, consts, &ranges)
		}

		return true
	})

	return consts, ranges
}

func collectLocalConstSpec(
	spec ast.Spec,
	fset *token.FileSet,
	nodeStart int,
	consts map[string]string,
	ranges *[][2]int,
) {
	vs, ok := spec.(*ast.ValueSpec)
	if !ok || len(vs.Names) != len(vs.Values) {
		return
	}

	for i, name := range vs.Names {
		lit, isStringLit := vs.Values[i].(*ast.BasicLit)
		if !isStringLit || lit.Kind != token.STRING {
			continue
		}

		val, err := strconv.Unquote(lit.Value)
		if err != nil {
			continue
		}

		consts[name.Name] = val
		*ranges = append(*ranges, [2]int{
			fset.Position(lit.Pos()).Offset - nodeStart,
			fset.Position(lit.End()).Offset - nodeStart,
		})
	}
}

// referencedNames walks node's AST and collects, in encounter order with
// duplicates removed, every identifier used as a function/method call
// (RouteMatcher's own "isXPath(path)" shape, or a method call like
// "h.routes()") or as a map index ("operationNames[path]",
// "rgRESTPathOps[path]"). A selector's package/receiver qualifier is
// ignored -- chaseClaims only ever finds a hit for names that are also a
// key in pd.namedBodies, so an unrelated stdlib or cross-package call
// (strings.HasPrefix, c.Request()) never matches anything.
func referencedNames(node ast.Node) []string {
	seen := map[string]bool{}

	var names []string

	ast.Inspect(node, func(n ast.Node) bool {
		switch x := n.(type) {
		case *ast.CallExpr:
			if name := callName(x); name != "" && !seen[name] {
				seen[name] = true

				names = append(names, name)
			}
		case *ast.IndexExpr:
			if id, ok := x.X.(*ast.Ident); ok && !seen[id.Name] {
				seen[id.Name] = true

				names = append(names, id.Name)
			}
		}

		return true
	})

	return names
}

func callName(call *ast.CallExpr) string {
	switch fn := call.Fun.(type) {
	case *ast.Ident:
		return fn.Name
	case *ast.SelectorExpr:
		return fn.Sel.Name
	default:
		return ""
	}
}

// scanMapKeyClaims walks node for every map composite literal it contains,
// however deeply nested (a func body that returns "map[string]T{...}"
// directly, e.g. resiliencehub's routesApps, counts), and adds an exact
// claim for each key that resolves to a "/"-prefixed value -- either a
// literal string key (resourcegroups' rgRESTPathOps) or an identifier key
// naming a package string const (account's operationNames).
func scanMapKeyClaims(cc *claimCollector, node ast.Node, consts map[string]string) {
	ast.Inspect(node, func(n ast.Node) bool {
		lit, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}

		if _, isMap := lit.Type.(*ast.MapType); !isMap {
			return true
		}

		for _, elt := range lit.Elts {
			kv, isKV := elt.(*ast.KeyValueExpr)
			if !isKV {
				continue
			}

			if key := mapKeyLiteral(kv.Key, consts); key != "" {
				cc.addExact(key)
			}
		}

		return true
	})
}

func mapKeyLiteral(key ast.Expr, consts map[string]string) string {
	switch k := key.(type) {
	case *ast.BasicLit:
		if k.Kind != token.STRING {
			return ""
		}

		s, err := strconv.Unquote(k.Value)
		if err != nil {
			return ""
		}

		return s
	case *ast.Ident:
		return consts[k.Name]
	default:
		return ""
	}
}

// dedupeClaims merges a chase's per-body claim slices (each individually
// deduplicated, but the same literal can surface from more than one body,
// or under both an exact and a prefix reading) into one sorted, unique-by-
// literal-and-kind list.
func dedupeClaims(claims []claim) []claim {
	seen := make(map[string]claim, len(claims))

	for _, c := range claims {
		key := c.Literal + "|" + c.KindStr
		if _, ok := seen[key]; !ok {
			seen[key] = c
		}
	}

	out := make([]claim, 0, len(seen))
	for _, c := range seen {
		out = append(out, c)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Literal < out[j].Literal })

	return out
}
