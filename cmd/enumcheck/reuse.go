package main

import (
	"go/ast"
	"go/token"
)

// dynamicKeyHelper describes a package-level function whose body builds a
// map[string]any using one of its OWN string parameters as the literal map
// key, and one of its OWN slice parameters as the source of the values
// stored under that key -- guardduty's usageByFeature(features []string,
// fieldName, unit string) is the shape this exists for: `map[string]any{
// fieldName: f, ...}` inside `for _, f := range features`.
type dynamicKeyHelper struct {
	keyParamIdx int
	valParamIdx int
}

// findDynamicKeyHelpers scans every package-level func for the
// dynamicKeyHelper shape.
func findDynamicKeyHelpers(files []*ast.File) map[string]dynamicKeyHelper {
	out := map[string]dynamicKeyHelper{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Body == nil || fd.Type.Params == nil {
				continue
			}

			if h, found := findDynamicKeyHelper(fd); found {
				out[fd.Name.Name] = h
			}
		}
	}

	return out
}

func findDynamicKeyHelper(fd *ast.FuncDecl) (dynamicKeyHelper, bool) {
	paramIndex, stringParams, sliceParams := indexParams(fd.Type.Params)
	if len(stringParams) == 0 || len(sliceParams) == 0 {
		return dynamicKeyHelper{}, false
	}

	if h, ok := scanCompositeLitsForKeyParam(fd.Body, nil, paramIndex, stringParams, sliceParams); ok {
		return h, true
	}

	return scanRangeBoundCompositeLits(fd.Body, paramIndex, stringParams, sliceParams)
}

func indexParams(fl *ast.FieldList) (map[string]int, map[string]bool, map[string]bool) {
	paramIndex := map[string]int{}
	stringParams := map[string]bool{}
	sliceParams := map[string]bool{}
	idx := 0

	for _, field := range fl.List {
		isString := isIdentNamed(field.Type, "string")

		at, isArr := field.Type.(*ast.ArrayType)
		isSlice := isArr && at.Len == nil

		for _, name := range field.Names {
			paramIndex[name.Name] = idx
			if isString {
				stringParams[name.Name] = true
			}

			if isSlice {
				sliceParams[name.Name] = true
			}

			idx++
		}
	}

	return paramIndex, stringParams, sliceParams
}

func isIdentNamed(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)

	return ok && id.Name == name
}

// scanCompositeLitsForKeyParam finds a map[string]any{...} anywhere in n
// with an entry keyed by one of stringParams whose value (after resolving
// through bound, a loop-variable->source-param binding) is one of
// sliceParams.
func scanCompositeLitsForKeyParam(
	n ast.Node, bound map[string]string, paramIndex map[string]int, stringParams, sliceParams map[string]bool,
) (dynamicKeyHelper, bool) {
	var result dynamicKeyHelper

	found := false

	ast.Inspect(n, func(node ast.Node) bool {
		if found {
			return false
		}

		cl, ok := node.(*ast.CompositeLit)
		if !ok || cl.Type == nil || !isStringAnyMapType(cl.Type) {
			return true
		}

		if h, matched := matchKeyParamElt(cl, bound, paramIndex, stringParams, sliceParams); matched {
			result, found = h, true

			return false
		}

		return true
	})

	return result, found
}

func matchKeyParamElt(
	cl *ast.CompositeLit, bound map[string]string, paramIndex map[string]int, stringParams, sliceParams map[string]bool,
) (dynamicKeyHelper, bool) {
	for _, elt := range cl.Elts {
		kv, ok := elt.(*ast.KeyValueExpr)
		if !ok {
			continue
		}

		keyIdent, ok := kv.Key.(*ast.Ident)
		if !ok || !stringParams[keyIdent.Name] {
			continue
		}

		valIdent, ok := kv.Value.(*ast.Ident)
		if !ok {
			continue
		}

		src := valIdent.Name
		if bound != nil {
			if s, has := bound[valIdent.Name]; has {
				src = s
			}
		}

		if !sliceParams[src] {
			continue
		}

		return dynamicKeyHelper{keyParamIdx: paramIndex[keyIdent.Name], valParamIdx: paramIndex[src]}, true
	}

	return dynamicKeyHelper{}, false
}

// scanRangeBoundCompositeLits handles the one-level-indirect shape (the real
// guardduty bug): `for _, f := range features { ... map[string]any{fieldName:
// f, ...} ... }`. Only a single level of range binding is tracked -- a
// disclosed simplification, not a general dataflow solver.
func scanRangeBoundCompositeLits(
	body ast.Node, paramIndex map[string]int, stringParams, sliceParams map[string]bool,
) (dynamicKeyHelper, bool) {
	var result dynamicKeyHelper

	found := false

	ast.Inspect(body, func(n ast.Node) bool {
		if found {
			return false
		}

		rs, ok := n.(*ast.RangeStmt)
		if !ok {
			return true
		}

		id, isIdentVal := rs.Value.(*ast.Ident)
		srcIdent, isIdentSrc := rs.X.(*ast.Ident)

		if !isIdentVal || !isIdentSrc {
			return true
		}

		bound := map[string]string{id.Name: srcIdent.Name}
		if h, matched := scanCompositeLitsForKeyParam(rs.Body, bound, paramIndex, stringParams, sliceParams); matched {
			result, found = h, true

			return false
		}

		return true
	})

	return result, found
}

// helperCallSite is one resolved call to a dynamicKeyHelper: the literal
// wire key it targets, that key's real (unambiguous) enum type, and the
// source text of the value-source argument it was called with.
type helperCallSite struct {
	key   string
	enum  string
	value string
	pos   token.Position
}

// checkCrossEnumReuse is check B, NEEDS REVIEW only: within one enclosing
// function, two calls to the same dynamicKeyHelper with the textually
// identical value-source argument, targeting two wire keys whose real SDK
// enums are different AND declare different member sets. Flags the shape,
// never the runtime value -- the actual value is never resolved, so this is
// never confident. See package doc comment.
func checkCrossEnumReuse(
	files []*ast.File, fset *token.FileSet, reg *enumRegistry, wireKeys map[string]wireKeyFact,
	pkgConsts map[string]string, repoRoot string,
) []finding {
	helpers := findDynamicKeyHelpers(files)
	if len(helpers) == 0 {
		return nil
	}

	groups := map[string][]helperCallSite{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			localConsts := localStringConsts(fd)
			collectHelperCallsInFunc(fd, fset, helpers, wireKeys, localConsts, pkgConsts, reg, groups)
		}
	}

	return crossEnumFindingsFromGroups(groups, reg, repoRoot)
}

func collectHelperCallsInFunc(
	fd *ast.FuncDecl, fset *token.FileSet, helpers map[string]dynamicKeyHelper, wireKeys map[string]wireKeyFact,
	localConsts, pkgConsts map[string]string, reg *enumRegistry, groups map[string][]helperCallSite,
) {
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		site, groupKey, ok := resolveHelperCallSite(fd, call, fset, helpers, wireKeys, localConsts, pkgConsts, reg)
		if ok {
			groups[groupKey] = append(groups[groupKey], site)
		}

		return true
	})
}

func resolveHelperCallSite(
	fd *ast.FuncDecl, call *ast.CallExpr, fset *token.FileSet, helpers map[string]dynamicKeyHelper,
	wireKeys map[string]wireKeyFact, localConsts, pkgConsts map[string]string, reg *enumRegistry,
) (helperCallSite, string, bool) {
	ident, ok := call.Fun.(*ast.Ident)
	if !ok {
		return helperCallSite{}, "", false
	}

	h, ok := helpers[ident.Name]
	if !ok || len(call.Args) <= h.keyParamIdx || len(call.Args) <= h.valParamIdx {
		return helperCallSite{}, "", false
	}

	key, ok := resolveConstString(call.Args[h.keyParamIdx], localConsts, pkgConsts, reg)
	if !ok {
		return helperCallSite{}, "", false
	}

	fact := wireKeys[key]
	if len(fact.Enums) != 1 {
		return helperCallSite{}, "", false
	}

	valueText := exprText(fset, call.Args[h.valParamIdx])
	groupKey := fd.Name.Name + "\x00" + valueText

	return helperCallSite{
		key:   key,
		enum:  fact.Enums[0],
		value: valueText,
		pos:   fset.Position(call.Pos()),
	}, groupKey, true
}

func crossEnumFindingsFromGroups(groups map[string][]helperCallSite, reg *enumRegistry, repoRoot string) []finding {
	out := make([]finding, 0, len(groups))

	for _, sites := range groups {
		out = append(out, crossEnumFindingsInGroup(sites, reg, repoRoot)...)
	}

	return out
}

func crossEnumFindingsInGroup(sites []helperCallSite, reg *enumRegistry, repoRoot string) []finding {
	var out []finding

	seenPairs := map[[2]string]bool{}

	for i := range sites {
		for j := i + 1; j < len(sites); j++ {
			a, b := sites[i], sites[j]
			if a.enum == b.enum || reg.sameMemberSet(a.enum, b.enum) {
				continue
			}

			pair := sortedPair(a.enum, b.enum)
			if seenPairs[pair] {
				continue
			}

			seenPairs[pair] = true

			out = append(out, finding{
				File: relPath(repoRoot, a.pos.Filename), Line: a.pos.Line,
				Kind: kindReuse, Key: a.key, Enum: a.enum,
				OtherKey: b.key, OtherEnum: b.enum, OtherLine: b.pos.Line,
				Confident: false,
			})
		}
	}

	return out
}

func sortedPair(a, b string) [2]string {
	if a > b {
		a, b = b, a
	}

	return [2]string{a, b}
}
