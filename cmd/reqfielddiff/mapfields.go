package main

import "go/ast"

// minMapFieldCallArgs is a map-field accessor's own minimum shape (the
// map-body parameter and its key parameter) and, identically, the minimum
// argument count a call site must pass for the read to be resolvable at
// all.
const minMapFieldCallArgs = 2

// isMapStringAnyType reports whether t is `map[string]any` (or
// `map[string]interface{}`) -- quicksight's hand-decoded request body
// shape.
func isMapStringAnyType(t ast.Expr) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}

	key, ok := mt.Key.(*ast.Ident)

	return ok && key.Name == goStringType && isAnyType(mt.Value)
}

// mapAnyNames finds every local identifier in fl that holds a
// map[string]any value: a direct PARAMETER of that type, or a local bound
// via `body, err := someFunc(...)` where someFunc (a known package func)
// declares a `map[string]any` result at that position -- quicksight's
// `body, err := readBody(c)`, readBody returning (map[string]any, error).
// Scoped to a single-call, same-length multi-assign (`x, y := f()`), the
// one shape observed in this repo; anything else is left unrecognised
// rather than guessed at.
func mapAnyNames(fl funcLike, ctx handlerResolveCtx) map[string]bool {
	out := map[string]bool{}

	for _, p := range namedParamsMatching(fl.Params, isMapStringAnyType) {
		out[p.name] = true
	}

	if fl.Body == nil {
		return out
	}

	ast.Inspect(fl.Body, func(n ast.Node) bool {
		if as, ok := n.(*ast.AssignStmt); ok {
			addMapAnyAssignTargets(as, ctx, out)
		}

		return true
	})

	return out
}

// addMapAnyAssignTargets handles one `x, y := f()` assignment: if f is a
// known package func whose flattened result types line up 1:1 with as's
// own LHS names, any LHS name at a map[string]any result position is added
// to out -- quicksight's `body, err := readBody(c)`, readBody returning
// (map[string]any, error).
func addMapAnyAssignTargets(as *ast.AssignStmt, ctx handlerResolveCtx, out map[string]bool) {
	if len(as.Rhs) != 1 {
		return
	}

	call, ok := as.Rhs[0].(*ast.CallExpr)
	if !ok {
		return
	}

	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return
	}

	fd, ok := ctx.funcs[id.Name]
	if !ok || fd.Type.Results == nil {
		return
	}

	results := flattenFieldTypes(fd.Type.Results)
	if len(results) != len(as.Lhs) {
		return
	}

	for i, resultType := range results {
		lhsID, isIdent := as.Lhs[i].(*ast.Ident)
		if isIdent && lhsID.Name != "_" && isMapStringAnyType(resultType) {
			out[lhsID.Name] = true
		}
	}
}

// isMapFieldAccessorSig reports whether ft's first two flattened
// parameters are (map[string]any, string) -- quicksight's
// strField/mapField/boolField/intField(body, "Key") shape, recognised
// structurally rather than by name.
func isMapFieldAccessorSig(ft *ast.FuncType) bool {
	if ft.Params == nil {
		return false
	}

	flat := flattenFieldTypes(ft.Params)
	if len(flat) < minMapFieldCallArgs {
		return false
	}

	return isMapStringAnyType(flat[0]) && isStringType(flat[1])
}

// matchMapFieldCall recognises `strField(body, "Role")` (and mapField/
// boolField/intField/...): a call to a package func matching
// isMapFieldAccessorSig, whose first argument is one of this scan's known
// map[string]any locals (mapNames) and whose second is a literal (or
// resolvable package const) key -- declared identically to a bare
// c.QueryParam("name") read, no struct behind it.
func matchMapFieldCall(call *ast.CallExpr, mapNames map[string]bool, ctx handlerResolveCtx, res *opResolution) {
	fn, ok := call.Fun.(*ast.Ident)
	if !ok || len(call.Args) < minMapFieldCallArgs {
		return
	}

	fd, ok := ctx.funcs[fn.Name]
	if !ok || !isMapFieldAccessorSig(fd.Type) {
		return
	}

	recv, ok := call.Args[0].(*ast.Ident)
	if !ok || !mapNames[recv.Name] {
		return
	}

	name, ok := resolveStringExpr(call.Args[1], ctx.pkgConsts)
	if !ok || name == "" {
		return
	}

	declareLiteralField(res, name)
}

// matchMapIndexExpr recognises direct `body["Role"]` indexing into a known
// map[string]any local, the same declaration signal as a matchMapFieldCall
// accessor call.
func matchMapIndexExpr(idx *ast.IndexExpr, mapNames map[string]bool, ctx handlerResolveCtx, res *opResolution) {
	recv, ok := idx.X.(*ast.Ident)
	if !ok || !mapNames[recv.Name] {
		return
	}

	name, ok := resolveStringExpr(idx.Index, ctx.pkgConsts)
	if !ok || name == "" {
		return
	}

	declareLiteralField(res, name)
}
