package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"sort"
	"strings"
)

// isOpParamName reports the names a dispatcher conventionally receives the
// operation under; they gate every dispatch shape added below.
func isOpParamName(name string) bool {
	switch name {
	case "action", "operation", "opName", "operationName":
		return true
	default:
		return false
	}
}

func (ps *pkgScan) recordUnresolvable(pos token.Pos, why string) {
	for _, have := range ps.unresolvableDispatch {
		if strings.HasSuffix(have, ": "+why) {
			return
		}
	}

	ps.unresolvableDispatch = append(ps.unresolvableDispatch, fmt.Sprintf("%s: %s", ps.fset.Position(pos), why))
}

func stringParamNames(fd *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}

	for _, field := range fd.Type.Params.List {
		if id, ok := field.Type.(*ast.Ident); !ok || id.Name != "string" {
			continue
		}

		for _, n := range field.Names {
			out[n.Name] = true
		}
	}

	return out
}

// recordIfDispatch binds `if action == "Op" { return h.handler(...) }` chains.
func (ps *pkgScan) recordIfDispatch(st *ast.IfStmt, enclosingFunc string) {
	fd, ok := ps.funcDecls[enclosingFunc]
	if !ok || st.Body == nil {
		return
	}

	cond, ok := st.Cond.(*ast.BinaryExpr)
	if !ok || cond.Op != token.EQL {
		return
	}

	id, ok := cond.X.(*ast.Ident)
	if !ok || !isOpParamName(id.Name) || !stringParamNames(fd)[id.Name] {
		return
	}

	op, dyn := ps.resolveKey(cond.Y)
	if dyn || !pascalOpRe.MatchString(op) {
		return
	}

	handler := ps.findHandlerCall(st.Body.List)
	if handler == "" {
		return
	}

	ps.deferredBindings = append(ps.deferredBindings, deferredBinding{
		ops: []string{op}, handler: handler, group: "if@" + enclosingFunc, source: enclosingFunc,
	})
}

// deferredBinding is a looser dispatch shape that must never rebind or
// conflict with an op a stricter convention already resolved.
type deferredBinding struct {
	handler string
	group   string
	source  string
	ops     []string
}

func (ps *pkgScan) deferLocalFuncCase(ops []string, body []ast.Stmt, group, source string) {
	if handler := ps.findLocalFuncCall(body); handler != "" {
		ps.deferredBindings = append(ps.deferredBindings, deferredBinding{
			ops: ops, handler: handler, group: group, source: source,
		})
	}
}

func (ps *pkgScan) applyDeferredBindings() {
	for _, d := range ps.deferredBindings {
		for _, op := range d.ops {
			if _, bound := ps.opToHandler[op]; bound {
				continue
			}

			if _, amb := ps.ambiguousOps[op]; amb {
				continue
			}

			ps.handlerSourceFunc[d.handler] = d.source
			ps.bindOp(op, d.handler, d.group)
		}
	}
}

func isOpParamTag(tag ast.Expr) bool {
	id, ok := tag.(*ast.Ident)

	return ok && isOpParamName(id.Name)
}

// findLocalFuncCall returns the package function a lone `return f(...)` calls.
func (ps *pkgScan) findLocalFuncCall(stmts []ast.Stmt) string {
	if len(stmts) != 1 {
		return ""
	}

	ret, ok := stmts[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return ""
	}

	call, ok := ret.Results[0].(*ast.CallExpr)
	if !ok {
		return ""
	}

	if id, isIdent := call.Fun.(*ast.Ident); isIdent {
		if _, declared := ps.funcDecls[id.Name]; declared {
			return id.Name
		}
	}

	return ""
}

func isFuncMapConstructor(ps *pkgScan, rhs ast.Expr) bool {
	switch v := rhs.(type) {
	case *ast.CompositeLit:
		return ps.mapValueIsFuncType(v.Type)
	case *ast.CallExpr:
		if fn, ok := v.Fun.(*ast.Ident); ok && fn.Name == "make" && len(v.Args) > 0 {
			return ps.mapValueIsFuncType(v.Args[0])
		}
	}

	return false
}

func (ps *pkgScan) localDispatchMapVars(fd *ast.FuncDecl) map[string]bool {
	vars := map[string]bool{}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != len(as.Rhs) {
			return true
		}

		for i, lhs := range as.Lhs {
			if id, isIdent := lhs.(*ast.Ident); isIdent && isFuncMapConstructor(ps, as.Rhs[i]) {
				vars[id.Name] = true
			}
		}

		return true
	})

	return vars
}

// recordIncrementalDispatch handles a func-valued map built in one function:
// constant-key index assignments bind, computed-key ones are unresolvable.
func (ps *pkgScan) recordIncrementalDispatch(fd *ast.FuncDecl) {
	vars := ps.localDispatchMapVars(fd)
	if len(vars) == 0 {
		return
	}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Lhs) != 1 || len(as.Rhs) != 1 {
			return true
		}

		ix, ok := as.Lhs[0].(*ast.IndexExpr)
		if !ok {
			return true
		}

		base, ok := ix.X.(*ast.Ident)
		if !ok || !vars[base.Name] {
			return true
		}

		ps.recordIncrementalEntry(fd, base.Name, ix.Index, as.Rhs[0], as.Pos())

		return true
	})
}

func (ps *pkgScan) recordIncrementalEntry(fd *ast.FuncDecl, mapVar string, key, value ast.Expr, pos token.Pos) {
	op, dyn := ps.resolveKey(key)
	if dyn || op == "" {
		ps.recordUnresolvable(pos, "dispatch map "+mapVar+" gets computed-key entries in "+fd.Name.Name)

		return
	}

	name := findHandlerSelector(value)
	if name == "" {
		name = ps.findHandlerSelectorLoose(value)
	}

	if name == "" {
		return
	}

	ps.handlerSourceFunc[name] = fd.Name.Name
	ps.bindOp(op, name, "local@"+fd.Name.Name+"."+mapVar)
}

// recordSpecTableLookup flags `spec, ok := table[action]` where the looked-up
// value is never called: ops are data rows, not per-op handlers.
func (ps *pkgScan) recordSpecTableLookup(fd *ast.FuncDecl) {
	params := stringParamNames(fd)

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || len(as.Rhs) != 1 || len(as.Lhs) == 0 {
			return true
		}

		ix, ok := as.Rhs[0].(*ast.IndexExpr)
		if !ok {
			return true
		}

		key, isIdent := ix.Index.(*ast.Ident)
		val, isLHSIdent := as.Lhs[0].(*ast.Ident)

		if isIdent && isLHSIdent && isOpParamName(key.Name) && params[key.Name] && !isCalled(fd.Body, val.Name) {
			ps.recordUnresolvable(as.Pos(), "data-driven op table looked up by "+key.Name+" in "+fd.Name.Name)
		}

		return true
	})
}

func isCalled(body *ast.BlockStmt, name string) bool {
	called := false

	ast.Inspect(body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			if id, isIdent := call.Fun.(*ast.Ident); isIdent && id.Name == name {
				called = true
			}
		}

		return !called
	})

	return called
}

// unboundSDKOps lists real SDK ops with no dispatch binding, only meaningful
// when the scan found a non-static dispatch construction.
func unboundSDKOps(idx *sdkIndex, ps *pkgScan) []string {
	if len(ps.unresolvableDispatch) == 0 {
		return nil
	}

	var out []string

	for op := range idx.ops {
		if _, bound := ps.opToHandler[op]; bound {
			continue
		}

		if _, amb := ps.ambiguousOps[op]; amb {
			continue
		}

		out = append(out, op)
	}

	sort.Strings(out)

	return out
}
