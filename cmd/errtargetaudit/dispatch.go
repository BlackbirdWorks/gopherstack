package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// This file's dispatch-table recognition is the same structural approach as
// cmd/reqfielddiff/dispatch.go: it does not model what a dispatch value
// resolves TO (that is resolveop.go's job here, aimed at a handler function
// body rather than a request type), only which shapes bind an operation
// name to a value expression at all. Reimplemented rather than imported --
// see this package's doc comment for why -- with the identical three shapes
// (map literal, slice-of-struct binder, switch-statement dispatch) that
// cmd/reqfieldscan and cmd/reqfielddiff needed to reach every service.

const wrapOpFuncName = "WrapOp"

func isDispatchMapType(t ast.Expr, funcTypeNames map[string]bool) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}

	switch v := mt.Value.(type) {
	case *ast.FuncType:
		return true
	case *ast.SelectorExpr:
		return v.Sel.Name == "JSONOpFunc"
	case *ast.Ident:
		return funcTypeNames[v.Name]
	default:
		return false
	}
}

// binderFields reports whether t is a slice-of-struct dispatch table --
// glue's shape: `[]struct{ name string; bind func(*Handler) T }{...}`.
func binderFields(t ast.Expr) (string, string, bool) {
	at, isSlice := t.(*ast.ArrayType)
	if !isSlice || at.Len != nil {
		return "", "", false
	}

	st, isStruct := at.Elt.(*ast.StructType)
	if !isStruct || st.Fields == nil {
		return "", "", false
	}

	var nameField, bindField string

	for _, f := range st.Fields.List {
		if len(f.Names) != 1 {
			continue
		}

		name := f.Names[0].Name

		if id, isIdent := f.Type.(*ast.Ident); isIdent && id.Name == "string" {
			nameField = name

			continue
		}

		if _, isFunc := f.Type.(*ast.FuncType); isFunc {
			bindField = name
		}
	}

	return nameField, bindField, nameField != "" && bindField != ""
}

func resolveStringExpr(e ast.Expr, pkgConsts map[string]string) (string, bool) {
	switch v := e.(type) {
	case *ast.BasicLit:
		if v.Kind != token.STRING {
			return "", false
		}

		s, err := strconv.Unquote(v.Value)

		return s, err == nil
	case *ast.Ident:
		s, ok := pkgConsts[v.Name]

		return s, ok
	default:
		return "", false
	}
}

func collectDispatchEntries(
	files []*ast.File,
	pkgConsts map[string]string,
	funcTypeNames map[string]bool,
) map[string]ast.Expr {
	out := map[string]ast.Expr{}

	collectMapLiteralEntries(files, pkgConsts, funcTypeNames, out)
	collectBinderSliceEntries(files, pkgConsts, out)
	collectSwitchDispatchEntries(files, pkgConsts, out)

	return out
}

func collectSwitchDispatchEntries(files []*ast.File, pkgConsts map[string]string, out map[string]ast.Expr) {
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			sw, ok := n.(*ast.SwitchStmt)
			if !ok {
				return true
			}

			for _, stmt := range sw.Body.List {
				cc, ccOK := stmt.(*ast.CaseClause)
				if !ccOK {
					continue
				}

				addSwitchCaseEntries(cc, pkgConsts, out)
			}

			return true
		})
	}
}

func addSwitchCaseEntries(cc *ast.CaseClause, pkgConsts map[string]string, out map[string]ast.Expr) {
	ret := firstReturnExpr(&ast.BlockStmt{List: cc.Body})
	if ret == nil {
		return
	}

	for _, caseExpr := range cc.List {
		if key, resolved := resolveStringExpr(caseExpr, pkgConsts); resolved {
			out[key] = ret
		}
	}
}

func collectMapLiteralEntries(
	files []*ast.File,
	pkgConsts map[string]string,
	funcTypeNames map[string]bool,
	out map[string]ast.Expr,
) {
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil || !isDispatchMapType(cl.Type, funcTypeNames) {
				return true
			}

			for _, elt := range cl.Elts {
				kv, kvOK := elt.(*ast.KeyValueExpr)
				if !kvOK {
					continue
				}

				if key, resolved := resolveStringExpr(kv.Key, pkgConsts); resolved {
					out[key] = kv.Value
				}
			}

			return true
		})
	}
}

func collectBinderSliceEntries(files []*ast.File, pkgConsts map[string]string, out map[string]ast.Expr) {
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil {
				return true
			}

			nameField, bindField, isBinder := binderFields(cl.Type)
			if !isBinder {
				return true
			}

			for _, elt := range cl.Elts {
				addBinderElement(elt, nameField, bindField, pkgConsts, out)
			}

			return true
		})
	}
}

func addBinderElement(elt ast.Expr, nameField, bindField string, pkgConsts map[string]string, out map[string]ast.Expr) {
	ecl, ok := elt.(*ast.CompositeLit)
	if !ok {
		return
	}

	var nameExpr, bindExpr ast.Expr

	for _, e := range ecl.Elts {
		kv, kvOK := e.(*ast.KeyValueExpr)
		if !kvOK {
			continue
		}

		key, keyOK := kv.Key.(*ast.Ident)
		if !keyOK {
			continue
		}

		switch key.Name {
		case nameField:
			nameExpr = kv.Value
		case bindField:
			bindExpr = kv.Value
		}
	}

	if nameExpr == nil || bindExpr == nil {
		return
	}

	name, resolved := resolveStringExpr(nameExpr, pkgConsts)

	lit, isLit := bindExpr.(*ast.FuncLit)
	if !resolved || !isLit {
		return
	}

	if ret := firstReturnExpr(lit.Body); ret != nil {
		out[name] = ret
	}
}

// firstReturnExpr finds the single-result expression of the first return
// statement reachable in body without crossing into a nested func literal.
func firstReturnExpr(body *ast.BlockStmt) ast.Expr {
	if body == nil {
		return nil
	}

	var found ast.Expr

	ast.Inspect(body, func(n ast.Node) bool {
		if found != nil {
			return false
		}

		switch v := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.ReturnStmt:
			if len(v.Results) == 1 {
				found = v.Results[0]
			}

			return false
		}

		return true
	})

	return found
}

// collectLocalWrapOpWrappers finds package-level functions whose entire
// body is `return service.WrapOp(<own parameter>)` -- cognitoidp's
// wrapAccuracy[I,O](fn) shape.
func collectLocalWrapOpWrappers(files []*ast.File) map[string]bool {
	out := map[string]bool{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Body == nil {
				continue
			}

			if isWrapOpForwarder(fd) {
				out[fd.Name.Name] = true
			}
		}
	}

	return out
}

func isWrapOpForwarder(fd *ast.FuncDecl) bool {
	ret := firstReturnExpr(fd.Body)
	if ret == nil {
		return false
	}

	call, ok := ret.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return false
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != wrapOpFuncName {
		return false
	}

	arg, ok := call.Args[0].(*ast.Ident)

	return ok && isOwnParam(fd, arg.Name)
}

func isOwnParam(fd *ast.FuncDecl, name string) bool {
	if fd.Type.Params == nil {
		return false
	}

	for _, p := range fd.Type.Params.List {
		for _, n := range p.Names {
			if n.Name == name {
				return true
			}
		}
	}

	return false
}

func unwrapParen(e ast.Expr) ast.Expr {
	for {
		p, ok := e.(*ast.ParenExpr)
		if !ok {
			return e
		}

		e = p.X
	}
}
