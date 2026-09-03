package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// minHandlerParams is the parameter count of a service.WrapOp-wrapped
// handler: (context.Context, *In). The request type is always the last one.
const minHandlerParams = 2

const wrapOpFuncName = "WrapOp"

// handlerResolveCtx bundles the structural lookups op resolution needs.
type handlerResolveCtx struct {
	fset           *token.FileSet
	structs        map[string]structDef
	methods        map[string][]*ast.FuncDecl
	funcs          map[string]*ast.FuncDecl
	wrapOpWrappers map[string]bool
}

func collectFuncs(files []*ast.File) (map[string][]*ast.FuncDecl, map[string]*ast.FuncDecl) {
	methods := map[string][]*ast.FuncDecl{}
	funcs := map[string]*ast.FuncDecl{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}

			if fd.Recv != nil {
				methods[fd.Name.Name] = append(methods[fd.Name.Name], fd)
			} else {
				funcs[fd.Name.Name] = fd
			}
		}
	}

	return methods, funcs
}

func collectPackageStringConsts(files []*ast.File) map[string]string {
	out := map[string]string{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.CONST {
				continue
			}

			for _, spec := range gd.Specs {
				addStringValueSpec(spec, out)
			}
		}
	}

	return out
}

func addStringValueSpec(spec ast.Spec, out map[string]string) {
	vs, ok := spec.(*ast.ValueSpec)
	if !ok || len(vs.Names) != 1 || len(vs.Values) != 1 {
		return
	}

	lit, ok := vs.Values[0].(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return
	}

	if v, err := strconv.Unquote(lit.Value); err == nil {
		out[vs.Names[0].Name] = v
	}
}

// collectLocalFuncTypeNames finds every package-level `type X func(...)...`
// declaration, so a dispatch table keyed by such a named type (apigateway's
// `map[string]actionFn`) is recognised the same way a literal `func(...)...`
// map value or `service.JSONOpFunc` is.
func collectLocalFuncTypeNames(files []*ast.File) map[string]bool {
	out := map[string]bool{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				ts, tsOK := spec.(*ast.TypeSpec)
				if !tsOK {
					continue
				}

				if _, isFunc := ts.Type.(*ast.FuncType); isFunc {
					out[ts.Name.Name] = true
				}
			}
		}
	}

	return out
}

// isDispatchMapType reports whether t is map[string]<func-shaped-value>:
// a literal func type, a named local func type (apigateway's actionFn), or
// service.JSONOpFunc specifically (kept as its own case since it's a
// qualified selector, not a bare identifier).
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
// glue's shape: `[]struct{ name string; bind func(*Handler) T }{...}` --
// returning the field names to key each element literal by. Generalized
// from cmd/reqfieldscan's jsonOpFuncBinderFields: the bind field may return
// any func-shaped value, not only service.JSONOpFunc specifically.
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

// collectDispatchEntries is the union, across the whole package, of every
// op-name -> value-expr pair found in any recognised dispatch-table shape.
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

// collectSwitchDispatchEntries handles acmpca's real shape (and appsync's,
// iotwireless's, amplify's, dynamodbstreams's): `switch action { case
// "CreateCertificateAuthority": return h.jsonCreateCA(ctx, body) ... }`,
// a switch statement keyed by operation name rather than a map literal at
// all. Every switch statement in the package is scanned unconditionally,
// with no attempt to first confirm its tag expression is actually an
// operation-name variable -- an unrelated switch's case labels (rarely
// even string literals; almost never a PascalCase AWS operation name by
// coincidence) simply never gets looked up by resolveOp, so the cost of
// over-collecting here is zero. Multiple case values (`case "A", "B":`)
// each map to the same case body's resolved expression.
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
// wrapAccuracy[I,O](fn) shape. A dispatch-table value calling one of these
// decodes exactly like a direct service.WrapOp call.
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

// resolveWrapOpReqType resolves a `service.WrapOp(handlerArg)` (or local
// wrapper) call's request type directly from the handler's own function
// signature -- the *In parameter type. Returns ("", false) when expr is not
// such a call at all, so the caller can fall through to body-scan
// resolution instead.
func resolveWrapOpReqType(expr ast.Expr, ctx handlerResolveCtx) (string, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return "", false
	}

	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		if fn.Sel.Name != wrapOpFuncName {
			return "", false
		}
	case *ast.Ident:
		if !ctx.wrapOpWrappers[fn.Name] {
			return "", false
		}
	default:
		return "", false
	}

	reqType, ok := resolveHandlerReqType(call.Args[0], ctx)

	return reqType, ok
}

func resolveHandlerReqType(arg ast.Expr, ctx handlerResolveCtx) (string, bool) {
	var ft *ast.FuncType

	switch v := arg.(type) {
	case *ast.SelectorExpr:
		cands, ok := ctx.methods[v.Sel.Name]
		if !ok || len(cands) == 0 {
			return "", false
		}

		ft = cands[0].Type
	case *ast.Ident:
		fd, ok := ctx.funcs[v.Name]
		if !ok {
			return "", false
		}

		ft = fd.Type
	case *ast.FuncLit:
		ft = v.Type
	default:
		return "", false
	}

	return resolveReqTypeFromFuncType(ft, ctx.structs)
}

func resolveReqTypeFromFuncType(ft *ast.FuncType, structs map[string]structDef) (string, bool) {
	total := 0

	var last *ast.Field

	for _, p := range ft.Params.List {
		n := len(p.Names)
		if n == 0 {
			n = 1
		}

		total += n
		last = p
	}

	if total < minHandlerParams || last == nil {
		return "", false
	}

	star, ok := last.Type.(*ast.StarExpr)
	if !ok {
		return "", false
	}

	id, ok := star.X.(*ast.Ident)
	if !ok {
		return "", false
	}

	if _, known := structs[id.Name]; !known {
		return "", false
	}

	return id.Name, true
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
