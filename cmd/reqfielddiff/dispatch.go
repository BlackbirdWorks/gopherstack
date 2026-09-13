package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// minHandlerParams is the parameter count of a service.WrapOp-wrapped
// handler: (context.Context, *In). The request type is always the last one.
const minHandlerParams = 2

// minGenericCallbackParams gates resolveHandlerReqType's callback-argument
// path (ssm's jsonOp, apigatewayv2's handleCreate/handleUpdate,
// dynamodb's handleOp): unlike WrapOp's fixed (ctx, *In) shape, these
// callbacks are sometimes a single `func(I) (*O, error)` with no context
// parameter at all, so only one param -- the request type itself -- can be
// required.
const minGenericCallbackParams = 1

const wrapOpFuncName = "WrapOp"

// handlerResolveCtx bundles the structural lookups op resolution needs.
type handlerResolveCtx struct {
	fset                  *token.FileSet
	structs               map[string]structDef
	methods               map[string][]*ast.FuncDecl
	funcs                 map[string]*ast.FuncDecl
	wrapOpWrappers        map[string]bool
	genericDecodeWrappers map[string]int
	decodeDstWrappers     map[string]int
	queryAccessorWrappers map[string]int
	subPackages           map[string]subPackageIndex
	pkgConsts             map[string]string
}

// subPackageIndex is the same structural index as handlerResolveCtx's own
// structs/funcs maps, built for one in-repo subpackage a service imports
// (dynamodb/models is the confirmed instance: its handleOp[...] dispatch
// wrapper decodes into a WireIn type whose value is resolved from
// models.ToSDK<Op>Input's own parameter type, which lives in this
// subpackage, not the service package being scanned). Methods are never
// needed here -- a subpackage's exported conversion functions are always
// bare package funcs in every observed instance.
type subPackageIndex struct {
	structs map[string]structDef
	funcs   map[string]*ast.FuncDecl
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

// isDispatchMapType reports whether t is map[string]<func-shaped-value> --
// a literal `map[string]F{...}` composite literal type -- or a bare
// identifier naming a package-level `type X = map[string]F` (or `type X
// map[string]F`) declaration whose own value type is func-shaped
// (namedMapTypes, appstream's `opTable{...}`: a composite literal of a
// named/aliased map type is spelled as a bare *ast.Ident, never an
// *ast.MapType, so the literal-map-type check alone can never see it).
func isDispatchMapType(t ast.Expr, funcTypeNames, namedMapTypes map[string]bool) bool {
	switch v := t.(type) {
	case *ast.MapType:
		return isDispatchMapValueType(v.Value, funcTypeNames)
	case *ast.Ident:
		return namedMapTypes[v.Name]
	default:
		return false
	}
}

// isDispatchMapValueType is isDispatchMapType's value-type check, factored
// out so collectNamedDispatchMapTypes can apply the identical test to a
// `type X map[string]F` declaration's own map type.
func isDispatchMapValueType(v ast.Expr, funcTypeNames map[string]bool) bool {
	switch val := v.(type) {
	case *ast.FuncType:
		return true
	case *ast.SelectorExpr:
		return val.Sel.Name == "JSONOpFunc"
	case *ast.Ident:
		return funcTypeNames[val.Name]
	default:
		return false
	}
}

// collectNamedDispatchMapTypes finds every package-level `type X =
// map[string]F` (a type alias) or `type X map[string]F` (a defined type)
// declaration whose value type is func-shaped, so a dispatch table built
// from a composite literal of that NAMED type is recognised the same way a
// literal map[string]F{} is -- see isDispatchMapType's doc.
func collectNamedDispatchMapTypes(files []*ast.File, funcTypeNames map[string]bool) map[string]bool {
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

				mt, isMap := ts.Type.(*ast.MapType)
				if isMap && isDispatchMapValueType(mt.Value, funcTypeNames) {
					out[ts.Name.Name] = true
				}
			}
		}
	}

	return out
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
	funcTypeNames, namedMapTypes map[string]bool,
) map[string]ast.Expr {
	out := map[string]ast.Expr{}

	collectMapLiteralEntries(files, pkgConsts, funcTypeNames, namedMapTypes, out)
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
	funcTypeNames, namedMapTypes map[string]bool,
	out map[string]ast.Expr,
) {
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil || !isDispatchMapType(cl.Type, funcTypeNames, namedMapTypes) {
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
// signature -- the *In parameter type. Returns (structDef{}, false) when
// expr is not such a call at all, so the caller can fall through to
// body-scan resolution instead.
func resolveWrapOpReqType(expr ast.Expr, ctx handlerResolveCtx) (structDef, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return structDef{}, false
	}

	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		if fn.Sel.Name != wrapOpFuncName {
			return structDef{}, false
		}
	case *ast.Ident:
		if !ctx.wrapOpWrappers[fn.Name] {
			return structDef{}, false
		}
	default:
		return structDef{}, false
	}

	return resolveHandlerReqType(call.Args[0], ctx, minHandlerParams)
}

// resolveGenericCallbackReqType resolves a call to a LOCAL generic
// "decode-dispatch wrapper" function (ssm's jsonOp[I,O], apigatewayv2's
// handleCreate/handleUpdate[I,O], dynamodb's handleOp[WireIn,...] --
// collectGenericDecodeWrapperFuncs, wrappers.go) to the request struct
// carried by its own callback ARGUMENT, at whichever position that
// function's signature says the callback sits.
func resolveGenericCallbackReqType(expr ast.Expr, ctx handlerResolveCtx) (structDef, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok {
		return structDef{}, false
	}

	id, ok := call.Fun.(*ast.Ident)
	if !ok {
		return structDef{}, false
	}

	idx, ok := ctx.genericDecodeWrappers[id.Name]
	if !ok || idx >= len(call.Args) {
		return structDef{}, false
	}

	return resolveHandlerReqType(call.Args[idx], ctx, minGenericCallbackParams)
}

// resolveHandlerReqType resolves arg -- a function reference (bare ident,
// method selector, or in-repo subpackage-qualified selector) or a func
// literal -- to the request struct its OWN signature's last parameter
// names, requiring at least minParams total parameters. A SelectorExpr
// whose receiver identifier names a known in-repo subpackage
// (ctx.subPackages, dynamodb's `models.ToSDKCreateTableInput`) resolves the
// callee and its struct within THAT subpackage's own index rather than this
// package's; any other selector resolves the method by name alone,
// ignoring the receiver's real type -- the same single-name lookup this
// scan uses throughout (see resolve.go's lookupFuncDecl).
func resolveHandlerReqType(arg ast.Expr, ctx handlerResolveCtx, minParams int) (structDef, bool) {
	switch v := arg.(type) {
	case *ast.SelectorExpr:
		if recv, isIdent := v.X.(*ast.Ident); isIdent {
			if sub, isSubPkg := ctx.subPackages[recv.Name]; isSubPkg {
				fd, found := sub.funcs[v.Sel.Name]
				if !found {
					return structDef{}, false
				}

				return resolveReqTypeFromFuncType(fd.Type, sub.structs, ctx, minParams)
			}
		}

		cands, ok := ctx.methods[v.Sel.Name]
		if !ok || len(cands) == 0 {
			return structDef{}, false
		}

		return resolveReqTypeFromFuncType(cands[0].Type, ctx.structs, ctx, minParams)
	case *ast.Ident:
		fd, ok := ctx.funcs[v.Name]
		if !ok {
			return structDef{}, false
		}

		return resolveReqTypeFromFuncType(fd.Type, ctx.structs, ctx, minParams)
	case *ast.FuncLit:
		return resolveReqTypeFromFuncType(v.Type, ctx.structs, ctx, minParams)
	default:
		return structDef{}, false
	}
}

// resolveReqTypeFromFuncType extracts ft's last (flattened) parameter's
// type and resolves it to a structDef -- accepting `T`, `*T` (WrapOp's
// pointer convention; apigatewayv2's handleCreate/handleUpdate pass the
// request BY VALUE instead: `backendFn func(I) (*O, error)`), or a
// package-qualified `pkg.T`/`*pkg.T` (dynamodb's local, unqualified
// `toSDKPutItemInputChecked(input *models.PutItemInput) (...)` wrapper:
// the wrapper itself is a bare package func resolved against structs, but
// its OWN parameter type is qualified, resolved instead via
// ctx.subPackages -- see resolveTypeExprDef).
func resolveReqTypeFromFuncType(
	ft *ast.FuncType,
	structs map[string]structDef,
	ctx handlerResolveCtx,
	minParams int,
) (structDef, bool) {
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

	if total < minParams || last == nil {
		return structDef{}, false
	}

	return resolveTypeExprDef(last.Type, structs, ctx)
}

// resolveTypeExprDef resolves t -- `T`, `*T`, `pkg.T`, or `*pkg.T` -- to a
// known structDef: a bare/pointer local type name against structs, or a
// package-qualified one against ctx.subPackages[pkg].structs.
func resolveTypeExprDef(t ast.Expr, structs map[string]structDef, ctx handlerResolveCtx) (structDef, bool) {
	if star, isPtr := t.(*ast.StarExpr); isPtr {
		t = star.X
	}

	switch id := t.(type) {
	case *ast.Ident:
		def, known := structs[id.Name]

		return def, known
	case *ast.SelectorExpr:
		pkgIdent, isIdent := id.X.(*ast.Ident)
		if !isIdent {
			return structDef{}, false
		}

		sub, isSubPkg := ctx.subPackages[pkgIdent.Name]
		if !isSubPkg {
			return structDef{}, false
		}

		def, known := sub.structs[id.Sel.Name]

		return def, known
	default:
		return structDef{}, false
	}
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
