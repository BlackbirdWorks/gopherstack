package main

import (
	"go/ast"
	"go/token"
	"sort"
	"strconv"
	"strings"
)

// minWrapOpParams is the parameter count of every service.WrapOp-wrapped
// handler: (context.Context, *In). The request type is always the last one.
const minWrapOpParams = 2

const (
	// jsonOpFuncTypeName is service.JSONOpFunc's own bare identifier, as it
	// appears in a selector expression (service.JSONOpFunc) anywhere this
	// scan matches it structurally rather than by go/types.
	jsonOpFuncTypeName = "JSONOpFunc"
	// wrapOpFuncName is service.WrapOp's own bare identifier, matched the
	// same way.
	wrapOpFuncName = "WrapOp"
)

// resolvedHandler is what a service.WrapOp(...) call site resolved to.
type resolvedHandler struct {
	ReqType string
	Reason  string
	File    string
	Line    int
}

// handlerResolveCtx bundles the structural lookups every handler/value
// resolution step needs, so resolution functions take one argument instead
// of four positionally-identical maps.
type handlerResolveCtx struct {
	fset           *token.FileSet
	structs        map[string]structDef
	methods        map[string][]*ast.FuncDecl
	funcs          map[string]*ast.FuncDecl
	wrapOpWrappers map[string]bool
}

// isJSONOpFuncMapType reports whether t is a map[string]service.JSONOpFunc
// type expression -- the dispatch-table shape most scanned services use,
// possibly assembled from several such literals merged at startup (see
// route53resolver's buildOps, which unions 13 of them).
func isJSONOpFuncMapType(t ast.Expr) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}

	sel, ok := mt.Value.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == jsonOpFuncTypeName
}

// jsonOpFuncBinderFields reports whether t is a slice-of-struct dispatch
// table -- glue's glueOpBindings shape:
//
//	[]struct{ name string; bind func(*Handler) service.JSONOpFunc }{...}
//
// -- returning the field names to key each element literal by. A
// map[string]service.JSONOpFunc composite literal is the only dispatch
// shape isJSONOpFuncMapType recognises; this is the confirmed second one
// (gopherstack-43o8). A repo-wide grep for any other field of type
// `func(...) service.JSONOpFunc` found only this one instance, in glue.
func jsonOpFuncBinderFields(t ast.Expr) (string, string, bool) {
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

		if ft, isFunc := f.Type.(*ast.FuncType); isFunc && returnsJSONOpFunc(ft) {
			bindField = name
		}
	}

	return nameField, bindField, nameField != "" && bindField != ""
}

func returnsJSONOpFunc(ft *ast.FuncType) bool {
	if ft.Results == nil || len(ft.Results.List) != 1 {
		return false
	}

	sel, ok := ft.Results.List[0].Type.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == jsonOpFuncTypeName
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

// collectDispatchTableEntries is the union of every op-name -> value-expr
// pair across every dispatch-table composite literal in the package,
// regardless of which of the two known shapes built it -- used both as the
// dispatch-table denominator (its key set) and, per entry, to resolve that
// op's handler directly by the value actually bound to it rather than by
// reconstructing "handle"+opName (gopherstack-43o8 fix c).
func collectDispatchTableEntries(files []*ast.File, pkgConsts map[string]string) map[string]ast.Expr {
	out := map[string]ast.Expr{}

	collectMapLiteralEntries(files, pkgConsts, out)
	collectBinderSliceEntries(files, pkgConsts, out)

	return out
}

func collectMapLiteralEntries(files []*ast.File, pkgConsts map[string]string, out map[string]ast.Expr) {
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil || !isJSONOpFuncMapType(cl.Type) {
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

// collectBinderSliceEntries handles the slice-of-struct shape: for each
// keyed struct-literal element (glue's real elements are always keyed,
// `{name: "...", bind: func(...) {...}}`), the op name comes from the
// string field and the dispatch value comes from the binder func literal's
// own return statement, e.g. `return service.WrapOp(h.handleFoo)`.
func collectBinderSliceEntries(files []*ast.File, pkgConsts map[string]string, out map[string]ast.Expr) {
	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok || cl.Type == nil {
				return true
			}

			nameField, bindField, isBinder := jsonOpFuncBinderFields(cl.Type)
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

// dispatchTableOpNames is the sorted, deduped key set of entries -- the
// dispatch-table denominator used when GetSupportedOperations has no
// static list of its own.
func dispatchTableOpNames(entries map[string]ast.Expr) []string {
	out := make([]string, 0, len(entries))
	for k := range entries {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// firstReturnExpr finds the single-result expression of the first return
// statement reachable in body without crossing into a nested func literal
// -- used both to read a binder field's own return value and to recognise
// a WrapOp-forwarding wrapper function's body.
func firstReturnExpr(body *ast.BlockStmt) ast.Expr {
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
// wrapAccuracy[I,O](fn) shape (handler.go:484). Matching the literal
// selector name "WrapOp" alone makes every call site reached only through
// such a wrapper invisible (gopherstack-43o8 fix b); a dispatch-table value
// calling one of these decodes exactly like a direct service.WrapOp call.
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

// resolveValueExprToReqType resolves one dispatch-table entry's value
// expression directly -- either a literal service.WrapOp(...) call, or a
// call through a local WrapOp-forwarding wrapper -- to the request type its
// handler decodes into.
func resolveValueExprToReqType(expr ast.Expr, ctx handlerResolveCtx) (string, string) {
	call, ok := unwrapParen(expr).(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return "", "unsupported dispatch value shape"
	}

	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		if fn.Sel.Name != wrapOpFuncName {
			return "", "dispatch value is not a WrapOp call"
		}
	case *ast.Ident:
		if !ctx.wrapOpWrappers[fn.Name] {
			return "", "dispatch value is not a WrapOp call"
		}
	default:
		return "", "unsupported dispatch value shape"
	}

	return resolveHandlerReqType(call.Args[0], ctx)
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

// collectWrapOpFuncNames finds every service.WrapOp(...) call anywhere in
// the package -- regardless of which map literal's value position it
// occupies, or how that map is keyed -- and resolves each to its handler's
// request type, keyed by the handler's own name (a bound method's or
// package func's identifier). This is the FALLBACK resolution path, kept
// for batch's dispatch table, which is keyed by REST path
// ("/v1/createcomputeenvironment") rather than the canonical operation name
// ("CreateComputeEnvironment") GetSupportedOperations advertises -- a shape
// collectDispatchTableEntries's op-keyed direct resolution cannot reach,
// since its key IS the dispatch table's own key. Keying this map by
// HANDLER NAME instead sidesteps that mismatch: this repo's handler naming
// is uniformly "handle" + the canonical op name in every service read
// while building this tool, aside from the suffixed exceptions
// resolveOneOp's direct path now catches first. A func-literal argument has
// no stable name to key by and is skipped here.
func collectWrapOpFuncNames(files []*ast.File, ctx handlerResolveCtx) map[string]resolvedHandler {
	out := map[string]resolvedHandler{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != wrapOpFuncName || len(call.Args) != 1 {
				return true
			}

			name, ok := handlerArgName(call.Args[0])
			if !ok {
				return true
			}

			reqType, reason := resolveHandlerReqType(call.Args[0], ctx)
			pos := ctx.fset.Position(call.Args[0].Pos())
			out[name] = resolvedHandler{ReqType: reqType, Reason: reason, File: pos.Filename, Line: pos.Line}

			return true
		})
	}

	return out
}

func handlerArgName(arg ast.Expr) (string, bool) {
	switch v := arg.(type) {
	case *ast.SelectorExpr:
		return v.Sel.Name, true
	case *ast.Ident:
		return v.Name, true
	default:
		return "", false
	}
}

func resolveHandlerReqType(arg ast.Expr, ctx handlerResolveCtx) (string, string) {
	var ft *ast.FuncType

	switch v := arg.(type) {
	case *ast.SelectorExpr:
		cands, ok := ctx.methods[v.Sel.Name]
		if !ok || len(cands) == 0 {
			return "", "handler method " + v.Sel.Name + " not found"
		}

		ft = cands[0].Type
	case *ast.Ident:
		fd, ok := ctx.funcs[v.Name]
		if !ok {
			return "", "handler func " + v.Name + " not found"
		}

		ft = fd.Type
	case *ast.FuncLit:
		ft = v.Type
	default:
		return "", "unsupported WrapOp argument shape"
	}

	return resolveReqTypeFromFuncType(ft, ctx.structs)
}

func resolveReqTypeFromFuncType(ft *ast.FuncType, structs map[string]structDef) (string, string) {
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

	if total < minWrapOpParams || last == nil {
		return "", "handler has fewer than 2 parameters"
	}

	star, ok := last.Type.(*ast.StarExpr)
	if !ok {
		return "", "request parameter is not a pointer type"
	}

	id, ok := star.X.(*ast.Ident)
	if !ok {
		return "", "request parameter is not a named local type"
	}

	if _, known := structs[id.Name]; !known {
		return "", "request type " + id.Name + " is not a local struct"
	}

	return id.Name, ""
}

// collectLiteralSites finds every `json.Unmarshal(body, &x)` call in the
// package whose target x's type is resolvable from its own declaration in
// the enclosing function -- this repo's decode path OUTSIDE service.WrapOp
// (e.g. batch's handleTagResource, whose TagResource op is dispatched by
// HTTP method inside handleTags and never appears in any WrapOp call at
// all).
func collectLiteralSites(files []*ast.File, fset *token.FileSet, structs map[string]structDef) []literalSite {
	var out []literalSite

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			bindings := collectLocalBindings(fd, fset, structs)
			out = append(out, literalSitesInFunc(fd, fset, bindings)...)
		}
	}

	return out
}

func literalSitesInFunc(fd *ast.FuncDecl, fset *token.FileSet, bindings map[string]string) []literalSite {
	var out []literalSite

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		typeName, ok := unmarshalTargetType(call, bindings)
		if !ok {
			return true
		}

		pos := fset.Position(call.Pos())
		out = append(out, literalSite{FuncName: fd.Name.Name, ReqType: typeName, File: pos.Filename, Line: pos.Line})

		return true
	})

	return out
}

func unmarshalTargetType(call *ast.CallExpr, bindings map[string]string) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Unmarshal" {
		return "", false
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok || pkgIdent.Name != "json" || len(call.Args) != 2 {
		return "", false
	}

	unary, ok := call.Args[1].(*ast.UnaryExpr)
	if !ok || unary.Op != token.AND {
		return "", false
	}

	target, ok := unary.X.(*ast.Ident)
	if !ok {
		return "", false
	}

	typeName, ok := bindings[target.Name]

	return typeName, ok
}

// collectStaticOpList reads GetSupportedOperations's own body for a
// []string{...} composite literal (batch-style: a hardcoded op list that
// can include ops -- e.g. batch's tag trio -- dispatched outside any
// WrapOp call entirely). Services that instead build the list from h.ops's
// own keys at runtime (route53resolver, workspaces, dms) have no such
// literal and fall back to dispatchTableOpNames in scanFiles.
func collectStaticOpList(files []*ast.File, pkgConsts map[string]string) []string {
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Name.Name != "GetSupportedOperations" || fd.Body == nil {
				continue
			}

			if ops := findStringSliceLiteral(fd.Body, pkgConsts); len(ops) > 0 {
				return ops
			}
		}
	}

	return nil
}

func findStringSliceLiteral(body *ast.BlockStmt, pkgConsts map[string]string) []string {
	var out []string

	ast.Inspect(body, func(n ast.Node) bool {
		cl, ok := n.(*ast.CompositeLit)
		if !ok {
			return true
		}

		at, ok := cl.Type.(*ast.ArrayType)
		if !ok || at.Len != nil {
			return true
		}

		id, ok := at.Elt.(*ast.Ident)
		if !ok || id.Name != "string" {
			return true
		}

		for _, elt := range cl.Elts {
			if s, resolved := resolveStringExpr(elt, pkgConsts); resolved {
				out = append(out, s)
			}
		}

		return false
	})

	return out
}

// resolveDispatchTable is the tool's real per-op resolution: for every
// canonical op name in denom, try (1) the value actually bound to that op
// in a dispatch-table entry, resolved directly through WrapOp or a local
// WrapOp-forwarding wrapper; then (2) a service.WrapOp-wrapped "handle" +
// op-name handler, found anywhere in the package regardless of which table
// it lives in (needed for batch's REST-path-keyed table, where (1) can
// never match by construction); then (3) a linked literal json.Unmarshal
// decode site; then give up as unresolved -- never silently dropped from
// the count.
func resolveDispatchTable(
	denom []string, tableEntries map[string]ast.Expr, wrapOpFuncs map[string]resolvedHandler,
	sites []literalSite, ctx handlerResolveCtx,
) []dispatchEntry {
	lower := lowerKeyedHandlers(wrapOpFuncs)
	out := make([]dispatchEntry, 0, len(denom))

	for _, op := range denom {
		out = append(out, resolveOneOp(op, tableEntries, wrapOpFuncs, lower, sites, ctx))
	}

	return out
}

// lowerKeyedHandlers indexes wrapOpFuncs by lowercased name, for a
// case-insensitive fallback match against "handle" + op name -- this
// repo's Go handler names capitalize AWS acronyms (handleAssociate
// ResolverEndpointIPAddress), while the AWS operation name itself does not
// (AssociateResolverEndpointIpAddress); confirmed live in route53resolver.
// A name collision that only differs by case does not occur among this
// repo's handler methods (Go itself forbids two identically-spelled
// methods on one receiver), so this fallback narrows, never guesses wrong.
func lowerKeyedHandlers(wrapOpFuncs map[string]resolvedHandler) map[string]resolvedHandler {
	out := make(map[string]resolvedHandler, len(wrapOpFuncs))
	for name, rh := range wrapOpFuncs {
		out[strings.ToLower(name)] = rh
	}

	return out
}

func resolveOneOp(
	op string, tableEntries map[string]ast.Expr, wrapOpFuncs, lower map[string]resolvedHandler,
	sites []literalSite, ctx handlerResolveCtx,
) dispatchEntry {
	if rh, ok := resolveDirectTableEntry(op, tableEntries, ctx); ok {
		return wrapOpDispatchEntry(op, rh)
	}

	handlerName := "handle" + op

	if rh, ok := wrapOpFuncs[handlerName]; ok {
		return wrapOpDispatchEntry(op, rh)
	}

	if rh, ok := lower[strings.ToLower(handlerName)]; ok {
		return wrapOpDispatchEntry(op, rh)
	}

	if site, ok := findLiteralSiteForOp(op, sites); ok {
		return dispatchEntry{Op: op, Anchor: anchorLiteral, ReqType: site.ReqType, File: site.File, Line: site.Line}
	}

	return dispatchEntry{
		Op: op, Anchor: anchorUnresolved,
		Reason: "no " + handlerName + " resolvable via WrapOp (even case-insensitively), " +
			"a dispatch-table entry, or a linked literal decode",
	}
}

func resolveDirectTableEntry(
	op string,
	tableEntries map[string]ast.Expr,
	ctx handlerResolveCtx,
) (resolvedHandler, bool) {
	expr, ok := tableEntries[op]
	if !ok {
		return resolvedHandler{}, false
	}

	reqType, reason := resolveValueExprToReqType(expr, ctx)
	if reqType == "" {
		return resolvedHandler{}, false
	}

	pos := ctx.fset.Position(expr.Pos())

	return resolvedHandler{ReqType: reqType, Reason: reason, File: pos.Filename, Line: pos.Line}, true
}

func wrapOpDispatchEntry(op string, rh resolvedHandler) dispatchEntry {
	return dispatchEntry{
		Op:      op,
		Anchor:  anchorWrapOp,
		ReqType: rh.ReqType,
		Reason:  rh.Reason,
		File:    rh.File,
		Line:    rh.Line,
	}
}

func findLiteralSiteForOp(op string, sites []literalSite) (literalSite, bool) {
	opLower := strings.ToLower(op)

	for _, s := range sites {
		if strings.TrimPrefix(strings.ToLower(s.FuncName), "handle") == opLower {
			return s, true
		}
	}

	return literalSite{}, false
}
