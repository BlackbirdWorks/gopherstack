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

// resolvedHandler is what a service.WrapOp(...) call site resolved to.
type resolvedHandler struct {
	ReqType string
	Reason  string
	File    string
	Line    int
}

// isJSONOpFuncMapType reports whether t is a map[string]service.JSONOpFunc
// type expression -- the dispatch-table shape every scanned service uses,
// possibly assembled from several such literals merged at startup (see
// route53resolver's buildOps, which unions 13 of them).
func isJSONOpFuncMapType(t ast.Expr) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}

	sel, ok := mt.Value.(*ast.SelectorExpr)

	return ok && sel.Sel.Name == "JSONOpFunc"
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

// collectDispatchMapKeys is the union of every key across every
// map[string]service.JSONOpFunc composite literal in the package, deduped
// and sorted -- used as the dispatch-table denominator ONLY when
// GetSupportedOperations has no static list of its own (route53resolver,
// workspaces, dms all key their ops maps by canonical AWS operation name
// directly, so the map's own keys already ARE that list).
func collectDispatchMapKeys(files []*ast.File, pkgConsts map[string]string) []string {
	seen := map[string]bool{}

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
					seen[key] = true
				}
			}

			return true
		})
	}

	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}

	sort.Strings(out)

	return out
}

// collectWrapOpFuncNames finds every service.WrapOp(...) call anywhere in
// the package -- regardless of which map literal's value position it
// occupies, or how that map is keyed -- and resolves each to its handler's
// request type, keyed by the handler's own name (a bound method's or
// package func's identifier). Batch's dispatch table is keyed by REST path
// ("/v1/createcomputeenvironment"), not by the canonical operation name
// ("CreateComputeEnvironment") GetSupportedOperations advertises; keying
// this map by HANDLER NAME instead of by whatever string the dispatch
// table itself used sidesteps that mismatch entirely, since this repo's
// handler naming is uniformly "handle" + the canonical op name in every
// service read while building this tool. A func-literal argument has no
// stable name to key by and is skipped here -- never observed among the
// four services this tool covers, disclosed in the package doc.
func collectWrapOpFuncNames(
	files []*ast.File, fset *token.FileSet, structs map[string]structDef,
	methods map[string][]*ast.FuncDecl, funcs map[string]*ast.FuncDecl,
) map[string]resolvedHandler {
	out := map[string]resolvedHandler{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			sel, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || sel.Sel.Name != "WrapOp" || len(call.Args) != 1 {
				return true
			}

			name, ok := handlerArgName(call.Args[0])
			if !ok {
				return true
			}

			reqType, reason := resolveHandlerReqType(call.Args[0], structs, methods, funcs)
			pos := fset.Position(call.Args[0].Pos())
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

func resolveHandlerReqType(
	arg ast.Expr, structs map[string]structDef, methods map[string][]*ast.FuncDecl, funcs map[string]*ast.FuncDecl,
) (string, string) {
	var ft *ast.FuncType

	switch v := arg.(type) {
	case *ast.SelectorExpr:
		cands, ok := methods[v.Sel.Name]
		if !ok || len(cands) == 0 {
			return "", "handler method " + v.Sel.Name + " not found"
		}

		ft = cands[0].Type
	case *ast.Ident:
		fd, ok := funcs[v.Name]
		if !ok {
			return "", "handler func " + v.Name + " not found"
		}

		ft = fd.Type
	case *ast.FuncLit:
		ft = v.Type
	default:
		return "", "unsupported WrapOp argument shape"
	}

	return resolveReqTypeFromFuncType(ft, structs)
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

			bindings := collectLocalBindings(fd, structs)
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
// literal and fall back to collectDispatchMapKeys in scanFiles.
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
// canonical op name in denom, try (1) a service.WrapOp-wrapped "handle" +
// op-name handler, then (2) a linked literal json.Unmarshal decode site,
// then give up as unresolved -- never silently dropped from the count.
func resolveDispatchTable(denom []string, wrapOpFuncs map[string]resolvedHandler, sites []literalSite) []dispatchEntry {
	lower := lowerKeyedHandlers(wrapOpFuncs)
	out := make([]dispatchEntry, 0, len(denom))

	for _, op := range denom {
		out = append(out, resolveOneOp(op, wrapOpFuncs, lower, sites))
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

func resolveOneOp(op string, wrapOpFuncs, lower map[string]resolvedHandler, sites []literalSite) dispatchEntry {
	handlerName := "handle" + op

	if rh, ok := wrapOpFuncs[handlerName]; ok {
		return dispatchEntry{
			Op:      op,
			Anchor:  anchorWrapOp,
			ReqType: rh.ReqType,
			Reason:  rh.Reason,
			File:    rh.File,
			Line:    rh.Line,
		}
	}

	if rh, ok := lower[strings.ToLower(handlerName)]; ok {
		return dispatchEntry{
			Op:      op,
			Anchor:  anchorWrapOp,
			ReqType: rh.ReqType,
			Reason:  rh.Reason,
			File:    rh.File,
			Line:    rh.Line,
		}
	}

	if site, ok := findLiteralSiteForOp(op, sites); ok {
		return dispatchEntry{Op: op, Anchor: anchorLiteral, ReqType: site.ReqType, File: site.File, Line: site.Line}
	}

	return dispatchEntry{
		Op: op, Anchor: anchorUnresolved,
		Reason: "no " + handlerName + " resolvable via WrapOp (even case-insensitively) or a linked literal decode",
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
