package main

import (
	"go/ast"
	"slices"
	"strings"
)

// opRoot is one entry point this scan will walk (emit.go) looking for
// error-code emissions attributable to an operation. Domain is the
// receiver-type name of the FuncDecl the root came from ("" for a bare
// package function or an unbound func literal) -- used only to disambiguate
// which pinned SDK module governs this operation when a service resolves
// more than one (moduleassign.go); see this package's doc comment for why
// that split exists at all (services/bedrock's Handler vs AgentsHandler).
type opRoot struct {
	Body   *ast.BlockStmt
	Domain string
	Name   string
}

// resolveOpRoots finds every entry point serving operation op, exactly as
// cmd/reqfielddiff's resolveOp does for a request TYPE: BOTH the package's
// dispatch table AND a name-convention handler search are tried, and
// whatever each finds is UNIONED rather than one being preferred and the
// search stopped -- deliberately over-inclusive, per that tool's own
// reasoning (a spurious extra root costs a human a few seconds; a
// suppressed one manufactures a missed finding, the worse failure for a
// scan whose premise is "an undeclared code is real, not a resolution gap").
func resolveOpRoots(op string, idx *pkgIndex) []opRoot {
	var out []opRoot

	if expr, ok := idx.Dispatch[op]; ok {
		out = append(out, resolveDispatchValueRoots(expr, idx)...)
	}

	if fds := findHandlersByName(op, idx); len(fds) > 0 {
		for _, fd := range fds {
			out = append(out, opRoot{Body: fd.Body, Domain: receiverTypeName(fd), Name: funcKey(fd)})
		}
	}

	return dedupRoots(out)
}

func dedupRoots(roots []opRoot) []opRoot {
	seen := map[*ast.BlockStmt]bool{}

	var out []opRoot

	for _, r := range roots {
		if r.Body == nil || seen[r.Body] {
			continue
		}

		seen[r.Body] = true

		out = append(out, r)
	}

	return out
}

// resolveDispatchValueRoots unwraps a dispatch-table value expression -- a
// direct WrapOp/wrapper call, a func literal whose first return forwards to
// one, or a func literal with real logic of its own -- to the root(s) whose
// body should be scanned.
func resolveDispatchValueRoots(expr ast.Expr, idx *pkgIndex) []opRoot {
	expr = unwrapParen(expr)

	if lit, isLit := expr.(*ast.FuncLit); isLit {
		if ret := firstReturnExpr(lit.Body); ret != nil {
			if roots := resolveCallLikeRoots(ret, idx); len(roots) > 0 {
				return roots
			}
		}

		return []opRoot{{Body: lit.Body, Name: "<closure>"}}
	}

	return resolveCallLikeRoots(expr, idx)
}

func resolveCallLikeRoots(expr ast.Expr, idx *pkgIndex) []opRoot {
	if handlerArg, ok := wrapOpHandlerArg(expr, idx); ok {
		return resolveHandlerArgRoots(handlerArg, idx)
	}

	switch v := expr.(type) {
	case *ast.CallExpr:
		return resolveCalleeRoots(v.Fun, idx)
	case *ast.SelectorExpr:
		return resolveCalleeRoots(v, idx)
	case *ast.Ident:
		return resolveCalleeRoots(v, idx)
	default:
		return nil
	}
}

// wrapOpHandlerArg reports whether expr is `service.WrapOp(handlerArg)` (or
// a local forwarding wrapper), returning the handler argument itself --
// unlike cmd/reqfielddiff's resolveWrapOpReqType, this tool wants the
// HANDLER FUNCTION to scan, not its request type, so the argument is
// returned unresolved for the caller to turn into root(s) directly.
func wrapOpHandlerArg(expr ast.Expr, idx *pkgIndex) (ast.Expr, bool) {
	call, ok := expr.(*ast.CallExpr)
	if !ok || len(call.Args) != 1 {
		return nil, false
	}

	switch fn := call.Fun.(type) {
	case *ast.SelectorExpr:
		if fn.Sel.Name != wrapOpFuncName {
			return nil, false
		}
	case *ast.Ident:
		if !idx.WrapOpWrappers[fn.Name] {
			return nil, false
		}
	default:
		return nil, false
	}

	return call.Args[0], true
}

func resolveHandlerArgRoots(arg ast.Expr, idx *pkgIndex) []opRoot {
	switch v := arg.(type) {
	case *ast.FuncLit:
		return []opRoot{{Body: v.Body, Name: "<closure>"}}
	case *ast.SelectorExpr, *ast.Ident:
		return resolveCalleeRoots(v, idx)
	default:
		return nil
	}
}

func resolveCalleeRoots(fn ast.Expr, idx *pkgIndex) []opRoot {
	switch v := fn.(type) {
	case *ast.SelectorExpr:
		return methodRoots(v.Sel.Name, idx)
	case *ast.Ident:
		if fd, ok := idx.Funcs[v.Name]; ok && fd.Body != nil {
			return []opRoot{{Body: fd.Body, Name: fd.Name.Name}}
		}
	}

	return nil
}

func methodRoots(name string, idx *pkgIndex) []opRoot {
	cands, ok := idx.Methods[name]
	if !ok {
		return nil
	}

	var out []opRoot

	for _, fd := range cands {
		if fd.Body == nil {
			continue
		}

		out = append(out, opRoot{Body: fd.Body, Domain: receiverTypeName(fd), Name: funcKey(fd)})
	}

	return out
}

func receiverTypeName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return ""
	}

	return underlyingIdentType(fd.Recv.List[0].Type)
}

func underlyingIdentType(expr ast.Expr) string {
	switch e := expr.(type) {
	case *ast.Ident:
		return e.Name
	case *ast.StarExpr:
		if id, ok := e.X.(*ast.Ident); ok {
			return id.Name
		}
	}

	return ""
}

func funcKey(fd *ast.FuncDecl) string {
	if fd.Recv != nil {
		return "(" + receiverTypeName(fd) + ")." + fd.Name.Name
	}

	return fd.Name.Name
}

// findHandlersByName is the name-convention fallback: "handle"+op, then the
// suffixed variants this repo is known to use (handle<Op>Full/Accurate/
// WithOpts -- cmd/reqfieldscan's package doc, blind spot 3), then
// lowerCamel(op)+"Action"/op+"Action" (apigateway's shape) and bare
// lowerCamel(op) (appsync's shape), then case-insensitively against either
// "handle"+op or bare op. Returns every distinct FuncDecl matched by any
// candidate name (there is almost always exactly one; a second is returned
// rather than discarded when a name genuinely collides across receiver
// types, so moduleassign.go's overlap heuristic gets a chance to sort out
// which domain each belongs to instead of this function silently guessing).
func findHandlersByName(op string, idx *pkgIndex) []*ast.FuncDecl {
	seen := map[*ast.FuncDecl]bool{}

	var out []*ast.FuncDecl

	add := func(fd *ast.FuncDecl) {
		if fd == nil || fd.Body == nil || seen[fd] {
			return
		}

		seen[fd] = true

		out = append(out, fd)
	}

	candidates := []string{
		"handle" + op,
		"handle" + op + "Full",
		"handle" + op + "Accurate",
		"handle" + op + "WithOpts",
		lowerFirst(op) + "Action",
		op + "Action",
		lowerFirst(op),
	}

	for _, name := range candidates {
		for _, fd := range idx.Methods[name] {
			add(fd)
		}

		add(idx.Funcs[name])
	}

	if len(out) > 0 {
		return out
	}

	return findHandlersByNameFold(op, idx)
}

func findHandlersByNameFold(op string, idx *pkgIndex) []*ast.FuncDecl {
	targets := []string{strings.ToLower("handle" + op), strings.ToLower(op)}

	var out []*ast.FuncDecl

	for name, fds := range idx.Methods {
		if slices.Contains(targets, strings.ToLower(name)) {
			out = append(out, fds...)
		}
	}

	for name, fd := range idx.Funcs {
		if slices.Contains(targets, strings.ToLower(name)) {
			out = append(out, fd)
		}
	}

	return out
}

func lowerFirst(s string) string {
	if s == "" {
		return s
	}

	return strings.ToLower(s[:1]) + s[1:]
}
