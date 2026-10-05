package main

import (
	"go/ast"
)

const (
	maxKeyHelperDepth = 3
	selForm           = "Form"
	selPostForm       = "PostForm"
	selHeader         = "Header"
)

// matchKeyReaderHelperCall credits the fixed query/form/header keys a helper reads
// when handed a request source, directly or through helpers it calls.
func matchKeyReaderHelperCall(call *ast.CallExpr, ctx handlerResolveCtx, res *opResolution) {
	if !isBareOrHandlerCall(unwrapInstantiation(call.Fun)) {
		return
	}

	collectHelperKeys(call, ctx, maxKeyHelperDepth, map[*ast.FuncDecl]bool{}, func(name string) {
		declareLiteralField(res, name)
	})
}

func unwrapInstantiation(fn ast.Expr) ast.Expr {
	switch v := fn.(type) {
	case *ast.IndexExpr:
		return v.X
	case *ast.IndexListExpr:
		return v.X
	default:
		return fn
	}
}

func collectHelperKeys(
	call *ast.CallExpr,
	ctx handlerResolveCtx,
	depth int,
	visited map[*ast.FuncDecl]bool,
	emit func(string),
) {
	fd := lookupFuncDecl(unwrapInstantiation(call.Fun), ctx)
	if fd == nil || fd.Body == nil || visited[fd] || !handedRequestSource(fd, call) {
		return
	}

	visited[fd] = true

	urlValues := urlValuesParamNames(fromFuncDecl(fd), ctx)
	headers := headerParamNames(fd)

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		inner, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}

		if key, isRead := fixedKeyRead(inner, urlValues, headers, ctx); isRead {
			emit(key)
		} else if depth > 0 && isBareOrHandlerCall(unwrapInstantiation(inner.Fun)) {
			collectHelperKeys(inner, ctx, depth-1, visited, emit)
		}

		return true
	})
}

// handedRequestSource reports whether call passes a non-nil value at a
// request-carrying parameter of fd.
func handedRequestSource(fd *ast.FuncDecl, call *ast.CallExpr) bool {
	for i, t := range flattenFieldTypes(fd.Type.Params) {
		if !isRequestSourceType(t) || i >= len(call.Args) {
			continue
		}

		if id, isIdent := call.Args[i].(*ast.Ident); !isIdent || id.Name != "nil" {
			return true
		}
	}

	return false
}

func isRequestSourceType(t ast.Expr) bool {
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	sel, ok := t.(*ast.SelectorExpr)
	if !ok {
		return false
	}

	pkg, isIdent := sel.X.(*ast.Ident)
	if !isIdent {
		return false
	}

	switch pkg.Name + "." + sel.Sel.Name {
	case "echo.Context", "http.Request", "url.Values", "http.Header":
		return true
	default:
		return false
	}
}

// fixedKeyRead returns the literal or package-const key of a query, form or
// header read, with any X-Amz- header prefix stripped.
func fixedKeyRead(
	call *ast.CallExpr,
	urlValues, headers map[string]bool,
	ctx handlerResolveCtx,
) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) == 0 {
		return "", false
	}

	header := isHeaderGetCall(call) || isHeaderParamGet(sel, headers)
	if !header && !isKeyedQueryRead(sel, urlValues) {
		return "", false
	}

	key, ok := resolveStringExpr(call.Args[0], ctx.pkgConsts)
	if !ok || key == "" {
		return "", false
	}

	if header {
		key = stripHeaderPrefix(key)
	}

	return key, true
}

func isKeyedQueryRead(sel *ast.SelectorExpr, urlValues map[string]bool) bool {
	if queryParamSelectors[sel.Sel.Name] || sel.Sel.Name == "PostFormValue" || isInlineQueryGet(sel) {
		return true
	}

	if sel.Sel.Name != methodGet {
		return false
	}

	switch recv := sel.X.(type) {
	case *ast.Ident:
		return urlValues[recv.Name]
	case *ast.SelectorExpr:
		return recv.Sel.Name == selForm || recv.Sel.Name == selPostForm
	default:
		return false
	}
}

func headerParamNames(fd *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}

	for _, p := range fd.Type.Params.List {
		sel, ok := p.Type.(*ast.SelectorExpr)
		if !ok || sel.Sel.Name != selHeader {
			continue
		}

		for _, n := range p.Names {
			out[n.Name] = true
		}
	}

	return out
}

func isHeaderParamGet(sel *ast.SelectorExpr, headers map[string]bool) bool {
	id, ok := sel.X.(*ast.Ident)

	return ok && sel.Sel.Name == methodGet && headers[id.Name]
}
