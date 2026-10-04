package main

import (
	"go/ast"
	"go/token"
	"maps"
	"strconv"
	"strings"
)

const (
	sdkBindingPrefix = "sdk:"
	inputTypeSuffix  = "Input"
	maxHeaderHops    = 3
)

// matchSignatureReqStruct credits the *In of a (context.Context, *In) handler reached by
// name rather than through a WrapOp table entry.
func matchSignatureReqStruct(fl funcLike, ctx handlerResolveCtx, res *opResolution) {
	if fl.Params == nil {
		return
	}

	types := flattenFieldTypes(fl.Params)
	if len(types) < minHandlerParams || !isContextType(types[0]) {
		return
	}

	def, ok := resolveTypeExprDef(types[len(types)-1], ctx.structs, ctx)
	if !ok {
		return
	}

	res.StructsUsed = append(res.StructsUsed, def.Name)
	res.HasSignal = true

	maps.Copy(res.Fields, fieldMap(def))
}

func isContextType(t ast.Expr) bool {
	sel, ok := t.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != "Context" {
		return false
	}

	pkg, isIdent := sel.X.(*ast.Ident)

	return isIdent && pkg.Name == "context"
}

// matchGenericInstantiationDecode recognises decodeBody[T](body): an
// explicit instantiation of a local generic function that decodes.
func matchGenericInstantiationDecode(call *ast.CallExpr, ctx handlerResolveCtx, res *opResolution) {
	name, typeArg := instantiation(call.Fun)
	if name == "" || typeArg == nil {
		return
	}

	fd, ok := ctx.funcs[name]
	if !ok || fd.Type.TypeParams == nil || !funcDecodes(fd) {
		return
	}

	if id, isIdent := typeArg.(*ast.Ident); isIdent {
		addStructFields(id.Name, ctx, res)
	}
}

func instantiation(fn ast.Expr) (string, ast.Expr) {
	var base, arg ast.Expr

	switch v := fn.(type) {
	case *ast.IndexExpr:
		base, arg = v.X, v.Index
	case *ast.IndexListExpr:
		if len(v.Indices) == 0 {
			return "", nil
		}

		base, arg = v.X, v.Indices[0]
	default:
		return "", nil
	}

	id, ok := base.(*ast.Ident)
	if !ok {
		return "", nil
	}

	return id.Name, arg
}

func funcDecodes(fd *ast.FuncDecl) bool {
	if isDecodeVerb(fd.Name.Name) {
		return true
	}

	found := false

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok && isDecodeVerb(callName(call.Fun)) {
			found = true
		}

		return !found
	})

	return found
}

// matchHeaderHelperCall credits header names a helper reads when the call
// passes it a request's http.Header (s3's aclXMLFromGrantHeaders(r.Header, ...)).
func matchHeaderHelperCall(call *ast.CallExpr, formKeys map[string]string, ctx handlerResolveCtx, res *opResolution) {
	if len(formKeys) == 0 || !isBareOrHandlerCall(call.Fun) || !passesHeaderArg(call) {
		return
	}

	fd := lookupFuncDecl(call.Fun, ctx)
	if fd == nil {
		return
	}

	collectHeaderLiterals(fd, ctx, maxHeaderHops, map[*ast.FuncDecl]bool{}, func(s string) {
		stripped := stripHeaderPrefix(s)
		if stripped == s {
			return
		}

		if canonical, ok := formKeys[normalizeWireName(stripped)]; ok {
			recordFormRead(res, canonical, s)
		}
	})
}

func passesHeaderArg(call *ast.CallExpr) bool {
	for _, a := range call.Args {
		if sel, ok := a.(*ast.SelectorExpr); ok && sel.Sel.Name == "Header" {
			return true
		}
	}

	return false
}

func collectHeaderLiterals(
	fd *ast.FuncDecl,
	ctx handlerResolveCtx,
	hops int,
	visited map[*ast.FuncDecl]bool,
	emit func(string),
) {
	if fd == nil || fd.Body == nil || visited[fd] {
		return
	}

	visited[fd] = true

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BasicLit:
			if v.Kind == token.STRING {
				if s, err := strconv.Unquote(v.Value); err == nil {
					emit(s)
				}
			}
		case *ast.CallExpr:
			if hops > 0 && isBareOrHandlerCall(v.Fun) {
				collectHeaderLiterals(lookupFuncDecl(v.Fun, ctx), ctx, hops-1, visited, emit)
			}
		}

		return true
	})
}

// sdkInputBindingType names the SDK Input type a `pkg.XInput` declaration
// binds, or "" when t is not one.
func sdkInputBindingType(t ast.Expr) string {
	if star, ok := t.(*ast.StarExpr); ok {
		t = star.X
	}

	sel, ok := t.(*ast.SelectorExpr)
	if !ok || !strings.HasSuffix(sel.Sel.Name, inputTypeSuffix) {
		return ""
	}

	if _, isIdent := sel.X.(*ast.Ident); !isIdent {
		return ""
	}

	return sdkBindingPrefix + sel.Sel.Name
}

// addSDKInputDecode declares every field of op when a decode binds the op's
// own SDK Input type: the SDK struct IS the request shape.
func addSDKInputDecode(typeName string, ctx handlerResolveCtx, res *opResolution) bool {
	sdkName, ok := strings.CutPrefix(typeName, sdkBindingPrefix)
	if !ok {
		return false
	}

	if ctx.opName != "" && sdkName == ctx.opName+inputTypeSuffix {
		res.sdkInputDecoded = true
		res.HasSignal = true
	}

	return true
}

//nolint:gochecknoglobals // read-only lookup table, same pattern as queryParamSelectors
var requestFormSelectors = map[string]bool{"FormValue": true, "PostFormValue": true}

// matchRequestFormCall matches r.FormValue/Form.Get/PostForm.Get keyed by a literal or
// Sprintf prefix naming one of this op's fields (sns' Attributes.entry.%d.key).
func matchRequestFormCall(
	call *ast.CallExpr,
	formKeys map[string]string,
	res *opResolution,
	localLits map[string]string,
) {
	if len(formKeys) == 0 || len(call.Args) == 0 {
		return
	}

	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return
	}

	isForm := requestFormSelectors[sel.Sel.Name]
	if inner, isSel := sel.X.(*ast.SelectorExpr); isSel && sel.Sel.Name == methodGet {
		isForm = inner.Sel.Name == "Form" || inner.Sel.Name == "PostForm"
	}

	if isForm {
		matchExprLiteral(call.Args[0], formKeys, res, false, localLits)
	}
}
