package main

import "go/ast"

// goStringType is the Go builtin `string` identifier's own name, shared by
// every scalar/map-key type check in this file and mapfields.go/
// formreads.go that needs to recognise it.
const goStringType = "string"

// isAnyType reports whether t is `any` or the equivalent empty
// `interface{}` -- the "dst" parameter shape iot's readBody(c, dst any)
// uses to forward an address-of argument into a decode call one level
// down.
func isAnyType(t ast.Expr) bool {
	switch v := t.(type) {
	case *ast.Ident:
		return v.Name == "any"
	case *ast.InterfaceType:
		return v.Methods == nil || len(v.Methods.List) == 0
	default:
		return false
	}
}

// isStringType reports whether t is the bare `string` identifier.
func isStringType(t ast.Expr) bool {
	id, ok := t.(*ast.Ident)

	return ok && id.Name == goStringType
}

// flattenFieldTypes expands fl's field groups (`a, b string` is two
// parameters, both typed string) into one type expression per logical
// parameter/result, in declaration order -- matching how Go call arguments
// and multi-value returns line up positionally.
func flattenFieldTypes(fl *ast.FieldList) []ast.Expr {
	if fl == nil {
		return nil
	}

	var out []ast.Expr

	for _, f := range fl.List {
		n := len(f.Names)
		if n == 0 {
			n = 1
		}

		for range n {
			out = append(out, f.Type)
		}
	}

	return out
}

// flatParamNamed pairs one flattened parameter's name with its own flat
// index, for the two wrapper-detection passes below that need to map a
// parameter identifier back to its call-site argument position.
type flatParamNamed struct {
	name string
	idx  int
}

// namedParamsMatching returns every flattened parameter of fl whose type
// satisfies want, alongside its flat index.
func namedParamsMatching(fl *ast.FieldList, want func(ast.Expr) bool) []flatParamNamed {
	if fl == nil {
		return nil
	}

	var out []flatParamNamed

	idx := 0

	for _, p := range fl.List {
		names := p.Names
		if len(names) == 0 {
			idx++

			continue
		}

		for _, n := range names {
			if want(p.Type) {
				out = append(out, flatParamNamed{name: n.Name, idx: idx})
			}

			idx++
		}
	}

	return out
}

// findForwardedArgIndex scans fd's body for a call to a function/method
// named by wantCall whose own first argument is directly one of
// candidates' identifiers (no unwrapping needed -- both the dst-any and
// query-accessor shapes forward their own parameter untouched), returning
// that candidate's flat index. Used by both collectLocalDecodeDstWrappers
// (wantCall: isDecodeVerb) and collectQueryAccessorWrappers (wantCall: a
// queryParamSelectors selector).
func findForwardedArgIndex(
	fd *ast.FuncDecl,
	candidates []flatParamNamed,
	matchesCall func(*ast.CallExpr) bool,
) (int, bool) {
	if len(candidates) == 0 {
		return 0, false
	}

	found := -1

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if found != -1 {
			return false
		}

		call, ok := n.(*ast.CallExpr)
		if !ok || !matchesCall(call) {
			return true
		}

		for _, arg := range call.Args {
			id, isIdent := arg.(*ast.Ident)
			if !isIdent {
				continue
			}

			for _, c := range candidates {
				if c.name == id.Name {
					found = c.idx

					return false
				}
			}
		}

		return true
	})

	if found == -1 {
		return 0, false
	}

	return found, true
}

// collectLocalDecodeDstWrappers finds package-level functions whose body
// decodes directly into one of their OWN `any`-typed parameters (iot's
// readBody(c, dst any) error: `json.NewDecoder(...).Decode(dst)`, no
// address-of needed since dst is already `any` holding whatever pointer
// the caller passed) -- so a call to one of these with `&boundVar`
// declares boundVar's struct the same way a direct json.Unmarshal(&x) call
// would (matchDecodeDstWrapperCall, resolve.go).
func collectLocalDecodeDstWrappers(files []*ast.File) map[string]int {
	out := map[string]int{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil || fd.Type.Params == nil {
				continue
			}

			candidates := namedParamsMatching(fd.Type.Params, isAnyType)

			idx, ok := findForwardedArgIndex(fd, candidates, func(call *ast.CallExpr) bool {
				return isDecodeVerb(callName(call.Fun))
			})
			if ok {
				out[fd.Name.Name] = idx
			}
		}
	}

	return out
}

// collectQueryAccessorWrappers finds package-level functions whose body
// forwards one of their own STRING parameters straight into a
// queryParamSelectors call (cleanrooms' qp(c,key) -> c.QueryParam(key),
// iot's parseInt32QueryParam(c,name) -> c.QueryParam(name)) -- so a call to
// one of these with a literal (or resolvable package const) argument
// declares that wire name the same way a direct c.QueryParam("name") call
// would (matchQueryAccessorWrapperCall, resolve.go).
func collectQueryAccessorWrappers(files []*ast.File) map[string]int {
	out := map[string]int{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Body == nil || fd.Type.Params == nil {
				continue
			}

			candidates := namedParamsMatching(fd.Type.Params, isStringType)

			idx, ok := findForwardedArgIndex(fd, candidates, func(call *ast.CallExpr) bool {
				sel, isSel := call.Fun.(*ast.SelectorExpr)

				return isSel && queryParamSelectors[sel.Sel.Name] && len(call.Args) > 0
			})
			if ok {
				out[fd.Name.Name] = idx
			}
		}
	}

	return out
}

// typeParamNames flattens a generic function's own type-parameter field
// list to the set of names it declares (ssm's jsonOp[I, O any] declares
// {"I", "O"}).
func typeParamNames(fl *ast.FieldList) map[string]bool {
	out := map[string]bool{}

	for _, f := range fl.List {
		for _, n := range f.Names {
			out[n.Name] = true
		}
	}

	return out
}

// collectGenericDecodeWrapperFuncs finds package-level GENERIC functions
// with a callback parameter whose OWN signature's last parameter is (or
// points to) one of the wrapper's own type parameters -- ssm's
// jsonOp[I,O any](fn func(context.Context, *I) (O, error)),
// apigatewayv2's handleCreate[I,O any](..., backendFn func(I) (*O,
// error)), dynamodb's handleOp[WireIn,...](..., toSDK func(*WireIn)
// *SDKIn, ...). This is a purely STRUCTURAL match on the wrapper's own
// declared signature, independent of what its body actually does with
// that callback -- resolveGenericCallbackReqType/matchGenericCallbackCall
// then resolve the CALL SITE's own callback argument to a concrete
// request struct, which only ever succeeds when that argument's own
// signature names a struct this scan already knows about, so a wrapper
// matched here for an unrelated reason (a generic Map/Filter-shaped
// helper, say) can never manufacture a false "declared" field on its own.
func collectGenericDecodeWrapperFuncs(files []*ast.File) map[string]int {
	out := map[string]int{}

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Recv != nil || fd.Body == nil || fd.Type.TypeParams == nil {
				continue
			}

			tp := typeParamNames(fd.Type.TypeParams)

			if idx, found := findCallbackParamIndex(fd.Type, tp); found {
				out[fd.Name.Name] = idx
			}
		}
	}

	return out
}

// findCallbackParamIndex returns the flat index of ft's first
// func-typed parameter whose own last parameter targets one of typeParams.
func findCallbackParamIndex(ft *ast.FuncType, typeParams map[string]bool) (int, bool) {
	if ft.Params == nil {
		return 0, false
	}

	idx := 0

	for _, p := range ft.Params.List {
		n := len(p.Names)
		if n == 0 {
			n = 1
		}

		if inner, ok := p.Type.(*ast.FuncType); ok && callbackTargetsTypeParam(inner, typeParams) {
			return idx, true
		}

		idx += n
	}

	return 0, false
}

// callbackTargetsTypeParam reports whether ft's own last parameter is `*T`
// or bare `T` for some T in typeParams.
func callbackTargetsTypeParam(ft *ast.FuncType, typeParams map[string]bool) bool {
	if ft.Params == nil || len(ft.Params.List) == 0 {
		return false
	}

	last := ft.Params.List[len(ft.Params.List)-1]

	name := ""

	switch t := last.Type.(type) {
	case *ast.StarExpr:
		if id, ok := t.X.(*ast.Ident); ok {
			name = id.Name
		}
	case *ast.Ident:
		name = t.Name
	}

	return name != "" && typeParams[name]
}
