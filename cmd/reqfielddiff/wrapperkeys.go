package main

import (
	"go/ast"
	"slices"
	"strings"
)

const wrapperKeyRounds = 4

func isByteSliceType(t ast.Expr) bool {
	at, ok := t.(*ast.ArrayType)
	if !ok || at.Len != nil {
		return false
	}

	id, isIdent := at.Elt.(*ast.Ident)

	return isIdent && id.Name == "byte"
}

type rawKeyScan struct {
	structs map[string]structDef
	consts  map[string]string
}

func collectBodyFuncs(files []*ast.File) ([]*ast.FuncDecl, map[string]*ast.FuncDecl) {
	byName := map[string]*ast.FuncDecl{}

	var decls []*ast.FuncDecl

	for _, f := range files {
		for _, d := range f.Decls {
			fd, ok := d.(*ast.FuncDecl)
			if !ok || fd.Body == nil || fd.Type.Params == nil {
				continue
			}

			decls = append(decls, fd)

			if _, dup := byName[fd.Name.Name]; !dup {
				byName[fd.Name.Name] = fd
			}
		}
	}

	return decls, byName
}

// collectWrapperKeyCredits maps each op name gated by a body-reading
// wrapper to the fixed keys the wrapper reads off the raw body.
func collectWrapperKeyCredits(
	files []*ast.File,
	structs map[string]structDef,
	consts map[string]string,
) map[string][]string {
	sc := rawKeyScan{structs: structs, consts: consts}
	decls, byName := collectBodyFuncs(files)
	keys := map[string][]string{}

	for _, fd := range decls {
		if k := sc.directKeys(fd); len(k) > 0 {
			keys[fd.Name.Name] = k
		}
	}

	for range wrapperKeyRounds {
		for _, fd := range decls {
			sc.addForwardedKeys(fd, keys)
		}
	}

	out := map[string][]string{}

	for _, fd := range decls {
		k := keys[fd.Name.Name]
		if len(k) == 0 || len(namedParamsMatching(fd.Type.Params, isStringType)) == 0 {
			continue
		}

		for _, op := range sc.gatedOps(fd, byName) {
			out[op] = append(out[op], k...)
		}
	}

	return out
}

func byteParamNames(fd *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}
	for _, p := range namedParamsMatching(fd.Type.Params, isByteSliceType) {
		out[p.name] = true
	}

	return out
}

func (sc rawKeyScan) addForwardedKeys(fd *ast.FuncDecl, keys map[string][]string) {
	body := byteParamNames(fd)
	if len(body) == 0 {
		return
	}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !argsMention(call.Args, body) {
			return true
		}

		for _, k := range keys[callName(call.Fun)] {
			if !slices.Contains(keys[fd.Name.Name], k) {
				keys[fd.Name.Name] = append(keys[fd.Name.Name], k)
			}
		}

		return true
	})
}

func argsMention(args []ast.Expr, names map[string]bool) bool {
	for _, a := range args {
		if id, ok := a.(*ast.Ident); ok && names[id.Name] {
			return true
		}
	}

	return false
}

// directKeys returns the literal keys fd reads from a decode of its own
// []byte parameter: map[string]any indexing, struct tags, or gjson.
func (sc rawKeyScan) directKeys(fd *ast.FuncDecl) []string {
	body := byteParamNames(fd)
	if len(body) == 0 {
		return nil
	}

	locals := localTypeExprs(fd.Body)
	maps := map[string]bool{}

	var out []string

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if call, ok := n.(*ast.CallExpr); ok {
			out = append(out, sc.callKeys(call, body, locals, maps)...)
		}

		return true
	})

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		if idx, ok := n.(*ast.IndexExpr); ok {
			out = append(out, sc.indexKey(idx, maps)...)
		}

		return true
	})

	return out
}

func (sc rawKeyScan) indexKey(idx *ast.IndexExpr, maps map[string]bool) []string {
	id, isID := idx.X.(*ast.Ident)
	if !isID || !maps[id.Name] {
		return nil
	}

	if k, isLit := resolveStringExpr(idx.Index, sc.consts); isLit {
		return []string{k}
	}

	return nil
}

func (sc rawKeyScan) callKeys(
	call *ast.CallExpr,
	body map[string]bool,
	locals map[string]ast.Expr,
	maps map[string]bool,
) []string {
	if k, isGjson := sc.gjsonKey(call, body); isGjson {
		return []string{k}
	}

	target, decoded := rawDecodeTarget(call, body)
	if !decoded {
		return nil
	}

	t := locals[target]
	if isMapAnyValue(t) {
		maps[target] = true

		return nil
	}

	var out []string
	for _, f := range sc.structs[underlyingIdentType(t)].Fields {
		out = append(out, f.WireName)
	}

	return out
}

func isMapAnyValue(t ast.Expr) bool {
	mt, ok := t.(*ast.MapType)
	if !ok {
		return false
	}

	k, isIdent := mt.Key.(*ast.Ident)

	return isIdent && k.Name == goStringType
}

func (sc rawKeyScan) gjsonKey(call *ast.CallExpr, body map[string]bool) (string, bool) {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || len(call.Args) < minMapFieldCallArgs {
		return "", false
	}

	pkg, isIdent := sel.X.(*ast.Ident)
	if !isIdent || pkg.Name != "gjson" || (sel.Sel.Name != "Get" && sel.Sel.Name != "GetBytes") {
		return "", false
	}

	if !argsMention(call.Args[:1], body) {
		return "", false
	}

	return resolveStringExpr(call.Args[1], sc.consts)
}

// rawDecodeTarget matches json.Unmarshal(body, &x) and
// json.NewDecoder(...body...).Decode(&x), returning x.
func rawDecodeTarget(call *ast.CallExpr, body map[string]bool) (string, bool) {
	if len(call.Args) == 0 {
		return "", false
	}

	name := strings.ToLower(callName(call.Fun))
	dst := call.Args[len(call.Args)-1]

	switch {
	case name == "unmarshal" && len(call.Args) == minMapFieldCallArgs && argsMention(call.Args[:1], body):
	case name == "decode" && len(call.Args) == 1 && mentionsIdent(call.Fun, body):
	default:
		return "", false
	}

	u, ok := dst.(*ast.UnaryExpr)
	if !ok {
		return "", false
	}

	id, isIdent := u.X.(*ast.Ident)

	return id.Name, isIdent
}

func mentionsIdent(e ast.Expr, names map[string]bool) bool {
	found := false

	ast.Inspect(e, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok && names[id.Name] {
			found = true
		}

		return !found
	})

	return found
}

// localTypeExprs maps local variable names to their declared or literal type.
func localTypeExprs(body *ast.BlockStmt) map[string]ast.Expr {
	out := map[string]ast.Expr{}

	ast.Inspect(body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.ValueSpec:
			recordValueSpecType(v, out)
		case *ast.AssignStmt:
			recordAssignType(v, out)
		}

		return true
	})

	return out
}

func recordValueSpecType(v *ast.ValueSpec, out map[string]ast.Expr) {
	if v.Type == nil {
		return
	}

	for _, name := range v.Names {
		out[name.Name] = v.Type
	}
}

func recordAssignType(v *ast.AssignStmt, out map[string]ast.Expr) {
	if len(v.Lhs) != 1 || len(v.Rhs) != 1 {
		return
	}

	id, ok := v.Lhs[0].(*ast.Ident)
	if !ok {
		return
	}

	if t := literalType(v.Rhs[0]); t != nil {
		out[id.Name] = t
	}
}

func literalType(e ast.Expr) ast.Expr {
	switch v := e.(type) {
	case *ast.CompositeLit:
		return v.Type
	case *ast.UnaryExpr:
		return literalType(v.X)
	case *ast.CallExpr:
		if id, ok := v.Fun.(*ast.Ident); ok && (id.Name == "make" || id.Name == "new") && len(v.Args) > 0 {
			return v.Args[0]
		}
	}

	return nil
}

// gatedOps returns the op names a bool gate function, called with one of
// fd's string parameters, accepts (isIdempotentOp's shape).
func (sc rawKeyScan) gatedOps(fd *ast.FuncDecl, byName map[string]*ast.FuncDecl) []string {
	strParams := map[string]bool{}
	for _, p := range namedParamsMatching(fd.Type.Params, isStringType) {
		strParams[p.name] = true
	}

	var out []string

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok || !argsMention(call.Args, strParams) {
			return true
		}

		gate, known := byName[callName(call.Fun)]
		if !known || gate == fd {
			return true
		}

		gateParams := map[string]bool{}
		for _, p := range namedParamsMatching(gate.Type.Params, isStringType) {
			gateParams[p.name] = true
		}

		out = append(out, sc.switchCaseNames(gate.Body, gateParams)...)

		return true
	})

	return out
}

func (sc rawKeyScan) switchCaseNames(body *ast.BlockStmt, tagNames map[string]bool) []string {
	var out []string

	ast.Inspect(body, func(n ast.Node) bool {
		sw, ok := n.(*ast.SwitchStmt)
		if !ok {
			return true
		}

		tag, isIdent := sw.Tag.(*ast.Ident)
		if !isIdent || !tagNames[tag.Name] {
			return true
		}

		for _, s := range sw.Body.List {
			cc, isCase := s.(*ast.CaseClause)
			if !isCase || !returnsTrue(cc.Body) {
				continue
			}

			for _, e := range cc.List {
				if name, lit := resolveStringExpr(e, sc.consts); lit {
					out = append(out, name)
				}
			}
		}

		return true
	})

	return out
}

func returnsTrue(stmts []ast.Stmt) bool {
	if len(stmts) == 0 {
		return false
	}

	ret, ok := stmts[0].(*ast.ReturnStmt)
	if !ok || len(ret.Results) != 1 {
		return false
	}

	id, isIdent := ret.Results[0].(*ast.Ident)

	return isIdent && id.Name == "true"
}

// creditWrapperKeys declares each wrapper-read key that names one of op's
// own SDK fields.
func creditWrapperKeys(op sdkOp, ctx handlerResolveCtx, res *opResolution) {
	for _, k := range ctx.wrapperKeys[op.Name] {
		norm := normalizeWireName(k)

		for _, f := range op.Fields {
			if normalizeWireName(f.Name) == norm {
				res.Fields[norm] = emuField{WireName: k, GoName: k}
			}
		}
	}
}
