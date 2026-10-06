package main

import (
	"go/ast"
	"go/token"
)

// routeStruct names the func-typed field and string fields of a local struct type.
type routeStruct struct {
	bindField string
	strFields []string
}

func collectRouteStructs(files []*ast.File, funcTypeNames map[string]bool) map[string]routeStruct {
	out := map[string]routeStruct{}

	for _, f := range files {
		for _, decl := range f.Decls {
			if gd, ok := decl.(*ast.GenDecl); ok && gd.Tok == token.TYPE {
				addRouteStructs(gd, funcTypeNames, out)
			}
		}
	}

	return out
}

func addRouteStructs(gd *ast.GenDecl, funcTypeNames map[string]bool, out map[string]routeStruct) {
	for _, spec := range gd.Specs {
		ts, ok := spec.(*ast.TypeSpec)
		if !ok {
			continue
		}

		st, isStruct := ts.Type.(*ast.StructType)
		if !isStruct {
			continue
		}

		if rs, found := classifyRouteStruct(st, funcTypeNames); found {
			out[ts.Name.Name] = rs
		}
	}
}

func classifyRouteStruct(st *ast.StructType, funcTypeNames map[string]bool) (routeStruct, bool) {
	var rs routeStruct

	for _, f := range st.Fields.List {
		for _, n := range f.Names {
			switch {
			case isFuncShapedType(f.Type, funcTypeNames):
				rs.bindField = n.Name
			case isStringType(f.Type):
				rs.strFields = append(rs.strFields, n.Name)
			}
		}
	}

	return rs, rs.bindField != "" && len(rs.strFields) > 0
}

func isFuncShapedType(t ast.Expr, funcTypeNames map[string]bool) bool {
	switch v := t.(type) {
	case *ast.FuncType:
		return true
	case *ast.Ident:
		return funcTypeNames[v.Name]
	default:
		return false
	}
}

// collectRouteTableEntries maps op names to handler references in
// `[]route{{op: "X", fn: h.dispatchX}}` tables of a local struct type.
func collectRouteTableEntries(
	files []*ast.File,
	pkgConsts map[string]string,
	funcTypeNames map[string]bool,
	out map[string]ast.Expr,
) {
	structs := collectRouteStructs(files, funcTypeNames)

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			cl, ok := n.(*ast.CompositeLit)
			if !ok {
				return true
			}

			at, isSlice := cl.Type.(*ast.ArrayType)
			if !isSlice {
				return true
			}

			elt, isIdent := at.Elt.(*ast.Ident)
			if !isIdent {
				return true
			}

			rs, known := structs[elt.Name]
			if !known {
				return true
			}

			for _, e := range cl.Elts {
				addRouteElement(e, rs, pkgConsts, out)
			}

			return true
		})
	}
}

func addRouteElement(elt ast.Expr, rs routeStruct, pkgConsts map[string]string, out map[string]ast.Expr) {
	ecl, ok := elt.(*ast.CompositeLit)
	if !ok {
		return
	}

	var bind ast.Expr

	names := map[string]ast.Expr{}

	for _, e := range ecl.Elts {
		kv, isKV := e.(*ast.KeyValueExpr)
		if !isKV {
			continue
		}

		key, isKey := kv.Key.(*ast.Ident)

		switch {
		case !isKey:
		case key.Name == rs.bindField:
			bind = kv.Value
		default:
			names[key.Name] = kv.Value
		}
	}

	if bind == nil {
		return
	}

	for _, field := range rs.strFields {
		name, resolved := resolveStringExpr(names[field], pkgConsts)
		if _, exists := out[name]; resolved && !exists && isOpName(name) {
			out[name] = bind
		}
	}
}

// isOpName reports whether s looks like an AWS operation name.
func isOpName(s string) bool {
	if s == "" || s[0] < 'A' || s[0] > 'Z' {
		return false
	}

	for _, r := range s {
		if (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') {
			return false
		}
	}

	return true
}

// collectSwitchBodyEntries maps every op-named case clause to a synthetic
// closure over its body; resolveOp uses them only when nothing better resolved.
func collectSwitchBodyEntries(
	files []*ast.File,
	pkgConsts map[string]string,
	out map[string][]ast.Expr,
) {
	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			sig := nonScalarSignature(fd.Type)

			ast.Inspect(fd.Body, func(n ast.Node) bool {
				if sw, isSwitch := n.(*ast.SwitchStmt); isSwitch {
					addSwitchBodyEntries(sw, sig, pkgConsts, out)
				}

				return true
			})
		}
	}
}

func addSwitchBodyEntries(
	sw *ast.SwitchStmt,
	sig *ast.FuncType,
	pkgConsts map[string]string,
	out map[string][]ast.Expr,
) {
	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok || len(cc.Body) == 0 {
			continue
		}

		lit := &ast.FuncLit{Type: sig, Body: &ast.BlockStmt{List: cc.Body}}

		for _, caseExpr := range cc.List {
			name, resolved := resolveStringExpr(caseExpr, pkgConsts)
			if !resolved || !isOpName(name) {
				continue
			}

			out[name] = append(out[name], lit)
		}
	}
}

// nonScalarSignature drops scalar parameters so a case body is never
// credited with the dispatcher's own op/path/query argument names.
func nonScalarSignature(ft *ast.FuncType) *ast.FuncType {
	out := &ast.FuncType{Params: &ast.FieldList{}}

	if ft.Params == nil {
		return out
	}

	for _, p := range ft.Params.List {
		if !isScalarParamType(p.Type) {
			out.Params.List = append(out.Params.List, p)
		}
	}

	return out
}
