package main

import (
	"go/ast"
	"go/token"
)

// funcLike is the common shape cmd/reqfielddiff needs from either a
// *ast.FuncDecl (a resolved handler method or package func) or an
// *ast.FuncLit (a dispatch-table closure, or a func literal argument), so
// binding collection and body scanning share one code path for both.
type funcLike struct {
	Recv   *ast.FieldList
	Params *ast.FieldList
	Body   *ast.BlockStmt
}

func fromFuncDecl(fd *ast.FuncDecl) funcLike {
	fl := funcLike{Recv: fd.Recv, Body: fd.Body}
	if fd.Type != nil {
		fl.Params = fd.Type.Params
	}

	return fl
}

func fromFuncLit(lit *ast.FuncLit) funcLike {
	fl := funcLike{Body: lit.Body}
	if lit.Type != nil {
		fl.Params = lit.Type.Params
	}

	return fl
}

// collectLocalBindings maps an identifier to a known struct type name for
// one function body: its receiver and parameters (by pointer or by value),
// and any `:=`/`=`-bound local resolved via rhsBoundType. Adapted from
// cmd/reqfieldscan's coverage.go collectLocalBindings -- duplicated rather
// than imported, see structs.go's doc for why.
func collectLocalBindings(fl funcLike, fset *token.FileSet, structs map[string]structDef) map[string]string {
	bindings := map[string]string{}

	bindFieldList(fl.Recv, structs, bindings)
	bindFieldList(fl.Params, structs, bindings)

	if fl.Body == nil {
		return bindings
	}

	ast.Inspect(fl.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.DeclStmt:
			recordVarDeclBindings(v, fset, structs, bindings)
		case *ast.AssignStmt:
			recordAssignBindings(v, structs, bindings)
		}

		return true
	})

	return bindings
}

func bindFieldList(flist *ast.FieldList, structs map[string]structDef, bindings map[string]string) {
	if flist == nil {
		return
	}

	for _, field := range flist.List {
		typeName := underlyingIdentType(field.Type)
		if _, known := structs[typeName]; !known {
			continue
		}

		for _, n := range field.Names {
			bindings[n.Name] = typeName
		}
	}
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

func recordVarDeclBindings(
	ds *ast.DeclStmt,
	fset *token.FileSet,
	structs map[string]structDef,
	bindings map[string]string,
) {
	gd, declOK := ds.Decl.(*ast.GenDecl)
	if !declOK || gd.Tok != token.VAR {
		return
	}

	for _, spec := range gd.Specs {
		vs, specOK := spec.(*ast.ValueSpec)
		if !specOK || vs.Type == nil {
			continue
		}

		if _, isAnon := vs.Type.(*ast.StructType); isAnon && len(vs.Names) == 1 {
			bindings[vs.Names[0].Name] = anonStructName(fset, vs)

			continue
		}

		typeName := underlyingIdentType(vs.Type)
		if _, known := structs[typeName]; !known {
			continue
		}

		for _, nm := range vs.Names {
			bindings[nm.Name] = typeName
		}
	}
}

func recordAssignBindings(as *ast.AssignStmt, structs map[string]structDef, bindings map[string]string) {
	if as.Tok != token.DEFINE && as.Tok != token.ASSIGN {
		return
	}

	for i, lhs := range as.Lhs {
		id, ok := lhs.(*ast.Ident)
		if !ok || i >= len(as.Rhs) {
			continue
		}

		if typeName, resolved := rhsBoundType(as.Rhs[i], structs, bindings); resolved {
			bindings[id.Name] = typeName
		}
	}
}

// rhsBoundType resolves the RHS of an assignment to a known struct type:
// `T{...}`, `&T{...}`, or a single-hop alias of an already-bound identifier
// (`x := in`, `x := *in`).
func rhsBoundType(expr ast.Expr, structs map[string]structDef, bindings map[string]string) (string, bool) {
	switch e := expr.(type) {
	case *ast.CompositeLit:
		if id, ok := e.Type.(*ast.Ident); ok {
			if _, known := structs[id.Name]; known {
				return id.Name, true
			}
		}
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			return rhsBoundType(e.X, structs, bindings)
		}
	case *ast.StarExpr:
		if id, ok := e.X.(*ast.Ident); ok {
			if t, bound := bindings[id.Name]; bound {
				return t, true
			}
		}
	case *ast.Ident:
		if t, ok := bindings[e.Name]; ok {
			return t, true
		}
	}

	return "", false
}

func unwrapExpr(e ast.Expr) ast.Expr {
	for {
		switch v := e.(type) {
		case *ast.ParenExpr:
			e = v.X
		case *ast.StarExpr:
			e = v.X
		default:
			return e
		}
	}
}
