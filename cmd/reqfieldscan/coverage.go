package main

import (
	"go/ast"
	"go/token"
)

// coverageKey identifies a field by (struct TYPE, field name) -- never a
// bare field name, so two structs that happen to share a field name never
// collide.
type coverageKey struct {
	Type  string
	Field string
}

type coverageInfo struct {
	File          string
	Line          int
	Read          bool
	ViaConversion bool
}

// collectFieldCoverage walks every function in the package independently,
// binding parameters and simple locals to known request struct types, then
// marks every (type, field) selector actually read. See the package doc
// for the exact binding rules and their disclosed limits.
func collectFieldCoverage(
	files []*ast.File,
	fset *token.FileSet,
	structs map[string]structDef,
) map[coverageKey]coverageInfo {
	cov := map[coverageKey]coverageInfo{}
	for typeName, def := range structs {
		for _, fld := range def.Fields {
			cov[coverageKey{typeName, fld.Name}] = coverageInfo{}
		}
	}

	declaredTypes := collectAllTypeNames(files)

	for _, f := range files {
		for _, decl := range f.Decls {
			fd, ok := decl.(*ast.FuncDecl)
			if !ok || fd.Body == nil {
				continue
			}

			bindings := collectLocalBindings(fd, structs)
			walkFuncForFieldReads(fd, fset, bindings, structs, declaredTypes, cov)
		}
	}

	return cov
}

func collectAllTypeNames(files []*ast.File) map[string]bool {
	out := map[string]bool{}

	for _, f := range files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.TYPE {
				continue
			}

			for _, spec := range gd.Specs {
				if ts, tsOK := spec.(*ast.TypeSpec); tsOK {
					out[ts.Name.Name] = true
				}
			}
		}
	}

	return out
}

// collectLocalBindings maps an identifier to a known request struct type
// name for one function: its parameters (by pointer or by value), and any
// `:=`/`=`-bound local resolved via rhsBoundType. Traversal order matches
// source order for straight-line code (ast.Inspect visits each statement's
// full subtree before its next sibling), so a binding is visible to every
// use that follows it -- the same single-assignment-style discipline
// cmd/enumcheck uses for its own local constant resolution.
func collectLocalBindings(fd *ast.FuncDecl, structs map[string]structDef) map[string]string {
	bindings := map[string]string{}

	if fd.Type.Params != nil {
		for _, p := range fd.Type.Params.List {
			typeName := underlyingIdentType(p.Type)
			if _, known := structs[typeName]; !known {
				continue
			}

			for _, n := range p.Names {
				bindings[n.Name] = typeName
			}
		}
	}

	if fd.Body == nil {
		return bindings
	}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.DeclStmt:
			recordVarDeclBindings(v, structs, bindings)
		case *ast.AssignStmt:
			recordAssignBindings(v, structs, bindings)
		}

		return true
	})

	return bindings
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

func recordVarDeclBindings(ds *ast.DeclStmt, structs map[string]structDef, bindings map[string]string) {
	gd, declOK := ds.Decl.(*ast.GenDecl)
	if !declOK || gd.Tok != token.VAR {
		return
	}

	for _, spec := range gd.Specs {
		vs, specOK := spec.(*ast.ValueSpec)
		if !specOK || vs.Type == nil {
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
// `T{...}`, `&T{...}`, or a single-hop alias of an already-bound
// identifier (`x := in`, `x := *in`).
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

func walkFuncForFieldReads(
	fd *ast.FuncDecl, fset *token.FileSet, bindings map[string]string,
	structs map[string]structDef, declaredTypes map[string]bool, cov map[coverageKey]coverageInfo,
) {
	ast.Inspect(fd.Body, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.SelectorExpr:
			markSelectorRead(v, fset, bindings, structs, cov)
		case *ast.CallExpr:
			markWholeStructConversion(v, fset, bindings, structs, declaredTypes, cov)
		}

		return true
	})
}

func markSelectorRead(
	sel *ast.SelectorExpr, fset *token.FileSet, bindings map[string]string,
	structs map[string]structDef, cov map[coverageKey]coverageInfo,
) {
	id, ok := unwrapExpr(sel.X).(*ast.Ident)
	if !ok {
		return
	}

	typeName, ok := bindings[id.Name]
	if !ok {
		return
	}

	def, ok := structs[typeName]
	if !ok || !hasField(def, sel.Sel.Name) {
		return
	}

	markCovered(cov, coverageKey{typeName, sel.Sel.Name}, fset.Position(sel.Pos()), false)
}

// markWholeStructConversion handles `SomeType(req)` / `SomeType(*req)`, a
// Go type conversion of the entire request value -- this repo's other
// common way of using every field at once with no per-field selector
// anywhere. See the package doc for why this suppression exists and its
// own disclosed limit.
func markWholeStructConversion(
	call *ast.CallExpr, fset *token.FileSet, bindings map[string]string,
	structs map[string]structDef, declaredTypes map[string]bool, cov map[coverageKey]coverageInfo,
) {
	if len(call.Args) != 1 {
		return
	}

	var targetName string

	switch fn := call.Fun.(type) {
	case *ast.Ident:
		targetName = fn.Name
	case *ast.SelectorExpr:
		targetName = fn.Sel.Name
	default:
		return
	}

	if !declaredTypes[targetName] {
		return
	}

	id, ok := unwrapExpr(call.Args[0]).(*ast.Ident)
	if !ok {
		return
	}

	typeName, ok := bindings[id.Name]
	if !ok {
		return
	}

	def, ok := structs[typeName]
	if !ok {
		return
	}

	pos := fset.Position(call.Pos())
	for _, fld := range def.Fields {
		markCovered(cov, coverageKey{typeName, fld.Name}, pos, true)
	}
}

func markCovered(cov map[coverageKey]coverageInfo, key coverageKey, pos token.Position, viaConversion bool) {
	info := cov[key]
	if info.Read {
		return
	}

	cov[key] = coverageInfo{Read: true, ViaConversion: viaConversion, File: pos.Filename, Line: pos.Line}
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

func hasField(def structDef, name string) bool {
	for _, f := range def.Fields {
		if f.Name == name {
			return true
		}
	}

	return false
}
