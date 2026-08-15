package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// collectConsts records, for every top-level const declaration in f:
//   - plain string literals into consts (identifier -> value)
//   - plain int literals into intConsts (identifier -> value)
//   - a bare "service.PriorityXxx" selector into selectorConsts
//     (identifier -> "PriorityXxx")
//   - "service.PriorityXxx + N" into selectorConsts as "PriorityXxx+N" (the
//     one deliberate priority-bump precedent in this repo: kafka bumps its
//     own priority above PriorityPathVersioned specifically to beat
//     AppSync's tied /v1/tags claim; resolving it avoids a false collision
//     report for whatever it also happens to outrank as a side effect)
func collectConsts(f *ast.File, consts, selectorConsts map[string]string, intConsts map[string]int) {
	for _, decl := range f.Decls {
		gd, ok := decl.(*ast.GenDecl)
		if !ok || gd.Tok != token.CONST {
			continue
		}

		for _, spec := range gd.Specs {
			collectConstSpec(spec, consts, selectorConsts, intConsts)
		}
	}
}

func collectConstSpec(spec ast.Spec, consts, selectorConsts map[string]string, intConsts map[string]int) {
	vs, ok := spec.(*ast.ValueSpec)
	if !ok || len(vs.Names) != len(vs.Values) {
		return
	}

	for i, name := range vs.Names {
		collectConstValue(name.Name, vs.Values[i], consts, selectorConsts, intConsts)
	}
}

func collectConstValue(
	name string,
	value ast.Expr,
	consts, selectorConsts map[string]string,
	intConsts map[string]int,
) {
	switch v := value.(type) {
	case *ast.BasicLit:
		collectBasicLitConst(name, v, consts, intConsts)
	case *ast.SelectorExpr:
		collectSelectorConst(name, v, selectorConsts)
	case *ast.BinaryExpr:
		collectPriorityBumpConst(name, v, selectorConsts)
	}
}

func collectBasicLitConst(name string, v *ast.BasicLit, consts map[string]string, intConsts map[string]int) {
	switch v.Kind {
	case token.STRING:
		if unquoted, err := strconv.Unquote(v.Value); err == nil {
			consts[name] = unquoted
		}
	case token.INT:
		if n, err := strconv.Atoi(v.Value); err == nil {
			intConsts[name] = n
		}
	default:
		// Float/char/imaginary consts are never path literals or match
		// priorities in this codebase; nothing to record.
	}
}

func collectSelectorConst(name string, v *ast.SelectorExpr, selectorConsts map[string]string) {
	pkg, isServicePkg := v.X.(*ast.Ident)
	if isServicePkg && pkg.Name == "service" {
		selectorConsts[name] = v.Sel.Name
	}
}

func collectPriorityBumpConst(name string, v *ast.BinaryExpr, selectorConsts map[string]string) {
	if v.Op != token.ADD {
		return
	}

	sel, isSelector := v.X.(*ast.SelectorExpr)
	if !isSelector {
		return
	}

	pkg, isServicePkg := sel.X.(*ast.Ident)
	if !isServicePkg || pkg.Name != "service" {
		return
	}

	lit, isIntLit := v.Y.(*ast.BasicLit)
	if !isIntLit || lit.Kind != token.INT {
		return
	}

	if n, err := strconv.Atoi(lit.Value); err == nil {
		selectorConsts[name] = sel.Sel.Name + "+" + strconv.Itoa(n)
	}
}
