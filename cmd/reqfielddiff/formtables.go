package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// matchDynamicKeyTable credits wire-name literals in body when it reads
// url.Values with a non-literal key; the table entries are the real keys.
func matchDynamicKeyTable(
	body *ast.BlockStmt,
	urlValuesNames map[string]bool,
	formKeys map[string]string,
	res *opResolution,
) {
	if body == nil || len(urlValuesNames) == 0 || len(formKeys) == 0 || !hasDynamicKeyGet(body, urlValuesNames) {
		return
	}

	ast.Inspect(body, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}

		if s, err := strconv.Unquote(lit.Value); err == nil && isWireKeyShaped(s) {
			matchLiteralString(s, formKeys, res, true)
		}

		return true
	})
}

func hasDynamicKeyGet(body *ast.BlockStmt, urlValuesNames map[string]bool) bool {
	found := false

	ast.Inspect(body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if ok && (isDynamicGet(call, urlValuesNames) || isDynamicKeyHelper(call, urlValuesNames)) {
			found = true
		}

		return !found
	})

	return found
}

func isDynamicGet(call *ast.CallExpr, urlValuesNames map[string]bool) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || sel.Sel.Name != methodGet || len(call.Args) != 1 {
		return false
	}

	recv, ok := sel.X.(*ast.Ident)
	if !ok || !urlValuesNames[recv.Name] {
		return false
	}

	_, isLit := call.Args[0].(*ast.BasicLit)

	return !isLit
}

// isDynamicKeyHelper matches `fn(vals, row.param, ...)`, the table-loop shape.
func isDynamicKeyHelper(call *ast.CallExpr, urlValuesNames map[string]bool) bool {
	passesValues, hasSelector := false, false

	for _, arg := range call.Args {
		switch a := arg.(type) {
		case *ast.Ident:
			passesValues = passesValues || urlValuesNames[a.Name]
		case *ast.SelectorExpr:
			hasSelector = true
		}
	}

	return passesValues && hasSelector
}

func isWireKeyShaped(s string) bool {
	if s == "" {
		return false
	}

	for _, r := range s {
		isAlnum := r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9'
		if !isAlnum && r != '.' {
			return false
		}
	}

	return true
}
