package main

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

const (
	reasonDefaultFallback = "default clause of an errors.Is classifier: reached only for errors no sentinel matched"
	reasonGenericCategory = "generic awserr category case: shadowed by service sentinels, verify reachability"
	reasonRouting         = "routing/unknown-operation fallback: no operation's deserializer applies"
	reasonDefaultInit     = "initial value overridden by errors.Is cases below: unclassified fallback"
)

var routingPhrases = []string{ //nolint:gochecknoglobals // read-only lookup table
	"unknown operation", "unknown action", "unsupported operation", "unsupported action",
	"x-amz-target", "unknown route", "no such operation",
}

var routingIdentParts = []string{ //nolint:gochecknoglobals // read-only lookup table
	"unknownaction", "unknownoperation", "unsupportedoperation", "unsupportedaction",
}

func codeLiteralsIn(n ast.Node, reason string, out map[token.Pos]string) {
	ast.Inspect(n, func(x ast.Node) bool {
		lit, ok := x.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}

		if v, err := strconv.Unquote(lit.Value); err == nil && looksLikeCode(v) {
			if _, set := out[lit.Pos()]; !set {
				out[lit.Pos()] = reason
			}
		}

		return true
	})
}

func caseHasErrorsIs(cc *ast.CaseClause) bool {
	for _, e := range cc.List {
		found := false

		ast.Inspect(e, func(n ast.Node) bool {
			if call, ok := n.(*ast.CallExpr); ok && isErrorsIsCall(call) {
				found = true
			}

			return !found
		})

		if found {
			return true
		}
	}

	return false
}

func isGenericAwserrCase(cc *ast.CaseClause) bool {
	if len(cc.List) == 0 {
		return false
	}

	for _, e := range cc.List {
		call, ok := e.(*ast.CallExpr)
		if !ok || !isErrorsIsCall(call) || len(call.Args) != 2 {
			return false
		}

		sel, isSel := call.Args[1].(*ast.SelectorExpr)
		pkg, isPkg := ast.Expr(nil), false

		if isSel {
			pkg, isPkg = sel.X, true
		}

		id, isID := pkg.(*ast.Ident)
		if !isPkg || !isID || id.Name != pkgAwserr {
			return false
		}
	}

	return true
}

func markSwitchFallbacks(s *ast.SwitchStmt, out map[token.Pos]string) {
	hasIs := false

	for _, stmt := range s.Body.List {
		if cc, ok := stmt.(*ast.CaseClause); ok && caseHasErrorsIs(cc) {
			hasIs = true
		}
	}

	if !hasIs {
		return
	}

	for _, stmt := range s.Body.List {
		cc, ok := stmt.(*ast.CaseClause)

		switch {
		case !ok:
		case cc.List == nil:
			codeLiteralsIn(cc, reasonDefaultFallback, out)
		case isGenericAwserrCase(cc):
			codeLiteralsIn(cc, reasonGenericCategory, out)
		}
	}
}

func isRoutingExpr(e ast.Expr) bool {
	switch x := e.(type) {
	case *ast.Ident:
		lower := strings.ToLower(x.Name)
		for _, part := range routingIdentParts {
			if strings.Contains(lower, part) {
				return true
			}
		}
	case *ast.BasicLit:
		if v, err := strconv.Unquote(x.Value); err == nil && x.Kind == token.STRING {
			lower := strings.ToLower(v)
			for _, phrase := range routingPhrases {
				if strings.Contains(lower, phrase) {
					return true
				}
			}
		}
	case *ast.BinaryExpr:
		return isRoutingExpr(x.X) || isRoutingExpr(x.Y)
	}

	return false
}

func markRoutingRow(exprs []ast.Expr, n ast.Node, out map[token.Pos]string) {
	for _, e := range exprs {
		if kv, ok := e.(*ast.KeyValueExpr); ok {
			e = kv.Value
		}

		if isRoutingExpr(e) {
			codeLiteralsIn(n, reasonRouting, out)

			return
		}
	}
}

func markDefaultInit(fd *ast.FuncDecl, out map[token.Pos]string) {
	if fd.Body == nil || !containsErrorsIsCall(fd.Body) {
		return
	}

	ast.Inspect(fd.Body, func(n ast.Node) bool {
		as, ok := n.(*ast.AssignStmt)
		if !ok || as.Tok != token.DEFINE || len(as.Lhs) != len(as.Rhs) {
			return true
		}

		for i, lhs := range as.Lhs {
			if id, isID := lhs.(*ast.Ident); isID && looksLikeCodeVarName(id.Name) {
				codeLiteralsIn(as.Rhs[i], reasonDefaultInit, out)
			}
		}

		return true
	})
}

// fallbackReasons maps literal positions in unreachable-by-operation
// positions (classifier defaults, generic cases, routing) to a reason.
func fallbackReasons(files []*ast.File) map[token.Pos]string {
	out := map[token.Pos]string{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch x := n.(type) {
			case *ast.SwitchStmt:
				markSwitchFallbacks(x, out)
			case *ast.CallExpr:
				markRoutingRow(x.Args, x, out)
			case *ast.CompositeLit:
				markRoutingRow(x.Elts, x, out)
			case *ast.FuncDecl:
				markDefaultInit(x, out)
			}

			return true
		})
	}

	return out
}

func applyFallbackDemotions(files []*ast.File, cands []candidate) {
	reasons := fallbackReasons(files)

	for i := range cands {
		if cands[i].DemoteReason == "" {
			cands[i].DemoteReason = reasons[cands[i].pos]
		}
	}
}
