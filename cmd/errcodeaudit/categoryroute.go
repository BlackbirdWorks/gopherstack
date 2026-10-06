package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// categoryRouting records, per awserr category sentinel, whether its errors.Is guard
// emits its own literal code or reads the code from the error (ecs).
type categoryRouting struct {
	literal map[string]bool
	dynamic map[string]bool
	outputs []candidate
}

// collectSentinelCategories maps each awserr.New("Lit", awserr.ErrX) message
// literal position to its category name.
func collectSentinelCategories(files []*ast.File) map[token.Pos]string {
	out := map[token.Pos]string{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || len(call.Args) < 2 {
				return true
			}

			pos, isSentinel := sentinelCallLiteralPos(call)
			if cat, isCat := awserrCategory(call.Args[1]); isSentinel && isCat {
				out[pos] = cat
			}

			return true
		})
	}

	return out
}

func awserrCategory(expr ast.Expr) (string, bool) {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return "", false
	}

	pkg, ok := sel.X.(*ast.Ident)

	return sel.Sel.Name, ok && pkg.Name == pkgAwserr
}

func categoriesTested(exprs []ast.Expr) []string {
	var cats []string

	for _, e := range exprs {
		ast.Inspect(e, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok || !isErrorsIsCall(call) || len(call.Args) < 2 {
				return true
			}

			if cat, isCat := awserrCategory(call.Args[1]); isCat {
				cats = append(cats, cat)
			}

			return true
		})
	}

	return cats
}

func bodyCodeLiterals(body []ast.Stmt, fset *token.FileSet, repoRoot string) []candidate {
	var out []candidate

	for _, st := range body {
		ast.Inspect(st, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			for _, arg := range call.Args {
				lit, isLit := arg.(*ast.BasicLit)
				if !isLit || lit.Kind != token.STRING {
					continue
				}

				if v, err := strconv.Unquote(lit.Value); err == nil && looksLikeCode(v) {
					out = append(out, newCandidate(fset, repoRoot, lit.Pos(), v, mechMapperOutput, false))
				}
			}

			return true
		})
	}

	return out
}

// scanCategoryRouting finds `case errors.Is(err, awserr.ErrX):` and
// `if errors.Is(err, awserr.ErrX)` guards and classifies their bodies.
func scanCategoryRouting(files []*ast.File, fset *token.FileSet, repoRoot string) categoryRouting {
	cr := categoryRouting{literal: map[string]bool{}, dynamic: map[string]bool{}}

	record := func(cond []ast.Expr, body []ast.Stmt) {
		cats := categoriesTested(cond)
		if len(cats) == 0 {
			return
		}

		lits := bodyCodeLiterals(body, fset, repoRoot)
		for _, cat := range cats {
			if len(lits) > 0 {
				cr.literal[cat] = true
			} else {
				cr.dynamic[cat] = true
			}
		}

		cr.outputs = append(cr.outputs, lits...)
	}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.CaseClause:
				record(v.List, v.Body)
			case *ast.IfStmt:
				record([]ast.Expr{v.Cond}, v.Body.List)
			}

			return true
		})
	}

	return cr
}

// applyCategoryRouting demotes awserr.New sentinels whose category guard
// emits its own literal (emr): the sentinel text never reaches the wire.
func applyCategoryRouting(
	files []*ast.File, fset *token.FileSet, repoRoot string, cands []candidate,
) []candidate {
	cats := collectSentinelCategories(files)
	if len(cats) == 0 {
		return nil
	}

	cr := scanCategoryRouting(files, fset, repoRoot)

	for i := range cands {
		c := &cands[i]
		cat, ok := cats[c.pos]

		if c.Mechanism != mechAwserrNew || !ok || c.MapperReason != "" || !cr.literal[cat] || cr.dynamic[cat] {
			continue
		}

		c.MapperReason = "sentinel's text is never written: its awserr." + cat +
			" category is matched by a guard that emits its own literal code -- check that output instead"
	}

	return cr.outputs
}
