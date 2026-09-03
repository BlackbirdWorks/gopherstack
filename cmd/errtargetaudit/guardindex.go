package main

import (
	"go/ast"
	"go/token"
	"strconv"
)

// guard is the condition gating one code-shaped literal inside a mapper's
// switch/if: the set of sentinel identities (bare or package-qualified,
// errors.Is-style) OR message substrings (strings.Contains(err.Error(), ...)
// -style) that must be reachable from an operation's own backend calls for
// that literal's code to be a real finding for that operation. Either set
// being non-empty is sufficient (case-list/`||` alternatives); both being
// empty is impossible for a value stored in guardsByPos (see caseGuard).
type guard struct {
	IdentityKeys []string
	MessageKeys  []string
}

// identKey renders expr as a guard/reachability key when it is a bare
// identifier ("ErrNotFound") or a package-qualified selector
// ("awserr.ErrNotFound") -- the two shapes this repo's errors.Is comparison
// argument and sentinel-wrapping base argument both take. sentinels
// restricts bare-identifier acceptance to this package's own known sentinel
// vars (idx.Sentinels), matching the rest of this tool's discipline; a
// qualified selector is always accepted since an imported package's own
// sentinel (pkgs/awserr's ErrNotFound/ErrAlreadyExists/...) is never
// package-local and so never appears in that set.
func identKey(expr ast.Expr, sentinels map[string]bool) (string, bool) {
	switch e := expr.(type) {
	case *ast.Ident:
		if sentinels[e.Name] {
			return e.Name, true
		}
	case *ast.SelectorExpr:
		if pkgIdent, ok := e.X.(*ast.Ident); ok {
			return pkgIdent.Name + "." + e.Sel.Name, true
		}
	}

	return "", false
}

// buildGuardIndex scans every function/method body in the package for a
// switch or if statement whose case/condition is a recognised guard
// (errors.Is against a sentinel, or strings.Contains(_, "CodeLiteral")),
// and records the position of every code-shaped string literal inside that
// guarded branch against the guard that reaches it. This is deliberately
// STRUCTURAL, independent of funcSentinelCodes' bare-identifier-only mapper
// detection: a switch that only ever compares a PACKAGE-QUALIFIED sentinel
// (pkgs/awserr's `errors.Is(err, awserr.ErrNotFound)`, this repo's own
// shared-sentinel package) never populates a single entry in
// funcSentinelCodes/ByFunc, so a fix scoped to that table alone would miss
// exactly the shape gopherstack-axs3 measured in services/bedrockagent.
//
// The second return is every function/method name that contributed at
// least one guarded literal -- reachability.go's computeReachSet must never
// recurse INTO one of these when looking for a backend method's own
// returned sentinel: a mapper function is reached by construction whenever
// there is a candidate emission to filter at all (that is how the literal
// was found in the first place), so counting it as "a backend call this
// scan read" would make reachSet.Determined true almost unconditionally --
// exactly backwards from what it exists to gate.
func buildGuardIndex(idx *pkgIndex) (map[token.Pos]guard, map[string]bool) {
	out := map[token.Pos]guard{}
	mapperNames := map[string]bool{}

	visit := func(name string, body *ast.BlockStmt) {
		if body == nil {
			return
		}

		before := len(out)

		ast.Inspect(body, func(n ast.Node) bool {
			switch v := n.(type) {
			case *ast.SwitchStmt:
				addSwitchGuards(v, idx, out)
			case *ast.IfStmt:
				addIfGuards(v, idx, out)
			}

			return true
		})

		if len(out) > before {
			mapperNames[name] = true
		}
	}

	for name, fd := range idx.Funcs {
		visit(name, fd.Body)
	}

	for name, fds := range idx.Methods {
		for _, fd := range fds {
			visit(name, fd.Body)
		}
	}

	return out, mapperNames
}

func addSwitchGuards(sw *ast.SwitchStmt, idx *pkgIndex, out map[token.Pos]guard) {
	if sw.Body == nil {
		return
	}

	for _, stmt := range sw.Body.List {
		cc, ok := stmt.(*ast.CaseClause)
		if !ok || len(cc.List) == 0 {
			continue // default clause: no guard, literals inside stay unguarded
		}

		g, ok := caseGuard(cc.List, idx.Sentinels)
		if !ok {
			continue
		}

		indexCaseLiterals(&ast.BlockStmt{List: cc.Body}, g, out)
	}
}

func addIfGuards(ifs *ast.IfStmt, idx *pkgIndex, out map[token.Pos]guard) {
	if ifs.Body == nil {
		return
	}

	g, ok := caseGuard([]ast.Expr{ifs.Cond}, idx.Sentinels)
	if !ok {
		return
	}

	indexCaseLiterals(ifs.Body, g, out)
}

// caseGuard extracts a guard from a case-list (OR'd alternatives) or a
// single if-condition. It refuses (returns false) the moment it sees ANY
// comparison it does not recognise (errors.As, or anything else) mixed in:
// a case reachable through an alternative this scan cannot model must never
// be suppressed as if the modeled alternative were the only way in --
// partial understanding here is more dangerous than none, matching this
// package's "silent miss over false finding" discipline elsewhere.
func caseGuard(exprs []ast.Expr, sentinels map[string]bool) (guard, bool) {
	var g guard

	unrecognized := false

	for _, e := range exprs {
		ast.Inspect(e, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}

			return inspectGuardCall(call, sentinels, &g, &unrecognized)
		})
	}

	if unrecognized || (len(g.IdentityKeys) == 0 && len(g.MessageKeys) == 0) {
		return guard{}, false
	}

	return g, true
}

func inspectGuardCall(
	call *ast.CallExpr,
	sentinels map[string]bool,
	g *guard,
	unrecognized *bool,
) bool {
	sel, ok := call.Fun.(*ast.SelectorExpr)
	if !ok {
		return true
	}

	pkgIdent, ok := sel.X.(*ast.Ident)
	if !ok {
		return true
	}

	switch {
	case pkgIdent.Name == pkgErrors && sel.Sel.Name == "Is" && len(call.Args) == 2:
		if key, keyOK := identKey(call.Args[1], sentinels); keyOK {
			g.IdentityKeys = append(g.IdentityKeys, key)
		} else {
			*unrecognized = true
		}

		return false
	case pkgIdent.Name == pkgErrors && sel.Sel.Name == "As":
		*unrecognized = true

		return false
	case pkgIdent.Name == "strings" && sel.Sel.Name == "Contains" && len(call.Args) == 2:
		if key, keyOK := stringLiteralArg(call.Args[1]); keyOK && looksLikeCode(key) {
			g.MessageKeys = append(g.MessageKeys, key)
		} else {
			*unrecognized = true
		}

		return false
	default:
		return true
	}
}

func stringLiteralArg(expr ast.Expr) (string, bool) {
	lit, ok := expr.(*ast.BasicLit)
	if !ok || lit.Kind != token.STRING {
		return "", false
	}

	v, err := strconv.Unquote(lit.Value)
	if err != nil {
		return "", false
	}

	return v, true
}

// indexCaseLiterals records every code-shaped string literal found anywhere
// inside body (an assignment's RHS, a const/var decl's value, a composite
// literal field, a direct call argument -- every shape emit.go's literal
// mechanisms read) against g, uniformly and regardless of which of those
// shapes produced it.
func indexCaseLiterals(body *ast.BlockStmt, g guard, out map[token.Pos]guard) {
	ast.Inspect(body, func(n ast.Node) bool {
		lit, ok := n.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}

		v, err := strconv.Unquote(lit.Value)
		if err != nil || !looksLikeCode(v) {
			return true
		}

		out[lit.Pos()] = g

		return true
	})
}
