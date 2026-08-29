package main

import (
	"go/ast"
	"go/token"
	"strconv"
	"strings"
)

// routingFallbackDispatchNames are the identifier names this repo's own
// dispatchers consistently route on: route53's own "method" (routeRRSet,
// routeHostedZone, routeHealthCheck, ... all switch/if on the HTTP verb)
// and quicksight's own "op" (dispatch, dispatchNamespace,
// dispatchAccountConfig, ... all switch/map-lookup on the classified
// operation name), plus "path"/"action" for the same pattern under other
// services' own naming.
var routingFallbackDispatchNames = map[string]bool{ //nolint:gochecknoglobals // read-only lookup table
	"op":     true,
	"method": true,
	"path":   true,
	"action": true,
}

// applyRoutingFallbackDetection marks every candidate in cands whose own
// emission call sits in a structural ROUTING FALLBACK position: reached
// only when a dispatcher's switch/if/map-lookup chain matched no known
// operation, HTTP method, or path at all -- never from inside a handler a
// dispatcher already selected for a specific operation. quicksight's
// UnsupportedOperationException (dispatch()'s own default case, and its
// eleven cousins: dispatchNamespace, dispatchAccountConfig,
// dispatchResourceSearch, ...) and route53's NoSuchOperation (every
// routeXxx's own `switch method { ...; default: ... }` /
// `if method == http.MethodX {...}; return xmlError(...)`) are both this
// shape -- confirmed live by reading all 67 call sites this detector
// matches in the current tree. There is no operation to consult here, the
// same reasoning services/codedeploy's dispatch-level unknown-action error
// was deliberately left unfixed under for the same reason (5e0b4978a): a
// per-op deserializer has nothing to check a no-op-matched fallback
// against, because dispatch never reached an op.
//
// It mutates cands in place, setting RoutingFallback; scan.go's classify
// drops a RoutingFallback candidate the same way it drops a
// genericProtocolCodes hit -- there is nothing to review, and the same
// reasoning applies wherever this exact structural shape recurs, not just
// in these two services.
func applyRoutingFallbackDetection(files []*ast.File, cands []candidate) {
	positions := routingFallbackPositions(files)
	if len(positions) == 0 {
		return
	}

	for i := range cands {
		if positions[cands[i].pos] {
			cands[i].RoutingFallback = true
		}
	}
}

func routingFallbackPositions(files []*ast.File) map[token.Pos]bool {
	out := map[token.Pos]bool{}

	for _, f := range files {
		ast.Inspect(f, func(n ast.Node) bool {
			if block, ok := n.(*ast.BlockStmt); ok {
				markBlockFallback(block.List, out)
			}

			return true
		})
	}

	return out
}

// markBlockFallback scans one flat statement list for the guard-chain
// shape: zero or more leading GUARDS -- an `if` with no else whose body
// always returns, or a `switch` with no default whose every case always
// returns -- each one gated on a routingFallbackDispatchNames identifier,
// followed immediately by an unconditional `return <expr>` reached only
// when none of the guards matched. It also recognizes the self-contained
// case of a single `switch` WITH a default clause whose own tag/cases
// carry the gated identifier: the default clause IS the fallback, no
// trailing statement required (quicksight's dispatch()).
func markBlockFallback(stmts []ast.Stmt, out map[token.Pos]bool) {
	var guards []map[string]bool

	for _, stmt := range stmts {
		switch s := stmt.(type) {
		case *ast.SwitchStmt:
			guards = markSwitchStmt(s, guards, out)
		case *ast.IfStmt:
			if s.Else == nil && bodyAlwaysReturns(s.Body) {
				guards = append(guards, ifGuardIdents(s))
			} else {
				guards = nil
			}
		case *ast.ReturnStmt:
			if allGuardsSatisfyGate(guards) {
				collectCodeLiteralPositions(s, out)
			}

			guards = nil
		default:
			guards = nil
		}
	}
}

// markSwitchStmt folds one *ast.SwitchStmt into the running guard chain.
// A switch carrying its own default clause is self-contained: combined
// with any guards already accumulated ahead of it, it either fires the
// default clause as a fallback right here (gate satisfied) or resets the
// chain -- either way nothing about it carries forward. A switch with no
// default, whose every case body always returns, is itself one more guard
// -- exactly route53's `switch method { case ...: return ...; case ...:
// return ... }` with no default, falling through to a trailing
// `return xmlError(...)`.
func markSwitchStmt(s *ast.SwitchStmt, guards []map[string]bool, out map[token.Pos]bool) []map[string]bool {
	ids := switchIdents(s)

	if def, hasOtherCases := switchDefaultBody(s); def != nil {
		if hasOtherCases && allGuardsSatisfyGate(append(append([]map[string]bool{}, guards...), ids)) {
			collectCodeLiteralPositions(def, out)
		}

		return nil
	}

	if allCaseBodiesReturn(s) {
		return append(guards, ids)
	}

	return nil
}

// switchDefaultBody returns the switch's own default *ast.CaseClause (nil
// if it has none) and whether the switch also carries at least one
// non-default case -- a switch that is nothing but a bare default is not
// genuine dispatch.
func switchDefaultBody(s *ast.SwitchStmt) (*ast.CaseClause, bool) {
	var def *ast.CaseClause

	other := false

	for _, c := range s.Body.List {
		cc, ok := c.(*ast.CaseClause)
		if !ok {
			continue
		}

		if cc.List == nil {
			def = cc
		} else {
			other = true
		}
	}

	return def, other
}

func allCaseBodiesReturn(s *ast.SwitchStmt) bool {
	if len(s.Body.List) == 0 {
		return false
	}

	for _, c := range s.Body.List {
		cc, ok := c.(*ast.CaseClause)
		if !ok || cc.List == nil {
			return false
		}

		if !bodyAlwaysReturns(&ast.BlockStmt{List: cc.Body}) {
			return false
		}
	}

	return true
}

func bodyAlwaysReturns(block *ast.BlockStmt) bool {
	if block == nil || len(block.List) == 0 {
		return false
	}

	_, isReturn := block.List[len(block.List)-1].(*ast.ReturnStmt)

	return isReturn
}

// switchIdents reports the identifier(s) this switch dispatches on: its
// own Tag when it has one (route53's `switch method`), or every
// identifier referenced across its non-default cases' own expressions
// when it doesn't (quicksight's bare `switch { case isNamespaceOp(op):
// ...; case op != opUnknown: ...; default: ... }`, where "op" is what
// every case actually shares).
func switchIdents(s *ast.SwitchStmt) map[string]bool {
	out := map[string]bool{}

	if s.Tag != nil {
		if id, ok := s.Tag.(*ast.Ident); ok {
			out[id.Name] = true

			return out
		}
	}

	for _, c := range s.Body.List {
		cc, ok := c.(*ast.CaseClause)
		if !ok || cc.List == nil {
			continue
		}

		for _, expr := range cc.List {
			collectIdentNames(expr, out)
		}
	}

	return out
}

func ifGuardIdents(s *ast.IfStmt) map[string]bool {
	out := map[string]bool{}

	if s.Init != nil {
		collectIdentNames(s.Init, out)
	}

	collectIdentNames(s.Cond, out)

	return out
}

func collectIdentNames(n ast.Node, out map[string]bool) {
	ast.Inspect(n, func(x ast.Node) bool {
		if id, ok := x.(*ast.Ident); ok {
			out[id.Name] = true
		}

		return true
	})
}

// allGuardsSatisfyGate requires every guard leading up to a candidate
// fallback statement to individually reference a
// routingFallbackDispatchNames identifier -- an empty chain (a bare
// literal return with no preceding guard at all) never qualifies, since
// that is not a fallback, just an unconditional emission.
func allGuardsSatisfyGate(guards []map[string]bool) bool {
	if len(guards) == 0 {
		return false
	}

	for _, g := range guards {
		if !identSetHasGateName(g) {
			return false
		}
	}

	return true
}

func identSetHasGateName(ids map[string]bool) bool {
	for name := range ids {
		if routingFallbackDispatchNames[strings.ToLower(name)] {
			return true
		}
	}

	return false
}

func collectCodeLiteralPositions(n ast.Node, out map[token.Pos]bool) {
	ast.Inspect(n, func(x ast.Node) bool {
		lit, ok := x.(*ast.BasicLit)
		if !ok || lit.Kind != token.STRING {
			return true
		}

		if v, err := strconv.Unquote(lit.Value); err == nil && looksLikeCode(v) {
			out[lit.Pos()] = true
		}

		return true
	})
}
