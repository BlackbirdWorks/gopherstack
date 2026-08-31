package main

import (
	"go/ast"
	"go/token"
	"maps"
	"slices"
	"sort"
	"strings"
)

// sentinelMeta describes how one package-level sentinel var was built, so a
// backend method that returns it can be traced one hop further: to the
// BASE sentinel it wraps (pkgs/awserr's New/Newf shape: `awserr.New(msg,
// awserr.ErrNotFound)`, which a mapper's errors.Is check compares against
// the base, not the local wrapper) and to the literal MESSAGE text it
// carries (this repo's OTHER real shape, services/account's `errors.New("
// ResourceNotFoundException: ...")`, matched by a mapper via
// strings.Contains(err.Error(), ...) rather than errors.Is at all).
type sentinelMeta struct {
	Message    string
	Base       string
	HasMessage bool
	HasBase    bool
}

// buildSentinelMeta inspects every package-level sentinel var's own
// constructor call for a string-literal message argument and an
// identifier/selector "base" argument -- deliberately permissive about
// which argument is which (first string literal found is the message,
// first identifier/selector found is the base), since over-collecting here
// only ever WIDENS a reachable set (biasing toward reporting a finding, the
// safe direction this tool's own doc commits to), never narrows one.
func buildSentinelMeta(idx *pkgIndex) map[string]sentinelMeta {
	out := map[string]sentinelMeta{}

	for _, f := range idx.Files {
		for _, decl := range f.Decls {
			gd, ok := decl.(*ast.GenDecl)
			if !ok || gd.Tok != token.VAR {
				continue
			}

			for _, spec := range gd.Specs {
				addSentinelMetaSpec(spec, idx.Sentinels, out)
			}
		}
	}

	return out
}

func addSentinelMetaSpec(spec ast.Spec, sentinels map[string]bool, out map[string]sentinelMeta) {
	vs, ok := spec.(*ast.ValueSpec)
	if !ok || len(vs.Names) != len(vs.Values) {
		return
	}

	for i, name := range vs.Names {
		if !sentinels[name.Name] {
			continue
		}

		call, isCall := vs.Values[i].(*ast.CallExpr)
		if !isCall {
			continue
		}

		out[name.Name] = sentinelMetaFromCall(call, sentinels)
	}
}

func sentinelMetaFromCall(call *ast.CallExpr, sentinels map[string]bool) sentinelMeta {
	var meta sentinelMeta

	for _, arg := range call.Args {
		if !meta.HasMessage {
			if v, ok := stringLiteralArg(arg); ok {
				meta.Message, meta.HasMessage = v, true

				continue
			}
		}

		if !meta.HasBase {
			if key, ok := identKey(arg, sentinels); ok {
				meta.Base, meta.HasBase = key, true
			}
		}
	}

	return meta
}

// reachSet is what an operation's own hop0/hop1 call graph (the SAME
// discipline emit.go's own emission walk uses) was found to be able to
// return: Identities is every sentinel identity (local or, via one hop of
// sentinelMeta unwrapping, its external base) a reachable return statement
// carries; Messages is the literal text of every reachable sentinel that
// carries one, for a strings.Contains-style guard to be matched against.
// Determined records whether this scan actually found and read at least
// one hop-1 call target: false means "nothing could be resolved," and a
// guard must never be treated as unreachable on that basis -- see
// filterUnreachable's own doc comment for why.
type reachSet struct {
	Identities map[string]bool
	Messages   []string
	Determined bool
}

// computeReachSet walks roots' own bodies (hop 0) and, up to maxEmitHop,
// every function/method they call directly (hop 1) -- identical recursion
// to emit.go's scanBodyEmissions/recurseCallEmissions, reusing
// calleeFuncDecls so a callee this scan's own emission walk would explore
// is exactly the callee this reachability walk explores too. mapperNames
// (guardindex.go's second return) is excluded from that recursion
// entirely -- not merely "harmless to include," but actively WRONG to
// include: a mapper is reached by construction whenever there is a
// candidate emission to filter at all, so recursing into it would make
// reachSet.Determined true almost unconditionally, defeating the "cannot
// determine reachability -> report" escape hatch this whole mechanism
// exists to provide (caught by this package's own
// TestReachability_UndeterminedReachability_StillReported failing against
// a version of this function that recursed into everything).
func computeReachSet(roots []opRoot, idx *pkgIndex, cls *classifiers) reachSet {
	rs := reachSet{Identities: map[string]bool{}}

	visited := map[*ast.BlockStmt]bool{}
	for _, r := range roots {
		collectReachIdentities(r.Body, idx, cls.MapperNames, &rs, 0, visited)
	}

	expandSentinelBases(&rs, cls.SentinelMeta)

	return rs
}

func collectReachIdentities(
	body *ast.BlockStmt,
	idx *pkgIndex,
	mapperNames map[string]bool,
	rs *reachSet,
	hop int,
	visited map[*ast.BlockStmt]bool,
) {
	if body == nil || visited[body] {
		return
	}

	visited[body] = true

	ast.Inspect(body, func(n ast.Node) bool {
		if ret, ok := n.(*ast.ReturnStmt); ok {
			collectReturnIdentities(ret, idx.Sentinels, rs)
		}

		if hop < maxEmitHop {
			if call, ok := n.(*ast.CallExpr); ok {
				recurseReachCall(call, idx, mapperNames, rs, hop, visited)
			}
		}

		return true
	})
}

func recurseReachCall(
	call *ast.CallExpr,
	idx *pkgIndex,
	mapperNames map[string]bool,
	rs *reachSet,
	hop int,
	visited map[*ast.BlockStmt]bool,
) {
	if name, ok := calleeSimpleName(call.Fun); ok && mapperNames[name] {
		return
	}

	for _, fd := range calleeFuncDecls(call.Fun, idx) {
		if fd.Body == nil {
			continue
		}

		rs.Determined = true

		collectReachIdentities(fd.Body, idx, mapperNames, rs, hop+1, visited)
	}
}

func collectReturnIdentities(ret *ast.ReturnStmt, sentinels map[string]bool, rs *reachSet) {
	for _, res := range ret.Results {
		if key, ok := guardIdentFromExpr(res, sentinels); ok {
			rs.Identities[key] = true
			rs.Determined = true
		}
	}
}

// guardIdentFromExpr mirrors sentinelRefCode's traversal (a bare reference,
// a unary `&`, a composite literal's own field values, an argument to
// fmt.Errorf) but resolves to the referenced identity itself rather than a
// code looked up from a fixed table -- reachability needs to know WHAT was
// returned, not what it maps to.
func guardIdentFromExpr(expr ast.Expr, sentinels map[string]bool) (string, bool) {
	switch e := expr.(type) {
	case *ast.Ident, *ast.SelectorExpr:
		return identKey(e, sentinels)
	case *ast.UnaryExpr:
		if e.Op == token.AND {
			return guardIdentFromExpr(e.X, sentinels)
		}
	case *ast.CompositeLit:
		return guardIdentFromElts(e.Elts, sentinels)
	case *ast.CallExpr:
		if isFmtErrorfCall(e) {
			return guardIdentFromArgs(e.Args, sentinels)
		}
	}

	return "", false
}

func guardIdentFromElts(elts []ast.Expr, sentinels map[string]bool) (string, bool) {
	for _, elt := range elts {
		v := elt
		if kv, ok := elt.(*ast.KeyValueExpr); ok {
			v = kv.Value
		}

		if key, ok := guardIdentFromExpr(v, sentinels); ok {
			return key, true
		}
	}

	return "", false
}

func guardIdentFromArgs(args []ast.Expr, sentinels map[string]bool) (string, bool) {
	for _, a := range args {
		if key, ok := guardIdentFromExpr(a, sentinels); ok {
			return key, true
		}
	}

	return "", false
}

// expandSentinelBases adds, for every identity already found reachable, the
// external base it wraps (if any) and the literal message it carries (if
// any) -- one hop of sentinelMeta indirection, matching this repo's
// standing one-hop discipline elsewhere in this tool. Iterates over a
// sorted snapshot of the starting identities so growing the map mid-walk
// never depends on Go's unspecified map-iteration-with-mutation order --
// required for this tool's own determinism guarantee.
func expandSentinelBases(rs *reachSet, meta map[string]sentinelMeta) {
	names := slices.Sorted(maps.Keys(rs.Identities))

	for _, name := range names {
		m, ok := meta[name]
		if !ok {
			continue
		}

		if m.HasBase {
			rs.Identities[m.Base] = true
		}

		if m.HasMessage {
			rs.Messages = append(rs.Messages, m.Message)
		}
	}

	sort.Strings(rs.Messages)
}

func guardReachable(g guard, rs reachSet) bool {
	for _, k := range g.IdentityKeys {
		if rs.Identities[k] {
			return true
		}
	}

	for _, k := range g.MessageKeys {
		for _, m := range rs.Messages {
			if strings.Contains(m, k) {
				return true
			}
		}
	}

	return false
}

// filterUnreachable drops an emission whose position is a code literal
// gated by a guard (guardindex.go) this operation's own reachable set
// (computeReachSet, over roots -- the SAME roots this operation's emission
// walk itself used) cannot satisfy: gopherstack-axs3's fix. An emission
// with no recorded guard (a default/fallback branch, an errors.As-gated
// branch, or simply a literal outside any recognised mapper switch/if) is
// always kept -- there is nothing to check reachability against. And an
// emission WITH a guard is also kept whenever this operation's own
// reachable set could not be determined at all (reachSet.Determined
// false -- no hop-1 call target this scan could resolve and read): an
// unresolved call graph is not evidence of unreachability, and this tool's
// own package doc commits to reporting rather than guessing in that case.
// reachSet is computed lazily (once per operation, not per emission) and
// only when at least one emission actually carries a guard, so an
// unaffected service pays nothing extra.
func filterUnreachable(
	emissions []emission,
	roots []opRoot,
	idx *pkgIndex,
	cls *classifiers,
) []emission {
	if len(cls.GuardsByPos) == 0 || len(emissions) == 0 {
		return emissions
	}

	var rs reachSet

	computed := false

	out := make([]emission, 0, len(emissions))

	for _, e := range emissions {
		g, guarded := cls.GuardsByPos[e.Pos]
		if !guarded {
			out = append(out, e)

			continue
		}

		if !computed {
			rs = computeReachSet(roots, idx, cls)
			computed = true
		}

		if !rs.Determined || guardReachable(g, rs) {
			out = append(out, e)
		}
	}

	return out
}
