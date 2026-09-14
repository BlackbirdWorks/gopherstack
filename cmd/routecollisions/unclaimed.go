package main

import (
	"fmt"
	"go/ast"
	"go/token"
	"maps"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// dispatchChaseDepthLimit bounds how many call/index hops the -unclaimed
// chase follows from a service.Service.Handler() method before it stops
// recursing. Dispatch chains run deeper than RouteMatcher chains (see
// chaseDepthLimit in delegation.go): Handler -> handleREST -> classifyPath
// -> classifyGET/POST/DELETE -> an opDispatch table is common (omics), and
// some services add one more hop through a sub-resource router.
const dispatchChaseDepthLimit = 8

// dispatchComparisonWindow bounds how far behind a quoted "/"-literal the
// dispatch-side scan looks for a comparison keyword before accepting it as
// a path-routing claim. extractClaims's plain scanQuotedLiterals (used
// as-is by the matcher-side chase, where nearly every "/"-literal in a
// short RouteMatcher body really is a path comparison) is too permissive
// once the chase runs 8 hops deep through arbitrary handler code: it reads
// a synthetic resource ID built by concatenation (route53's
// `"/hostedzone/" + hz.ID`) or a redirect Location header value
// (`"/2013-04-01" + ds.ID`) as if it were a route claim, because both are
// bare "/"-prefixed quoted literals with no comparison anywhere nearby.
const dispatchComparisonWindow = 40

func isDispatchComparisonContext(body string, pos int) bool {
	start := max(pos-dispatchComparisonWindow, 0)
	ctx := body[start:pos]

	return strings.Contains(ctx, "HasPrefix(") || strings.Contains(ctx, "HasSuffix(") ||
		strings.Contains(ctx, "Contains(") || strings.Contains(ctx, "CutPrefix(") ||
		strings.Contains(ctx, "==") || strings.Contains(ctx, "case ")
}

// urlPathMarker is present in essentially every path-based RouteMatcher
// body ("c.Request().URL.Path" or a local alias assigned from it) and
// absent from every header/X-Amz-Target-based one -- the cheap, robust way
// to tell the two apart without re-deriving the guard heuristics claims.go
// already uses for a different purpose (carve-out exclusions, not matcher
// kind).
const urlPathMarker = "URL.Path"

// unclaimedHit is one dispatcher path literal no RouteMatcher claim in its
// own service package covers.
type unclaimedHit struct {
	Dir     string
	Literal string
	Kind    string
}

// runUnclaimed drives the gopherstack-blzga sweep: for every service with a
// path-based RouteMatcher, chase the dispatch entry point(s) for path
// literals and report every one no RouteMatcher claim in the same package
// covers. Services whose only RouteMatcher(s) are header/X-Amz-Target-based
// (no URL.Path reference at all) are structurally immune and skipped, as
// are services whose RouteMatcher is path-based but extracts zero literal
// claims (a known tool limitation -- see services/_ROUTE_COLLISIONS.md --
// not something this diff can safely compare against).
func runUnclaimed() ([]unclaimedHit, error) {
	root, dirs, priorityConsts, err := loadServiceDirs()
	if err != nil {
		return nil, err
	}

	var hits []unclaimedHit

	_ = priorityConsts // MatchPriority is irrelevant to this sweep; loadServiceDirs returns it for run() symmetry

	for _, dir := range dirs {
		dirHits, analyzeErr := analyzeUnclaimedDir(filepath.Join(root, "services", dir), dir)
		if analyzeErr != nil {
			fmt.Fprintf(os.Stderr, "analyze-unclaimed %s: %v\n", dir, analyzeErr)

			continue
		}

		hits = append(hits, dirHits...)
	}

	sort.Slice(hits, func(i, j int) bool {
		if hits[i].Dir != hits[j].Dir {
			return hits[i].Dir < hits[j].Dir
		}

		return hits[i].Literal < hits[j].Literal
	})

	return hits, nil
}

func analyzeUnclaimedDir(dir, name string) ([]unclaimedHit, error) {
	pd, err := parsePackage(dir)
	if err != nil {
		return nil, err
	}

	if len(pd.routeMatchers) == 0 || len(pd.handlerFuncs) == 0 {
		return nil, nil
	}

	matcherClaims, pathBased := matcherClaimsFor(pd)
	if !pathBased {
		return nil, nil // every RouteMatcher in this package is header/X-Amz-Target-based
	}

	if len(matcherClaims) == 0 {
		return nil, nil // path-based but zero extractable claims: known tool limitation, can't diff
	}

	dispatchClaims := dedupeClaims(chaseDispatchEntries(pd))

	return unclaimedHitsFor(name, dispatchClaims, matcherClaims), nil
}

// blankComments returns a copy of src with every comment's bytes replaced
// by spaces (newlines preserved), so fset byte offsets computed against the
// ORIGINAL src still address the right text in the copy. A doc comment
// describing a path shape (extremely common in this repo -- see
// services/omics/routes.go's own doc comments) would otherwise read as a
// dispatcher path claim once the -unclaimed chase follows a call into the
// function it documents.
func blankComments(fset *token.FileSet, src []byte, comments []*ast.CommentGroup) []byte {
	out := append([]byte(nil), src...)

	for _, group := range comments {
		for _, c := range group.List {
			start := fset.Position(c.Pos()).Offset
			end := fset.Position(c.End()).Offset

			for i := start; i < end && i < len(out); i++ {
				if out[i] != '\n' {
					out[i] = ' '
				}
			}
		}
	}

	return out
}

// bodyTextNoComments is bodyText sourced from pd's comment-blanked copy of
// node's file -- see chaseDispatchClaims's doc comment for why the dispatch
// chase needs this and the matcher chase (delegation.go's chaseClaims,
// unchanged) does not.
func bodyTextNoComments(pd *pkgData, node ast.Node) string {
	fp := pd.fset.Position(node.Pos()).Filename

	return bodyText(pd.fset, pd.srcByFileNoComments[fp], node)
}

// matcherClaimsFor unions the extracted claims of every path-based
// RouteMatcher in pd (there can be more than one receiver per package --
// bedrock's agents dispatcher, redshift's serverless handler). pathBased is
// false only when every RouteMatcher found never references URL.Path at
// all, meaning the whole package is structurally immune to this bug class.
func matcherClaimsFor(pd *pkgData) ([]claim, bool) {
	var claims []claim

	pathBased := false

	for _, fn := range pd.routeMatchers {
		body := bodyTextNoComments(pd, fn.Body)

		if !strings.Contains(body, urlPathMarker) {
			continue
		}

		pathBased = true
		claims = append(claims, chaseClaims(body, fn.Body, pd, pd.consts, pd.sliceConsts, 0, map[string]bool{})...)
	}

	return dedupeClaims(claims), pathBased
}

// chaseDispatchEntries runs the dispatch-side chase from every Handler()
// method found in pd, unioning the results.
func chaseDispatchEntries(pd *pkgData) []claim {
	claims := make([]claim, 0, len(pd.handlerFuncs))

	for _, fn := range pd.handlerFuncs {
		body := bodyTextNoComments(pd, fn.Body)

		claims = append(
			claims,
			chaseDispatchClaims(body, fn.Body, pd, pd.consts, pd.sliceConsts, 0, map[string]bool{})...,
		)
	}

	return claims
}

// chaseDispatchClaims mirrors chaseClaims (delegation.go) but always runs
// the full text-based literal scan on every visited body, including a map
// composite literal's -- unlike a RouteMatcher's route-table map (keys are
// the only thing that matters there), a dispatcher's op table commonly maps
// operation NAMES to handler closures that themselves TrimPrefix/extractID
// against a path literal, e.g. omics' opDispatch. extractClaimsForNode's
// map-keys-only shortcut would silently drop those.
func chaseDispatchClaims(
	body string,
	node ast.Node,
	pd *pkgData,
	consts map[string]string,
	sliceConsts map[string][]string,
	depth int,
	visited map[string]bool,
) []claim {
	claims := extractDispatchClaimsForNode(body, node, pd.fset, consts, sliceConsts)

	if depth >= dispatchChaseDepthLimit {
		return claims
	}

	for _, name := range referencedNames(node) {
		if visited[name] {
			continue
		}

		childNode, ok := pd.namedBodies[name]
		if !ok {
			continue
		}

		visited[name] = true

		childBody := bodyTextNoComments(pd, childNode)

		claims = append(claims, chaseDispatchClaims(childBody, childNode, pd, consts, sliceConsts, depth+1, visited)...)
	}

	return claims
}

func extractDispatchClaimsForNode(
	body string,
	node ast.Node,
	fset *token.FileSet,
	consts map[string]string,
	sliceConsts map[string][]string,
) []claim {
	localConsts, declRanges := localConstTable(fset, node)

	merged := consts
	if len(localConsts) > 0 {
		merged = make(map[string]string, len(consts)+len(localConsts))

		maps.Copy(merged, consts)
		maps.Copy(merged, localConsts)
	}

	claims := extractDispatchClaims(body, merged, sliceConsts, declRanges)

	cc := newClaimCollector(body)
	scanMapKeyClaims(cc, node, merged)
	scanSliceLiteralIdentClaims(cc, node, merged)
	scanSwitchCaseIdentClaims(cc, node, merged)

	return append(claims, cc.claims()...)
}

// extractDispatchClaims mirrors extractClaims (claims.go) but swaps in
// scanQuotedLiteralsStrict for the plain scanQuotedLiterals -- see
// dispatchComparisonWindow's doc comment for why.
func extractDispatchClaims(
	body string,
	consts map[string]string,
	sliceConsts map[string][]string,
	declRanges [][2]int,
) []claim {
	cc := newClaimCollector(body)

	scanQuotedLiteralsStrict(cc, body, declRanges)
	scanConcatLiterals(cc, body, consts)
	scanIdentifierLiterals(cc, body, consts, sliceConsts)
	scanSecondArgPrefixIdent(cc, body, consts)

	return cc.claims()
}

func scanQuotedLiteralsStrict(cc *claimCollector, body string, declRanges [][2]int) {
	for _, m := range quotedRe.FindAllStringSubmatchIndex(body, -1) {
		if inDeclRange(m[0], m[1], declRanges) {
			continue
		}

		if !isDispatchComparisonContext(body, m[0]) {
			continue
		}

		cc.add(body[m[2]:m[3]], m[0], m[1])
	}
}

// unclaimedHitsFor reports every dispatchClaims literal no matcherClaims
// entry covers, then drops any hit that is itself a strict extension of
// another reported hit in the same service -- fixing the shorter (more
// general) literal as a matcher claim would cover the longer one too, so
// reporting both is redundant noise ("normalised to its longest literal
// prefix" per gopherstack-blzga).
func unclaimedHitsFor(dir string, dispatchClaims, matcherClaims []claim) []unclaimedHit {
	var uncovered []claim

	for _, dc := range dispatchClaims {
		if !coveredByAny(dc, matcherClaims) {
			uncovered = append(uncovered, dc)
		}
	}

	return unclaimedHitsFromRoots(dir, minimalClaims(uncovered))
}

func coveredByAny(dc claim, matcherClaims []claim) bool {
	for _, mc := range matcherClaims {
		if dc.Literal == mc.Literal {
			return true
		}

		if mc.Kind == kindPrefix && strings.HasPrefix(dc.Literal, mc.Literal) {
			return true
		}
	}

	return false
}

// minimalClaims sorts claims shortest-literal-first and drops any claim
// that is a strict prefix-extension of one already kept.
func minimalClaims(claims []claim) []claim {
	sort.Slice(claims, func(i, j int) bool { return len(claims[i].Literal) < len(claims[j].Literal) })

	var kept []claim

	for _, c := range claims {
		if !isExtensionOfAny(c, kept) {
			kept = append(kept, c)
		}
	}

	return kept
}

func isExtensionOfAny(c claim, kept []claim) bool {
	for _, k := range kept {
		if c.Literal != k.Literal && strings.HasPrefix(c.Literal, k.Literal) {
			return true
		}
	}

	return false
}

func unclaimedHitsFromRoots(dir string, claims []claim) []unclaimedHit {
	if len(claims) == 0 {
		return nil
	}

	out := make([]unclaimedHit, 0, len(claims))
	for _, c := range claims {
		out = append(out, unclaimedHit{Dir: dir, Literal: c.Literal, Kind: c.KindStr})
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Literal < out[j].Literal })

	return out
}

func printUnclaimedReport(hits []unclaimedHit) {
	fmt.Fprintf(os.Stdout, "%d unclaimed dispatcher path literal(s) found across all services\n\n", len(hits))

	lastDir := ""

	for _, h := range hits {
		if h.Dir != lastDir {
			fmt.Fprintf(os.Stdout, "%s:\n", h.Dir)

			lastDir = h.Dir
		}

		fmt.Fprintf(os.Stdout, "  %-8s %s\n", h.Kind, h.Literal)
	}
}
