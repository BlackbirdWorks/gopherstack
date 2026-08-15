package main

import (
	"sort"
	"strings"
)

const (
	// exclusionLookbehindChars/exclusionLookaheadChars bound the text window
	// isExclusion scans around a literal to recognize the "if HasPrefix(...)
	// { return false }" and "!HasPrefix(...)" carve-out shapes.
	exclusionLookbehindChars = 60
	exclusionLookaheadChars  = 250

	// inferKindLookbehindChars bounds the window inferKind scans behind a
	// literal for "HasPrefix"/"CutPrefix"/"=="/"case " context.
	inferKindLookbehindChars = 40

	// pathSegmentSplitLimit keeps segmentOf to the first "/"-delimited
	// segment only.
	pathSegmentSplitLimit = 2
)

// claimCollector de-duplicates and accumulates path claims found while
// scanning a single RouteMatcher body's source text.
type claimCollector struct {
	seen map[string]claim
	body string
}

func newClaimCollector(body string) *claimCollector {
	return &claimCollector{body: body, seen: map[string]claim{}}
}

func (cc *claimCollector) add(lit string, pos, endPos int) {
	if !strings.HasPrefix(lit, "/") || lit == "/" {
		return
	}

	if isExclusion(cc.body, pos, endPos) {
		return
	}

	kind := inferKind(cc.body, pos)

	key := lit + "|" + kind.String()
	if _, dup := cc.seen[key]; dup {
		return
	}

	cc.seen[key] = claim{Literal: lit, Segment: segmentOf(lit), Kind: kind, KindStr: kind.String()}
}

func (cc *claimCollector) claims() []claim {
	out := make([]claim, 0, len(cc.seen))
	for _, c := range cc.seen {
		out = append(out, c)
	}

	sort.Slice(out, func(i, j int) bool { return out[i].Literal < out[j].Literal })

	return out
}

// extractClaims pulls path-prefix claims out of a RouteMatcher body's source
// text: direct "/..." literals, "/"+identifier and identifier+"/"
// concatenations resolved against the package const table, and bare
// path-const identifiers (including package-level []string tables) used
// standalone. kind is inferred from nearby context (== implies exact,
// HasPrefix/CutPrefix implies prefix; anything else is conservatively
// treated as prefix, since that's the riskier case to under-report).
func extractClaims(body string, consts map[string]string, sliceConsts map[string][]string) []claim {
	cc := newClaimCollector(body)

	scanQuotedLiterals(cc, body)
	scanConcatLiterals(cc, body, consts)
	scanIdentifierLiterals(cc, body, consts, sliceConsts)

	return cc.claims()
}

func scanQuotedLiterals(cc *claimCollector, body string) {
	for _, m := range quotedRe.FindAllStringSubmatchIndex(body, -1) {
		cc.add(body[m[2]:m[3]], m[0], m[1])
	}
}

func scanConcatLiterals(cc *claimCollector, body string, consts map[string]string) {
	for _, m := range concatLeftRe.FindAllStringSubmatchIndex(body, -1) {
		ident := body[m[2]:m[3]]
		if val, ok := consts[ident]; ok {
			cc.add("/"+strings.TrimPrefix(val, "/"), m[0], m[1])
		}
	}

	for _, m := range concatRightRe.FindAllStringSubmatchIndex(body, -1) {
		ident := body[m[2]:m[3]]
		if val, ok := consts[ident]; ok && strings.HasPrefix(val, "/") {
			cc.add(val, m[0], m[1])
		}
	}
}

func scanIdentifierLiterals(
	cc *claimCollector,
	body string,
	consts map[string]string,
	sliceConsts map[string][]string,
) {
	for _, m := range bareIdentRe.FindAllStringSubmatchIndex(body, -1) {
		ident := strings.TrimSuffix(body[m[2]:m[3]], "()")

		if val, ok := consts[ident]; ok {
			cc.add(val, m[0], m[1])

			continue
		}

		for _, e := range sliceConsts[ident] {
			cc.add(e, m[0], m[1])
		}
	}
}

// isExclusion recognizes the "path prefix means NOT this service" pattern
// used to carve UI/internal routes (dashboard, /api/, /metrics/) out of an
// otherwise-broad matcher: a leading "!" on the containing HasPrefix/Contains
// call, or a "return false" appearing before any "return true" in the
// window immediately following the literal (the single-condition
// `if strings.HasPrefix(path, "/x/") { return false }` shape).
func isExclusion(body string, pos, endPos int) bool {
	behindStart := max(pos-exclusionLookbehindChars, 0)

	behind := body[behindStart:pos]
	if idx := strings.LastIndexAny(behind, "(,"); idx >= 0 {
		if strings.HasSuffix(strings.TrimSpace(behind[:idx]), "!") {
			return true
		}
	}

	aheadEnd := min(endPos+exclusionLookaheadChars, len(body))
	ahead := body[endPos:aheadEnd]

	falseIdx := strings.Index(ahead, "return false")
	trueIdx := strings.Index(ahead, "return true")

	return falseIdx >= 0 && (trueIdx < 0 || falseIdx < trueIdx)
}

func inferKind(body string, pos int) claimKind {
	start := max(pos-inferKindLookbehindChars, 0)
	ctx := body[start:pos]

	if strings.Contains(ctx, "HasPrefix") || strings.Contains(ctx, "CutPrefix") || strings.Contains(ctx, "HasSuffix") {
		return kindPrefix
	}

	if strings.Contains(ctx, "==") || strings.Contains(ctx, "case ") {
		return kindExact
	}

	return kindPrefix
}

func segmentOf(lit string) string {
	trimmed := strings.TrimPrefix(lit, "/")
	parts := strings.SplitN(trimmed, "/", pathSegmentSplitLimit)

	return strings.ToLower(parts[0])
}
