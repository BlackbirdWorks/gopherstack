package azuresearch

import (
	"math"
	"regexp"
	"strings"
)

const (
	bm25K1 = 1.2
	bm25B  = 0.75
)

type searchCtx struct {
	ix      *Index
	weights map[string]float64
	syn     map[string]*synRules
	fields  []*fieldDef
	anyMode bool
}

func (sc *searchCtx) fieldsFor(name string) []*fieldDef {
	if name == "" {
		return sc.fields
	}

	if fd, ok := sc.ix.comp.byPath[name]; ok {
		return []*fieldDef{fd}
	}

	return nil
}

func (sc *searchCtx) weight(path string) float64 {
	if w, ok := sc.weights[path]; ok {
		return w
	}

	return 1
}

func (sc *searchCtx) eval(n qNode, doc *Doc) (bool, float64) {
	switch t := n.(type) {
	case *qAll:
		return true, 1
	case *qBool:
		m, s := sc.evalBool(t, doc)

		return m, s * t.boost
	case *qTerm:
		return sc.evalLeaf(t.field, doc, func(fd *fieldDef, ft *fieldTokens) float64 { return sc.termScore(fd, ft, t) })
	case *qPhrase:
		return sc.evalLeaf(
			t.field,
			doc,
			func(fd *fieldDef, ft *fieldTokens) float64 { return sc.phraseScore(fd, ft, t) },
		)
	case *qRegex:
		return sc.evalLeaf(t.field, doc, func(_ *fieldDef, ft *fieldTokens) float64 { return regexScore(ft, t) })
	}

	return false, 0
}

func (sc *searchCtx) evalLeaf(field string, doc *Doc, score func(*fieldDef, *fieldTokens) float64) (bool, float64) {
	total, matched := 0.0, false

	for _, fd := range sc.fieldsFor(field) {
		ft := doc.tok[fd.Path]
		if ft == nil {
			continue
		}

		if s := score(fd, ft); s > 0 {
			total += s * sc.weight(fd.Path)
			matched = true
		}
	}

	return matched, total
}

func (sc *searchCtx) evalBool(b *qBool, doc *Doc) (bool, float64) {
	var (
		mustOK, anyShould, anyNotFail, hasMust, hasShould, hasNot bool
		score                                                     float64
	)

	mustOK = true

	for _, c := range b.clauses {
		m, s := sc.eval(c.node, doc)

		switch c.occ {
		case occMust:
			hasMust = true
			mustOK = mustOK && m
			score += s
		case occShould:
			hasShould = true
			if m {
				anyShould = true
				score += s
			}
		case occMustNot:
			hasNot = true
			if m {
				anyNotFail = true
			}
		}
	}

	return finishBool(sc.anyMode, hasMust, hasShould, hasNot, mustOK, anyShould, anyNotFail), score
}

func finishBool(anyMode, hasMust, hasShould, hasNot, mustOK, anyShould, notHit bool) bool {
	switch {
	case hasMust:
		return mustOK && !notHit
	case anyMode && hasNot:
		return anyShould || !notHit
	case hasShould:
		return anyShould && !notHit
	case hasNot:
		return !notHit
	}

	return true
}

func (sc *searchCtx) idf(path, term string) float64 {
	n := float64(sc.ix.stats.docs[path])
	df := float64(sc.ix.stats.df[path][term])

	return math.Log(1 + (n-df+half)/(df+half))
}

func (sc *searchCtx) bm25(path string, ft *fieldTokens, idf, tf float64) float64 {
	norm := 1 - bm25B + bm25B*float64(ft.length)/sc.ix.stats.avgLen(path)

	return idf * tf * (bm25K1 + 1) / (tf + bm25K1*norm)
}

func hasWildcard(s string) bool { return strings.ContainsAny(s, "*?") }

func wildcardRE(pattern string) *regexp.Regexp {
	var sb strings.Builder

	sb.WriteString("^")

	for _, r := range strings.ToLower(pattern) {
		switch r {
		case '*':
			sb.WriteString(".*")
		case '?':
			sb.WriteString(".")
		default:
			sb.WriteString(regexp.QuoteMeta(string(r)))
		}
	}

	sb.WriteString("$")

	return regexp.MustCompile(sb.String())
}

func (sc *searchCtx) termScore(fd *fieldDef, ft *fieldTokens, t *qTerm) float64 {
	switch {
	case hasWildcard(t.text):
		re := wildcardRE(t.text)
		for term := range ft.terms {
			if re.MatchString(term) {
				return t.boost
			}
		}

		return 0
	case t.fuzzy >= 0:
		return fuzzyScore(ft, strings.ToLower(t.text), t.fuzzy, t.boost)
	}

	toks := sc.searchTerms(fd, t.text)
	if len(toks) == 0 {
		return 0
	}

	best := 0.0

	for _, alt := range sc.synonymAlts(fd, toks) {
		if s := sc.altScore(fd, ft, alt); s > best {
			best = s
		}
	}

	return best * t.boost
}

func (sc *searchCtx) searchTerms(fd *fieldDef, text string) []string {
	toks := sc.ix.comp.searchAnalyzerFor(fd)(text)
	out := make([]string, len(toks))

	for i, t := range toks {
		out[i] = t.Term
	}

	return out
}

func (sc *searchCtx) synonymAlts(fd *fieldDef, terms []string) [][]string {
	for _, name := range fd.SynonymMaps {
		if r := sc.syn[name]; r != nil {
			return r.expand(terms)
		}
	}

	return [][]string{terms}
}

func (sc *searchCtx) altScore(fd *fieldDef, ft *fieldTokens, alt []string) float64 {
	if len(alt) == 1 {
		tf := float64(len(ft.terms[alt[0]]))
		if tf == 0 {
			return 0
		}

		return sc.bm25(fd.Path, ft, sc.idf(fd.Path, alt[0]), tf)
	}

	freq := phraseFreq(ft, alt, 0)
	if freq == 0 {
		return 0
	}

	idf := 0.0
	for _, t := range alt {
		idf += sc.idf(fd.Path, t)
	}

	return sc.bm25(fd.Path, ft, idf, float64(freq))
}

func (sc *searchCtx) phraseScore(fd *fieldDef, ft *fieldTokens, p *qPhrase) float64 {
	terms := sc.searchTerms(fd, p.text)
	if len(terms) == 0 {
		return 0
	}

	best := 0.0

	for _, alt := range sc.synonymAlts(fd, terms) {
		freq := phraseFreq(ft, alt, p.slop)
		if freq == 0 {
			continue
		}

		idf := 0.0
		for _, t := range alt {
			idf += sc.idf(fd.Path, t)
		}

		if s := sc.bm25(fd.Path, ft, idf, float64(freq)); s > best {
			best = s
		}
	}

	return best * p.boost
}

func regexScore(ft *fieldTokens, q *qRegex) float64 {
	for term := range ft.terms {
		if q.re.MatchString(term) {
			return q.boost
		}
	}

	return 0
}

// phraseFreq counts occurrences of terms as a phrase, allowing slop positions of deviation.
func phraseFreq(ft *fieldTokens, terms []string, slop int) int {
	first := ft.terms[terms[0]]
	count := 0

	for _, p0 := range first {
		ok := true

		for i := 1; i < len(terms) && ok; i++ {
			ok = hasPosNear(ft.terms[terms[i]], p0+i, slop)
		}

		if ok {
			count++
		}
	}

	return count
}

func hasPosNear(positions []int, want, slop int) bool {
	for _, p := range positions {
		if p >= want-slop && p <= want+slop {
			return true
		}
	}

	return false
}

func fuzzyScore(ft *fieldTokens, term string, maxDist int, boost float64) float64 {
	best := 0.0

	for cand := range ft.terms {
		d := editDistance(cand, term, maxDist)
		if d > maxDist {
			continue
		}

		l := float64(max(len([]rune(term)), 1))
		if s := boost * (1 - float64(d)/l); s > best {
			best = s
		}
	}

	return best
}

// editDistance is the Damerau-Levenshtein distance, returning limit+1 once it exceeds limit.
func editDistance(a, b string, limit int) int {
	ra, rb := []rune(a), []rune(b)
	if abs(len(ra)-len(rb)) > limit {
		return limit + 1
	}

	prev2 := make([]int, len(rb)+1)
	prev := make([]int, len(rb)+1)
	cur := make([]int, len(rb)+1)

	for j := range prev {
		prev[j] = j
	}

	for i := 1; i <= len(ra); i++ {
		cur[0] = i

		for j := 1; j <= len(rb); j++ {
			cost := 1
			if ra[i-1] == rb[j-1] {
				cost = 0
			}

			cur[j] = min(prev[j]+1, cur[j-1]+1, prev[j-1]+cost)
			if i > 1 && j > 1 && ra[i-1] == rb[j-2] && ra[i-2] == rb[j-1] {
				cur[j] = min(cur[j], prev2[j-2]+1)
			}
		}

		prev2, prev, cur = prev, cur, prev2
	}

	return prev[len(rb)]
}

func abs(n int) int {
	if n < 0 {
		return -n
	}

	return n
}
