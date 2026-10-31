package cosmosdb

import (
	"encoding/json"
	"fmt"
	"math"
	"regexp"
	"slices"
	"strings"
)

const (
	maxFullTextPaths = 20
	maxFullTextTerms = 100

	bm25K1   = 1.2
	bm25B    = 0.75
	bm25Half = 0.5

	rrfK = 60
)

var languageRe = regexp.MustCompile(`^[a-z]{2,3}-[A-Z]{2}$`)

type fullTextPath struct {
	Path     string `json:"path"`
	Language string `json:"language"`
}

type fullTextPolicy struct {
	DefaultLanguage string         `json:"defaultLanguage"`
	FullTextPaths   []fullTextPath `json:"fullTextPaths"`
}

func parseFullTextPolicy(raw json.RawMessage) (fullTextPolicy, error) {
	var pol fullTextPolicy
	if isJSONNull(raw) {
		return pol, nil
	}

	if err := json.Unmarshal(raw, &pol); err != nil {
		return pol, fmt.Errorf("%w: fullTextPolicy is malformed", ErrInvalidContainerPolicy)
	}

	return pol, nil
}

func validateFullTextPolicy(raw json.RawMessage) error {
	pol, err := parseFullTextPolicy(raw)
	if err != nil {
		return err
	}

	if pol.DefaultLanguage != "" && !languageRe.MatchString(pol.DefaultLanguage) {
		return fmt.Errorf("%w: fullTextPolicy defaultLanguage %q is not a language tag like en-US",
			ErrInvalidContainerPolicy, pol.DefaultLanguage)
	}

	if len(pol.FullTextPaths) > maxFullTextPaths {
		return fmt.Errorf("%w: at most %d full text paths are allowed", ErrInvalidContainerPolicy, maxFullTextPaths)
	}

	seen := make(map[string]struct{}, len(pol.FullTextPaths))

	for _, p := range pol.FullTextPaths {
		if !strings.HasPrefix(p.Path, "/") || len(p.Path) == 1 {
			return fmt.Errorf("%w: full text path %q must start with '/'", ErrInvalidContainerPolicy, p.Path)
		}

		if p.Language != "" && !languageRe.MatchString(p.Language) {
			return fmt.Errorf("%w: full text path language %q is not a language tag like en-US",
				ErrInvalidContainerPolicy, p.Language)
		}

		if _, dup := seen[p.Path]; dup {
			return fmt.Errorf("%w: duplicate full text path %q", ErrInvalidContainerPolicy, p.Path)
		}

		seen[p.Path] = struct{}{}
	}

	return nil
}

// validateFullTextIndexes checks that every fullTextIndexes path is declared in the fullTextPolicy.
func validateFullTextIndexes(pol indexingPolicy, fp fullTextPolicy) error {
	for _, fi := range pol.FullTextIndexes {
		if !slices.ContainsFunc(fp.FullTextPaths, func(p fullTextPath) bool { return p.Path == fi.Path }) {
			return fmt.Errorf("%w: full text index path %q is not in the fullTextPolicy",
				ErrInvalidContainerPolicy, fi.Path)
		}
	}

	return nil
}

// ftTerm is one search term: the analysed word and how many edits a document word may differ by (fuzzy search,
// at most two).
type ftTerm struct {
	text string
	dist int
}

const maxFuzzyDistance = 2

// ftTerms collects the search terms from string arguments, {"term": ..., "distance": n} objects or one array of
// either, analysed and deduplicated. A string with several words contributes each word.
func ftTerms(lang string, args []any) ([]ftTerm, bool) {
	var raw []any
	if len(args) == 1 {
		if arr, ok := args[0].([]any); ok {
			raw = arr
		}
	}

	if raw == nil {
		raw = args
	}

	if len(raw) == 0 || len(raw) > maxFullTextTerms {
		return nil, false
	}

	var terms []ftTerm

	for _, a := range raw {
		text, dist, ok := ftTermOf(a)
		if !ok {
			return nil, false
		}

		for _, t := range ftAnalyze(lang, text) {
			term := ftTerm{text: t, dist: dist}
			if !slices.Contains(terms, term) {
				terms = append(terms, term)
			}
		}
	}

	return terms, len(terms) > 0
}

func ftTermOf(a any) (string, int, bool) {
	switch t := a.(type) {
	case string:
		return t, 0, true
	case map[string]any:
		text, ok := t["term"].(string)
		if !ok {
			return "", 0, false
		}

		dist := 0

		if d, has := t["distance"]; has {
			n, nok := intArg(d)
			if !nok || n < 0 || n > maxFuzzyDistance {
				return "", 0, false
			}

			dist = n
		}

		return text, dist, true
	}

	return "", 0, false
}

func termFreq(tokens []string) map[string]int {
	tf := make(map[string]int, len(tokens))
	for _, t := range tokens {
		tf[t]++
	}

	return tf
}

// evalFullText evaluates FullTextContains, FullTextContainsAll, FullTextContainsAny and FullTextScore.
func (ec *evalCtx) evalFullText(n *exprCall, args []any) any {
	text, ok := args[0].(string)
	if !ok {
		return undef{}
	}

	lang := ec.ftLanguage(n)

	terms, ok := ftTerms(lang, args[1:])
	if !ok {
		return undef{}
	}

	tokens := ftAnalyze(lang, text)
	tf := termFreq(tokens)

	switch n.name {
	case biFullTextContains, biFullTextContainsAll:
		return countPresent(tf, terms) == len(terms)
	case biFullTextContainsAny:
		return countPresent(tf, terms) > 0
	default:
		for _, t := range terms {
			if t.dist > 0 {
				return undef{}
			}
		}

		return floatResult(ec.bm25(n, tf, len(tokens), terms))
	}
}

func countPresent(tf map[string]int, terms []ftTerm) int {
	c := 0

	for _, t := range terms {
		if termPresent(tf, t) {
			c++
		}
	}

	return c
}

func termPresent(tf map[string]int, t ftTerm) bool {
	if tf[t.text] > 0 {
		return true
	}

	if t.dist == 0 {
		return false
	}

	for word := range tf {
		if editDistance(word, t.text) <= t.dist {
			return true
		}
	}

	return false
}

// ftLanguage is the language of the text path a call reads: the path's language in the full-text policy, else the
// policy default, else en-US.
func (ec *evalCtx) ftLanguage(n *exprCall) string {
	if ec.coll != nil {
		if path, ok := rootPath(n.args[0]); ok {
			if lang := ec.coll.ftLang[path]; lang != "" {
				return lang
			}
		}

		if ec.coll.ftDefaultLang != "" {
			return ec.coll.ftDefaultLang
		}
	}

	return defaultFullTextLanguage
}

// bm25Stats are the corpus statistics of one full-text path.
type bm25Stats struct {
	df     map[string]int
	n      int
	avgLen float64
}

// bm25 scores a document against terms with Okapi BM25 over the whole scanned container (the real service
// scores per physical partition).
func (ec *evalCtx) bm25(n *exprCall, tf map[string]int, docLen int, terms []ftTerm) float64 {
	st := ec.ftCorpus(n)
	score := 0.0

	for _, term := range terms {
		t := term.text
		f := float64(tf[t])
		if f == 0 {
			continue
		}

		idf := math.Log(1 + (float64(st.n)-float64(st.df[t])+bm25Half)/(float64(st.df[t])+bm25Half))
		norm := f + bm25K1*(1-bm25B+bm25B*float64(docLen)/math.Max(st.avgLen, 1))
		score += idf * f * (bm25K1 + 1) / norm
	}

	return score
}

// ftCorpus returns the cached statistics for the call's text path; without a container path the single document is
// its own corpus.
func (ec *evalCtx) ftCorpus(n *exprCall) *bm25Stats {
	path, ok := rootPath(n.args[0])
	if !ok || path == "" || ec.coll == nil || ec.coll.rows == nil {
		return &bm25Stats{n: 1, avgLen: 1, df: map[string]int{}}
	}

	if st, cached := ec.coll.ftStats[path]; cached {
		return st
	}

	st := &bm25Stats{df: map[string]int{}}
	total := 0

	for i := range ec.coll.rows.count() {
		row := ec.coll.rows.row(i)

		text, isStr := ec.eval(n.args[0], []any{&row}).(string)
		if !isStr {
			continue
		}

		tokens := ftAnalyze(ec.ftLanguage(n), text)
		st.n++
		total += len(tokens)

		for t := range termFreq(tokens) {
			st.df[t]++
		}
	}

	if st.n > 0 {
		st.avgLen = float64(total) / float64(st.n)
	}

	ec.coll.ftStats[path] = st

	return st
}
