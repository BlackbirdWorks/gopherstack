package azuresearch

import (
	"maps"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/blevesearch/snowballstem"
	"github.com/blevesearch/snowballstem/arabic"
	"github.com/blevesearch/snowballstem/danish"
	"github.com/blevesearch/snowballstem/dutch"
	"github.com/blevesearch/snowballstem/english"
	"github.com/blevesearch/snowballstem/finnish"
	"github.com/blevesearch/snowballstem/french"
	"github.com/blevesearch/snowballstem/german"
	"github.com/blevesearch/snowballstem/hungarian"
	"github.com/blevesearch/snowballstem/irish"
	"github.com/blevesearch/snowballstem/italian"
	"github.com/blevesearch/snowballstem/norwegian"
	"github.com/blevesearch/snowballstem/porter"
	"github.com/blevesearch/snowballstem/portuguese"
	"github.com/blevesearch/snowballstem/romanian"
	"github.com/blevesearch/snowballstem/russian"
	"github.com/blevesearch/snowballstem/spanish"
	"github.com/blevesearch/snowballstem/swedish"
	"github.com/blevesearch/snowballstem/turkish"
)

const (
	typeClassicTokenizer   = "#Microsoft.Azure.Search.ClassicTokenizer"
	typeStandardTokenizer  = "#Microsoft.Azure.Search.StandardTokenizerV2"
	typeUaxTokenizer       = "#Microsoft.Azure.Search.UaxUrlEmailTokenizer"
	typeKeywordTokenizer   = "#Microsoft.Azure.Search.KeywordTokenizerV2"
	typeLetterTokenizer    = "#Microsoft.Azure.Search.LetterTokenizer"
	typeLowerTokenizer     = "#Microsoft.Azure.Search.LowercaseTokenizerV2"
	typeNGramTokenizer     = "#Microsoft.Azure.Search.NGramTokenizer"
	typeEdgeNGramTokenizer = "#Microsoft.Azure.Search.EdgeNGramTokenizer"
	typePathTokenizer      = "#Microsoft.Azure.Search.PathHierarchyTokenizerV2"
	typeMSTokenizer        = "#Microsoft.Azure.Search.MicrosoftLanguageTokenizer"
	typeMSStemTokenizer    = "#Microsoft.Azure.Search.MicrosoftLanguageStemmingTokenizer"

	typeTruncateFilter    = "#Microsoft.Azure.Search.TruncateTokenFilter"
	typeUniqueFilter      = "#Microsoft.Azure.Search.UniqueTokenFilter"
	typeShingleFilter     = "#Microsoft.Azure.Search.ShingleTokenFilter"
	typeCommonGramsFilter = "#Microsoft.Azure.Search.CommonGramTokenFilter"
	typeCjkBigramFilter   = "#Microsoft.Azure.Search.CjkBigramTokenFilter"
	typeKeepFilter        = "#Microsoft.Azure.Search.KeepTokenFilter"
	typeKeywordMarker     = "#Microsoft.Azure.Search.KeywordMarkerTokenFilter"
	typePatternCapture    = "#Microsoft.Azure.Search.PatternCaptureTokenFilter"
	typeStemmerFilter     = "#Microsoft.Azure.Search.StemmerTokenFilter"
	typeSnowballFilter    = "#Microsoft.Azure.Search.SnowballTokenFilter"
	typeStopwordsFilter   = "#Microsoft.Azure.Search.StopwordsTokenFilter"
	typeWordDelimiter     = "#Microsoft.Azure.Search.WordDelimiterTokenFilter"
	typeDecompounder      = "#Microsoft.Azure.Search.DictionaryDecompounderTokenFilter"
	typeStemmerOverride   = "#Microsoft.Azure.Search.StemmerOverrideTokenFilter"

	defaultTokenLen   = 255
	pairLen           = 2
	ngramTokenizerMax = 2048
	defaultMinWord    = 5
	defaultMinSub     = 2
	defaultMaxSub     = 15
	fullwidthFirst    = 0xFF01
	fullwidthLast     = 0xFF5E
	asciiFirst        = 0x21
	langEnglish       = "english"
	langFrench        = "french"
	langGerman        = "german"
	langSpanish       = "spanish"
	langItalian       = "italian"
	langHungarian     = "hungarian"
	langRomanian      = "romanian"
	langTurkish       = "turkish"
	langNorwegian     = "norwegian"
	langRussian       = "russian"
	langPorter        = "porter"
	langArabic        = "arabic"
	langDanish        = "danish"
	langDutch         = "dutch"
	langFinnish       = "finnish"
	langIrish         = "irish"
	langPortuguese    = "portuguese"
	langSwedish       = "swedish"
)

func stemmerFunc(f func(*snowballstem.Env) bool) func(string) string {
	return func(w string) string {
		env := snowballstem.NewEnv(w)
		f(env)

		return env.Current()
	}
}

//nolint:gochecknoglobals // read-only language table
var snowballLangs = map[string]func(*snowballstem.Env) bool{
	langArabic: arabic.Stem, langDanish: danish.Stem, langDutch: dutch.Stem, langEnglish: english.Stem,
	langFinnish: finnish.Stem, langFrench: french.Stem, langGerman: german.Stem, langHungarian: hungarian.Stem,
	langIrish: irish.Stem, langItalian: italian.Stem, langNorwegian: norwegian.Stem, langPorter: porter.Stem,
	langPortuguese: portuguese.Stem, langRomanian: romanian.Stem, langRussian: russian.Stem, langSpanish: spanish.Stem,
	langSwedish: swedish.Stem, langTurkish: turkish.Stem,
}

// stemmerLanguages maps StemmerTokenFilter languages to Snowball: english is Lucene's PorterStemFilter and
// porter2 the Snowball English stemmer; light/minimal variants are different algorithms and are not mapped.
//
//nolint:gochecknoglobals // read-only language table
var stemmerLanguages = map[string]string{
	langEnglish:    langPorter,
	"porter2":      langEnglish,
	langArabic:     langArabic,
	langDanish:     langDanish,
	langDutch:      langDutch,
	langFinnish:    langFinnish,
	langFrench:     langFrench,
	langGerman:     langGerman,
	langHungarian:  langHungarian,
	langIrish:      langIrish,
	langItalian:    langItalian,
	langNorwegian:  langNorwegian,
	langPortuguese: langPortuguese,
	langRomanian:   langRomanian,
	langRussian:    langRussian,
	langSpanish:    langSpanish,
	langSwedish:    langSwedish,
	langTurkish:    langTurkish,
}

func stemFilter(stem func(string) string) tokenFilterFn {
	return func(t []Token) []Token {
		for i := range t {
			if !t[i].Keyword {
				t[i].Term = stem(t[i].Term)
			}
		}

		return t
	}
}

func languageStemFilter(name, kind string, o map[string]any) (tokenFilterFn, error) {
	lang := strOf(o, "language")
	if lang == "" {
		lang = langEnglish
	}

	key := lang
	if kind == typeStemmerFilter {
		key = stemmerLanguages[lang]
	}

	f, ok := snowballLangs[key]
	if !ok {
		return nil, badf(
			"The request is invalid. Details: token filter '%s': language '%s' is not supported.",
			name,
			lang,
		)
	}

	return stemFilter(stemmerFunc(f)), nil
}

func chunkToken(t Token, limit int) []Token {
	r := []rune(t.Term)
	if len(r) <= limit {
		return []Token{t}
	}

	var out []Token

	off := t.Start

	for len(r) > 0 {
		n := min(limit, len(r))
		piece := string(r[:n])
		out = append(out, Token{Term: piece, Start: off, End: off + len(piece)})
		off += len(piece)
		r = r[n:]
	}

	return out
}

func limitedTokenizer(base Analyzer, limit int) Analyzer {
	return func(text string) []Token {
		out := make([]Token, 0, len(text))
		for _, t := range base(text) {
			out = append(out, chunkToken(t, limit)...)
		}

		return renumber(out)
	}
}

var uaxRE = regexp.MustCompile(
	`[A-Za-z0-9._%+\-]+@[A-Za-z0-9.\-]+\.[A-Za-z]{2,}|https?://[^\s<>"]+|[\p{L}\p{N}_]+(?:['’.][\p{L}\p{N}_]+)*`,
)

func uaxTokenizer(limit int) Analyzer {
	return limitedTokenizer(func(text string) []Token {
		locs := uaxRE.FindAllStringIndex(text, -1)
		out := make([]Token, 0, len(locs))

		for _, loc := range locs {
			out = append(out, Token{Term: text[loc[0]:loc[1]], Start: loc[0], End: loc[1]})
		}

		return out
	}, limit)
}

func tokenChars(o map[string]any) func(rune) bool {
	classes := listOf(o, "tokenChars")
	if len(classes) == 0 {
		return func(rune) bool { return true }
	}

	set := map[string]bool{}
	for _, c := range classes {
		s, _ := c.(string)
		set[s] = true
	}

	return func(r rune) bool {
		return set["letter"] && unicode.IsLetter(r) || set["digit"] && unicode.IsDigit(r) ||
			set["whitespace"] && unicode.IsSpace(r) || set["punctuation"] && unicode.IsPunct(r) ||
			set["symbol"] && unicode.IsSymbol(r)
	}
}

func gramTokenizer(name string, o map[string]any, edge bool) (Analyzer, error) {
	lo, hi := intProp(o, "minGram", 1), intProp(o, "maxGram", defaultMaxGram)
	limit := ngramTokenizerMax
	if edge {
		limit = maxAnalysisLength
	}

	if lo < 1 || hi > limit || lo >= hi {
		return nil, badf("The request is invalid. Details: tokenizer '%s': minGram must be less than maxGram, and "+
			"maxGram cannot exceed %d.", name, limit)
	}

	keep := tokenChars(o)

	return func(text string) []Token {
		out := make([]Token, 0, len(text))
		for _, run := range scan(text, keep, nil) {
			out = append(out, gramsOfRun(run, lo, hi, edge)...)
		}

		return renumber(out)
	}, nil
}

func gramsOfRun(run Token, lo, hi int, edge bool) []Token {
	rs := []rune(run.Term)
	offs := make([]int, len(rs)+1)

	for i, r := range rs {
		offs[i+1] = offs[i] + utf8.RuneLen(r)
	}

	last := len(rs) - 1
	if edge {
		last = 0
	}

	var out []Token

	for s := 0; s <= last; s++ {
		for n := lo; n <= hi && s+n <= len(rs); n++ {
			out = append(out, Token{Term: string(rs[s : s+n]), Start: run.Start + offs[s], End: run.Start + offs[s+n]})
		}
	}

	return out
}

func pathTokenizer(o map[string]any) (Analyzer, error) {
	delim := strOf(o, "delimiter")
	if delim == "" {
		delim = "/"
	}

	repl := strOf(o, "replacement")
	if repl == "" {
		repl = delim
	}

	if utf8.RuneCountInString(delim) != 1 || utf8.RuneCountInString(repl) != 1 {
		return nil, badf("The request is invalid. Details: delimiter and replacement must be single characters.")
	}

	limit, skip := intProp(o, "maxTokenLength", maxAnalysisLength), intProp(o, "skip", 0)
	reverse := boolOf(o, "reverse", false)

	return func(text string) []Token {
		parts := strings.Split(text, delim)
		if reverse {
			return reversePaths(parts, delim, repl, skip, limit)
		}

		return forwardPaths(parts, delim, repl, skip, limit)
	}, nil
}

func forwardPaths(parts []string, delim, repl string, skip, limit int) []Token {
	var out []Token

	for i := skip; i < len(parts); i++ {
		head := strings.Join(parts[:i+1], delim)
		if head == "" || i == len(parts)-1 && parts[i] == "" {
			continue
		}

		term := strings.ReplaceAll(strings.Join(parts[skip:i+1], delim), delim, repl)
		if skip > 0 {
			term = repl + term
		}

		if utf8.RuneCountInString(term) <= limit {
			out = append(out, Token{Term: term, Start: 0, End: len(head)})
		}
	}

	return out
}

func reversePaths(parts []string, delim, repl string, skip, limit int) []Token {
	var out []Token

	for i := range len(parts) - skip {
		if parts[i] == "" && i == len(parts)-1 {
			break
		}

		term := strings.ReplaceAll(strings.Join(parts[i:len(parts)-skip], delim), delim, repl)
		if term != "" && utf8.RuneCountInString(term) <= limit {
			start := len(strings.Join(parts[:i], delim))
			if i > 0 {
				start++
			}

			out = append(out, Token{Term: term, Start: start, End: start + len(term)})
		}
	}

	return out
}

func buildTokenizer(name, kind string, o map[string]any) (Analyzer, error) {
	limit := intProp(o, "maxTokenLength", defaultTokenLen)

	switch kind {
	case typeClassicTokenizer, typeStandardTokenizer:
		return limitedTokenizer(func(t string) []Token { return scan(t, isWordRune, standardJoin) }, limit), nil
	case typeUaxTokenizer:
		return uaxTokenizer(limit), nil
	case typeKeywordTokenizer:
		return limitedKeyword(intProp(o, "maxTokenLength", maxAnalysisLength+1)), nil
	case typeLetterTokenizer:
		return func(t string) []Token { return renumber(scan(t, isLetterRune, nil)) }, nil
	case typeLowerTokenizer:
		return func(t string) []Token { return renumber(mapTerms(scan(t, isLetterRune, nil), strings.ToLower)) }, nil
	case typeNGramTokenizer:
		return gramTokenizer(name, o, false)
	case typeEdgeNGramTokenizer:
		return gramTokenizer(name, o, true)
	case typePathTokenizer:
		return pathTokenizer(o)
	case typeMSTokenizer, typeMSStemTokenizer:
		return nil, badf("The request is invalid. Details: tokenizer '%s': the Microsoft language tokenizers are "+
			"proprietary and not available.", name)
	}

	return nil, badf("The request is invalid. Details: tokenizer '%s' has unsupported type '%s'.", name, kind)
}

func limitedKeyword(limit int) Analyzer {
	return func(text string) []Token {
		toks := keywordAnalyzer(text)
		if len(toks) == 1 && utf8.RuneCountInString(text) > limit {
			r := []rune(text)[:limit]
			toks[0] = Token{Term: string(r), End: len(string(r))}
		}

		return toks
	}
}

func wordList(o map[string]any, key string, fold bool) map[string]bool {
	out := map[string]bool{}

	for _, w := range listOf(o, key) {
		if s, ok := w.(string); ok {
			if fold {
				s = strings.ToLower(s)
			}

			out[s] = true
		}
	}

	return out
}

func termKey(s string, fold bool) string {
	if fold {
		return strings.ToLower(s)
	}

	return s
}

func buildMoreFilter(name, kind string, o map[string]any) (tokenFilterFn, error) {
	if b, ok := moreFilterBuilders[kind]; ok {
		return b(name, o)
	}

	return nil, badf("The request is invalid. Details: token filter '%s' has unsupported type '%s'.", name, kind)
}

type filterBuilder func(name string, o map[string]any) (tokenFilterFn, error)

func stemBuilder(kind string) filterBuilder {
	return func(name string, o map[string]any) (tokenFilterFn, error) { return languageStemFilter(name, kind, o) }
}

func plainBuilder(f func(o map[string]any) tokenFilterFn) filterBuilder {
	return func(_ string, o map[string]any) (tokenFilterFn, error) { return f(o), nil }
}

//nolint:gochecknoglobals // dispatch table
var moreFilterBuilders = map[string]filterBuilder{
	typeTruncateFilter: truncateBuilder,
	typeUniqueFilter: plainBuilder(
		func(o map[string]any) tokenFilterFn { return uniqueFilter(boolOf(o, "onlyOnSamePosition", false)) },
	),
	typeShingleFilter:     shingleFilter,
	typeCommonGramsFilter: plainBuilder(commonGramsFilter),
	typeCjkBigramFilter:   plainBuilder(cjkBigramFilter),
	typeKeepFilter:        plainBuilder(keepFilter),
	typeKeywordMarker:     plainBuilder(keywordMarkerFilter),
	typePatternCapture:    patternCaptureFilter,
	typeStemmerFilter:     stemBuilder(typeStemmerFilter),
	typeSnowballFilter:    stemBuilder(typeSnowballFilter),
	typeStopwordsFilter:   stopwordsTokenFilter,
	typeWordDelimiter:     plainBuilder(wordDelimiterFilter),
	typeDecompounder:      decompounderFilter,
	typeStemmerOverride:   stemmerOverrideFilter,
}

func truncateBuilder(name string, o map[string]any) (tokenFilterFn, error) {
	n := intProp(o, "length", maxAnalysisLength)
	if n < 1 || n > maxAnalysisLength {
		return nil, badf("The request is invalid. Details: token filter '%s': length must be between 1 and %d.",
			name, maxAnalysisLength)
	}

	return truncateFilter(n), nil
}

func keepFilter(o map[string]any) tokenFilterFn {
	fold := !boolOf(o, "keepWordsCase", false)
	words := wordList(o, "keepWords", fold)

	return func(t []Token) []Token {
		return slices.DeleteFunc(t, func(k Token) bool { return !words[termKey(k.Term, fold)] })
	}
}

func keywordMarkerFilter(o map[string]any) tokenFilterFn {
	fold := boolOf(o, "ignoreCase", false)
	words := wordList(o, "keywords", fold)

	return func(t []Token) []Token {
		for i := range t {
			t[i].Keyword = t[i].Keyword || words[termKey(t[i].Term, fold)]
		}

		return t
	}
}

func truncateFilter(n int) tokenFilterFn {
	return func(t []Token) []Token {
		return mapTerms(t, func(s string) string {
			if r := []rune(s); len(r) > n {
				return string(r[:n])
			}

			return s
		})
	}
}

func uniqueFilter(samePos bool) tokenFilterFn {
	return func(t []Token) []Token {
		seen := map[string]bool{}
		out := t[:0:0]

		for _, k := range t {
			key := k.Term
			if samePos {
				key = strconv.Itoa(k.Pos) + "\x00" + k.Term
			}

			if !seen[key] {
				seen[key] = true

				out = append(out, k)
			}
		}

		return out
	}
}

func reverseFilter(t []Token) []Token {
	return mapTerms(t, func(s string) string {
		r := []rune(s)
		slices.Reverse(r)

		return string(r)
	})
}

func keywordRepeatFilter(t []Token) []Token {
	out := make([]Token, 0, len(t)*pairLen)

	for _, k := range t {
		kw := k
		kw.Keyword = true
		out = append(out, kw, k)
	}

	return out
}

func shingleFilter(name string, o map[string]any) (tokenFilterFn, error) {
	lo, hi := intProp(o, "minShingleSize", pairLen), intProp(o, "maxShingleSize", pairLen)
	if lo < pairLen || hi < lo {
		return nil, badf("The request is invalid. Details: token filter '%s': maxShingleSize must be at least "+
			"minShingleSize, which must be at least 2.", name)
	}

	unigrams := boolOf(o, "outputUnigrams", true)
	onlyIfNone := boolOf(o, "outputUnigramsIfNoShingles", false)

	sep := " "
	if v, ok := o["tokenSeparator"].(string); ok {
		sep = v
	}

	return func(t []Token) []Token {
		var out []Token

		made := false

		for i, k := range t {
			if unigrams {
				out = append(out, k)
			}

			for n := lo; n <= hi && i+n <= len(t); n++ {
				terms := make([]string, n)
				for j := range n {
					terms[j] = t[i+j].Term
				}

				out = append(out, Token{Term: strings.Join(terms, sep), Pos: k.Pos, Start: k.Start, End: t[i+n-1].End})
				made = true
			}
		}

		if !made && onlyIfNone && !unigrams {
			out = append(out, t...)
		}

		return out
	}, nil
}

func commonGramsFilter(o map[string]any) tokenFilterFn {
	fold := boolOf(o, "ignoreCase", false)
	words := wordList(o, "commonWords", fold)
	query := boolOf(o, "useQueryMode", false)

	return func(t []Token) []Token {
		var out []Token

		for i, k := range t {
			common := words[termKey(k.Term, fold)]
			prevCommon := i > 0 && words[termKey(t[i-1].Term, fold)]

			emitUni := !query || (!common && !prevCommon && (i+1 >= len(t) || !words[termKey(t[i+1].Term, fold)]))
			if emitUni {
				out = append(out, k)
			}

			if i+1 < len(t) && (common || words[termKey(t[i+1].Term, fold)]) {
				out = append(out, Token{Term: k.Term + "_" + t[i+1].Term, Pos: k.Pos, Start: k.Start, End: t[i+1].End})
			}
		}

		return out
	}
}

func cjkClass(r rune) string {
	switch {
	case unicode.Is(unicode.Han, r):
		return "han"
	case unicode.Is(unicode.Hiragana, r):
		return "hiragana"
	case unicode.Is(unicode.Katakana, r):
		return "katakana"
	case unicode.Is(unicode.Hangul, r):
		return "hangul"
	}

	return ""
}

func cjkBigramFilter(o map[string]any) tokenFilterFn {
	ignored := wordList(o, "ignoreScripts", false)
	unigrams := boolOf(o, "outputUnigrams", false)

	return func(t []Token) []Token {
		out := make([]Token, 0, len(t))

		for _, k := range t {
			out = append(out, bigramToken(k, ignored, unigrams)...)
		}

		return out
	}
}

func bigramToken(k Token, ignored map[string]bool, unigrams bool) []Token {
	rs := []rune(k.Term)

	allCJK := len(rs) > 0

	for _, r := range rs {
		if c := cjkClass(r); c == "" || ignored[c] {
			allCJK = false
		}
	}

	if !allCJK {
		return []Token{k}
	}

	if len(rs) == 1 {
		return []Token{k}
	}

	var out []Token

	for i := range rs {
		if unigrams {
			out = append(out, Token{Term: string(rs[i]), Pos: k.Pos + i, Start: k.Start, End: k.End})
		}

		if i+1 < len(rs) {
			out = append(out, Token{Term: string(rs[i : i+2]), Pos: k.Pos + i, Start: k.Start, End: k.End})
		}
	}

	return out
}

func patternCaptureFilter(name string, o map[string]any) (tokenFilterFn, error) {
	var res []*regexp.Regexp

	for _, p := range listOf(o, "patterns") {
		s, _ := p.(string)

		re, err := regexp.Compile(s)
		if err != nil {
			return nil, badf("The request is invalid. Details: token filter '%s' has an invalid pattern.", name)
		}

		res = append(res, re)
	}

	if len(res) == 0 {
		return nil, badf("The request is invalid. Details: token filter '%s' requires at least one pattern.", name)
	}

	keepOrig := boolOf(o, "preserveOriginal", true)

	return func(t []Token) []Token {
		out := make([]Token, 0, len(t))
		for _, k := range t {
			out = append(out, captureTokens(k, res, keepOrig)...)
		}

		return out
	}, nil
}

func captureTokens(k Token, res []*regexp.Regexp, keepOrig bool) []Token {
	var out []Token
	if keepOrig {
		out = append(out, k)
	}

	seen := map[string]bool{}

	for _, re := range res {
		for _, m := range re.FindAllStringSubmatch(k.Term, -1) {
			for _, g := range m[1:] {
				if g == "" || seen[g] || g == k.Term && keepOrig {
					continue
				}

				seen[g] = true

				out = append(out, Token{Term: g, Pos: k.Pos, Start: k.Start, End: k.End})
			}
		}
	}

	return out
}

func stopwordsTokenFilter(name string, o map[string]any) (tokenFilterFn, error) {
	fold := boolOf(o, "ignoreCase", false)
	words := wordList(o, "stopwords", fold)

	if list := strOf(o, "stopwordsList"); list != "" {
		set, ok := stopWords[langCode(list)]
		if !ok {
			return nil, badf("The request is invalid. Details: token filter '%s': stopwordsList '%s' is not "+
				"available.", name, list)
		}

		for w := range set {
			words[w] = true
		}
	} else if len(words) == 0 {
		words = maps.Clone(stopWords["en"])
	}

	trailing := boolOf(o, "removeTrailing", false)

	return func(t []Token) []Token {
		if trailing {
			for len(t) > 0 && words[termKey(t[len(t)-1].Term, fold)] {
				t = t[:len(t)-1]
			}

			return t
		}

		return slices.DeleteFunc(t, func(k Token) bool { return words[termKey(k.Term, fold)] })
	}, nil
}

func langCode(name string) string {
	switch strings.ToLower(name) {
	case langEnglish:
		return "en"
	case langFrench:
		return "fr"
	case langGerman:
		return "de"
	case langSpanish:
		return "es"
	}

	return strings.ToLower(name)
}

func stemmerOverrideFilter(name string, o map[string]any) (tokenFilterFn, error) {
	rules := map[string]string{}

	for _, r := range listOf(o, "rules") {
		s, _ := r.(string)

		from, to, ok := strings.Cut(s, mappingArrow)
		if !ok || strings.TrimSpace(from) == "" {
			return nil, badf("The request is invalid. Details: token filter '%s': rule '%s' must have the form "+
				"'a => b'.", name, s)
		}

		rules[strings.TrimSpace(from)] = strings.TrimSpace(to)
	}

	return func(t []Token) []Token {
		for i := range t {
			if to, ok := rules[t[i].Term]; ok {
				t[i].Term, t[i].Keyword = to, true
			}
		}

		return t
	}, nil
}

func decompounderFilter(name string, o map[string]any) (tokenFilterFn, error) {
	dict := wordList(o, "wordList", true)
	if len(dict) == 0 {
		return nil, badf("The request is invalid. Details: token filter '%s' requires a wordList.", name)
	}

	minWord := intProp(o, "minWordSize", defaultMinWord)
	minSub, maxSub := intProp(o, "minSubwordSize", defaultMinSub), intProp(o, "maxSubwordSize", defaultMaxSub)
	longest := boolOf(o, "onlyLongestMatch", false)

	return func(t []Token) []Token {
		var out []Token

		for _, k := range t {
			out = append(out, k)

			r := []rune(strings.ToLower(k.Term))
			if len(r) < minWord {
				continue
			}

			for _, p := range subwords(r, dict, minSub, maxSub, longest) {
				out = append(out, Token{Term: p, Pos: k.Pos, Start: k.Start, End: k.End})
			}
		}

		return out
	}, nil
}

func subwords(r []rune, dict map[string]bool, minSub, maxSub int, longest bool) []string {
	var parts []string

	for i := 0; i+minSub <= len(r); i++ {
		best := ""

		for n := minSub; n <= min(maxSub, len(r)-i); n++ {
			w := string(r[i : i+n])
			if !dict[w] {
				continue
			}

			if longest {
				best = w
			} else {
				parts = append(parts, w)
			}
		}

		if best != "" {
			parts = append(parts, best)
		}
	}

	return parts
}

type wordDelimiterOpts struct {
	protected                                    map[string]bool
	genWords, genNums, catWords, catNums, catAll bool
	splitCase, splitNum, preserve, possessive    bool
}

func wordDelimiterFilter(o map[string]any) tokenFilterFn {
	opts := wordDelimiterOpts{
		genWords: boolOf(o, "generateWordParts", true), genNums: boolOf(o, "generateNumberParts", true),
		catWords: boolOf(o, "catenateWords", false), catNums: boolOf(o, "catenateNumbers", false),
		catAll: boolOf(o, "catenateAll", false), splitCase: boolOf(o, "splitOnCaseChange", true),
		splitNum: boolOf(o, "splitOnNumerics", true), preserve: boolOf(o, "preserveOriginal", false),
		possessive: boolOf(o, "stemEnglishPossessive", true), protected: wordList(o, "protectedWords", false),
	}

	return func(t []Token) []Token {
		out := make([]Token, 0, len(t))
		for _, k := range t {
			out = append(out, opts.split(k)...)
		}

		return out
	}
}

type subword struct {
	text   string
	number bool
}

func (w wordDelimiterOpts) parts(term string) []subword {
	var parts []subword

	var cur []rune

	curNum := false

	flush := func() {
		if len(cur) > 0 {
			parts = append(parts, subword{string(cur), curNum})
			cur = nil
		}
	}

	rs := []rune(term)
	for i, r := range rs {
		if !unicode.IsLetter(r) && !unicode.IsDigit(r) {
			flush()

			continue
		}

		isNum := unicode.IsDigit(r)
		if len(cur) > 0 {
			prev := rs[i-1]
			switch {
			case w.splitNum && isNum != curNum:
				flush()
			case w.splitCase && unicode.IsLower(prev) && unicode.IsUpper(r):
				flush()
			}
		}

		curNum = isNum
		cur = append(cur, r)
	}

	flush()

	return parts
}

func (w wordDelimiterOpts) split(k Token) []Token {
	if w.protected[k.Term] {
		return []Token{k}
	}

	term := k.Term
	if w.possessive {
		term = strings.TrimSuffix(strings.TrimSuffix(term, "'s"), "’s")
	}

	parts := w.parts(term)

	var out []Token
	if w.preserve || len(parts) == 0 {
		out = append(out, k)
	}

	var words, nums, all []string

	for _, p := range parts {
		all = append(all, p.text)

		if p.number {
			nums = append(nums, p.text)
		} else {
			words = append(words, p.text)
		}

		if (p.number && w.genNums) || (!p.number && w.genWords) {
			out = append(out, partToken(k, p.text))
		}
	}

	for _, c := range []struct {
		parts []string
		on    bool
	}{{words, w.catWords}, {nums, w.catNums}, {all, w.catAll}} {
		if c.on && len(c.parts) > 1 {
			out = append(out, partToken(k, strings.Join(c.parts, "")))
		}
	}

	return out
}

func partToken(k Token, s string) Token {
	return Token{Term: s, Pos: k.Pos, Start: k.Start, End: k.End}
}

func foldRunes(m map[rune]string) func(string) string {
	return func(s string) string {
		var sb strings.Builder

		for _, r := range s {
			if v, ok := m[r]; ok {
				sb.WriteString(v)
			} else {
				sb.WriteRune(r)
			}
		}

		return sb.String()
	}
}

//nolint:gochecknoglobals // read-only folding tables
var (
	arabicNorm = foldRunes(map[rune]string{
		'آ': "ا", 'أ': "ا", 'إ': "ا", 'ى': "ي", 'ة': "ه", 'ـ': "",
		'ً': "", 'ٌ': "", 'ٍ': "", 'َ': "", 'ُ': "", 'ِ': "", 'ّ': "", 'ْ': "",
	})
	persianNorm = foldRunes(map[rune]string{
		'ي': "ی", 'ك': "ک", 'ۀ': "ه", 'ة': "ه", 'ٔ': "",
	})
	scandinavianFold = foldRunes(map[rune]string{
		'å': "a", 'ä': "a", 'æ': "a", 'Å': "A", 'Ä': "A", 'Æ': "A", 'ö': "o", 'ø': "o", 'Ö': "O", 'Ø': "O",
	})
)

func cjkWidth(s string) string {
	var sb strings.Builder

	for _, r := range s {
		if r >= fullwidthFirst && r <= fullwidthLast {
			r = r - fullwidthFirst + asciiFirst
		}

		sb.WriteRune(r)
	}

	return sb.String()
}

func germanNormalize(s string) string {
	rs := []rune(s)

	var sb strings.Builder

	for i := 0; i < len(rs); i++ {
		r := rs[i]
		switch r {
		case 'ä', 'Ä':
			sb.WriteRune('a')
		case 'ö', 'Ö':
			sb.WriteRune('o')
		case 'ü', 'Ü':
			sb.WriteRune('u')
		case 'ß':
			sb.WriteString("ss")
		case 'a', 'o', 'u':
			sb.WriteRune(r)

			if i+1 < len(rs) && rs[i+1] == 'e' {
				i++
			}
		default:
			sb.WriteRune(r)
		}
	}

	return sb.String()
}

func classicFilter(s string) string {
	s = strings.TrimSuffix(strings.TrimSuffix(s, "'s"), "’s")
	if strings.Count(s, ".") > 0 && strings.Trim(s, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ.") == "" {
		return strings.ReplaceAll(s, ".", "")
	}

	return s
}

func defaultElision() tokenFilterFn {
	arts := []any{"l", "m", "t", "qu", "n", "s", "j", "d", "c", "jusqu", "quoi", "lorsqu", "puisqu"}

	return elisionFilter(map[string]any{"articles": arts})
}

// moreBuiltinFilter resolves the built-in token filter names that need no definition.
func moreBuiltinFilter(name string) (tokenFilterFn, bool) {
	lower := strings.ToLower(name)
	if f, ok := moreBuiltinFilters[lower]; ok {
		return f(), true
	}

	if lower == "edgengram_v2" || lower == "ngram_v2" {
		kind := typeNGramFilter
		if strings.HasPrefix(lower, "edge") {
			kind = typeEdgeNGramFilter
		}

		f, _ := gramFilter(name, kind, map[string]any{})

		return f, true
	}

	return nil, false
}

func termFilter(fn func(string) string) func() tokenFilterFn {
	return func() tokenFilterFn { return func(t []Token) []Token { return mapTerms(t, fn) } }
}

//nolint:gochecknoglobals // dispatch table
var moreBuiltinFilters = map[string]func() tokenFilterFn{
	"reverse":               func() tokenFilterFn { return reverseFilter },
	"unique":                func() tokenFilterFn { return uniqueFilter(false) },
	"keyword_repeat":        func() tokenFilterFn { return keywordRepeatFilter },
	"classic":               termFilter(classicFilter),
	"cjk_width":             termFilter(cjkWidth),
	"cjk_bigram":            func() tokenFilterFn { return cjkBigramFilter(map[string]any{}) },
	"german_normalization":  termFilter(germanNormalize),
	"arabic_normalization":  termFilter(arabicNorm),
	"persian_normalization": termFilter(persianNorm),
	"scandinavian_folding":  termFilter(scandinavianFold),
	"word_delimiter":        func() tokenFilterFn { return wordDelimiterFilter(map[string]any{}) },
	"truncate":              func() tokenFilterFn { return truncateFilter(maxAnalysisLength) },
	"shingle":               shingleDefault,
	"common_grams":          func() tokenFilterFn { return commonGramsFilter(map[string]any{}) },
	"elision":               defaultElision,
	"stemmer":               func() tokenFilterFn { return stemFilter(stemmerFunc(porter.Stem)) },
}

// moreBuiltinTokenizer resolves the built-in tokenizer names that need no definition.
func moreBuiltinTokenizer(name string) (Analyzer, bool) {
	switch strings.ToLower(name) {
	case "uax_url_email":
		return uaxTokenizer(defaultTokenLen), true
	case "ngram":
		a, _ := gramTokenizer(name, map[string]any{}, false)

		return a, true
	case "edgengram":
		a, _ := gramTokenizer(name, map[string]any{}, true)

		return a, true
	case "path_hierarchy_v2":
		a, _ := pathTokenizer(map[string]any{})

		return a, true
	}

	return nil, false
}

func shingleDefault() tokenFilterFn {
	f, _ := shingleFilter("shingle", map[string]any{})

	return f
}
