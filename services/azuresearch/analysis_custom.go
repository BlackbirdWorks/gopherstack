package azuresearch

import (
	"strings"
	"unicode"
)

const (
	typeCustomAnalyzer   = "#Microsoft.Azure.Search.CustomAnalyzer"
	typeStandardAnalyzer = "#Microsoft.Azure.Search.StandardAnalyzer"
	typeStopAnalyzer     = "#Microsoft.Azure.Search.StopAnalyzer"
)

func customTokenizer(name string) (Analyzer, bool) {
	switch strings.ToLower(name) {
	case "standard_v2", "classic":
		return func(t string) []Token { return renumber(scan(t, isWordRune, standardJoin)) }, true
	case "whitespace":
		return whitespaceAnalyzer, true
	case "keyword_v2":
		return keywordAnalyzer, true
	case "letter":
		return func(t string) []Token { return renumber(scan(t, isLetterRune, nil)) }, true
	case "lowercase":
		return func(t string) []Token { return renumber(mapTerms(scan(t, isLetterRune, nil), strings.ToLower)) }, true
	}

	return nil, false
}

func customFilter(name string, stops map[string]bool) (func([]Token) []Token, bool) {
	termFn := func(fn func(string) string) func([]Token) []Token {
		return func(t []Token) []Token { return mapTerms(t, fn) }
	}

	switch strings.ToLower(name) {
	case "lowercase":
		return termFn(strings.ToLower), true
	case "uppercase":
		return termFn(strings.ToUpper), true
	case "asciifolding":
		return termFn(foldASCII), true
	case "trim":
		return termFn(strings.TrimSpace), true
	case "apostrophe":
		return termFn(func(s string) string {
			head, _, _ := strings.Cut(strings.ReplaceAll(s, "’", "'"), "'")

			return head
		}), true
	case "porter_stem", "kstem", "snowball":
		return termFn(stemEnglish), true
	case "stopwords":
		return func(t []Token) []Token { return dropStop(t, stops) }, true
	}

	return nil, false
}

// customAnalyzer builds an analyzer from an index-definition analyzer entry.
func customAnalyzer(def map[string]any) (Analyzer, error) {
	name, _ := def["name"].(string)
	kind, _ := def["@odata.type"].(string)
	stops := stopWords["en"]

	if list, ok := def["stopwords"].([]any); ok {
		stops = map[string]bool{}
		for _, w := range list {
			s, _ := w.(string)
			stops[strings.ToLower(s)] = true
		}
	}

	switch kind {
	case typeStandardAnalyzer:
		return standardAnalyzer(stops, false, nil), nil
	case typeStopAnalyzer:
		return simpleAnalyzer(stops), nil
	case typeCustomAnalyzer:
		return pipelineAnalyzer(name, def, stops)
	}

	return nil, wrapf(ErrBadRequest, "analyzer %q has unsupported type %q", name, kind)
}

func pipelineAnalyzer(name string, def map[string]any, stops map[string]bool) (Analyzer, error) {
	tokName, _ := def["tokenizer"].(string)

	tok, ok := customTokenizer(tokName)
	if !ok {
		return nil, wrapf(ErrBadRequest, "analyzer %q uses unsupported tokenizer %q", name, tokName)
	}

	var filters []func([]Token) []Token

	list, _ := def["tokenFilters"].([]any)
	for _, f := range list {
		fname, _ := f.(string)

		fn, found := customFilter(fname, stops)
		if !found {
			return nil, wrapf(ErrBadRequest, "analyzer %q uses unsupported token filter %q", name, fname)
		}

		filters = append(filters, fn)
	}

	if cf, _ := def["charFilters"].([]any); len(cf) > 0 {
		return nil, wrapf(ErrBadRequest, "analyzer %q uses unsupported char filters", name)
	}

	return func(text string) []Token {
		toks := tok(text)
		for _, fn := range filters {
			toks = fn(toks)
		}

		out := toks[:0:0]

		for _, t := range toks {
			if t.Term != "" && !strings.ContainsFunc(t.Term, unicode.IsControl) {
				out = append(out, t)
			}
		}

		return out
	}, nil
}
