package azuresearch_test

import (
	"maps"
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const searchNS = "#Microsoft.Azure.Search."

func def(kind, name string, extra map[string]any) map[string]any {
	m := map[string]any{"@odata.type": searchNS + kind, "name": name}
	maps.Copy(m, extra)

	return m
}

func moreAnalysisIndex() map[string]any {
	return map[string]any{
		"name":   "more",
		"fields": []any{map[string]any{"name": "id", "type": "Edm.String", "key": true}},
		"tokenizers": []any{
			def("NGramTokenizer", "ng", map[string]any{"minGram": 2, "maxGram": 3, "tokenChars": []any{"letter"}}),
			def("EdgeNGramTokenizer", "eg", map[string]any{"minGram": 1, "maxGram": 3}),
			def("PathHierarchyTokenizerV2", "path", nil),
			def("PathHierarchyTokenizerV2", "rpath", map[string]any{"reverse": true}),
			def("ClassicTokenizer", "cl", map[string]any{"maxTokenLength": 4}),
			def("KeywordTokenizerV2", "kw", map[string]any{"maxTokenLength": 3}),
		},
		"tokenFilters": []any{
			def("ShingleTokenFilter", "sh", map[string]any{"outputUnigrams": false}),
			def("TruncateTokenFilter", "tr", map[string]any{"length": 3}),
			def("UniqueTokenFilter", "un", nil),
			def("KeepTokenFilter", "keep", map[string]any{"keepWords": []any{"b", "c"}}),
			def("StemmerTokenFilter", "stem", map[string]any{"language": "porter2"}),
			def("WordDelimiterTokenFilter", "wd", map[string]any{"catenateAll": true}),
			def("CjkBigramTokenFilter", "cjk", nil),
			def("CommonGramTokenFilter", "cg", map[string]any{"commonWords": []any{"the"}}),
			def("PatternCaptureTokenFilter", "cap", map[string]any{"patterns": []any{`(\d+)`}}),
			def("DictionaryDecompounderTokenFilter", "dec", map[string]any{"wordList": []any{"sun", "flower"}}),
			def("StemmerOverrideTokenFilter", "ov", map[string]any{"rules": []any{"ran => run"}}),
			def("StopwordsTokenFilter", "sw", map[string]any{"stopwords": []any{"X"}, "ignoreCase": true}),
		},
	}
}

func TestMoreTokenizersAndFilters(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, moreAnalysisIndex(), http.StatusCreated)

	type analysisCase struct {
		name    string
		text    string
		tok     string
		filters []any
		want    []string
	}

	mk := func(name, text, tok string, filters []any, want []string) analysisCase {
		return analysisCase{name: name, text: text, tok: tok, filters: filters, want: want}
	}

	tests := []analysisCase{
		mk("ngram", "abc de", "ng", nil, []string{"ab", "abc", "bc", "de"}),
		mk("edge ngram", "abcd", "eg", nil, []string{"a", "ab", "abc"}),
		mk("path", "/a/b/c", "path", nil, []string{"/a", "/a/b", "/a/b/c"}),
		mk("reverse path", "a/b/c", "rpath", nil, []string{"a/b/c", "b/c", "c"}),
		mk("classic limit", "abcdefgh ij", "cl", nil, []string{"abcd", "efgh", "ij"}),
		mk("keyword limit", "abcdef", "kw", nil, []string{"abc"}),
		mk("uax email", "mail a@b.com now", "uax_url_email", nil, []string{"mail", "a@b.com", "now"}),
		mk("shingle", "a b c", "whitespace", []any{"sh"}, []string{"a b", "b c"}),
		mk("truncate", "abcdef xy", "whitespace", []any{"tr"}, []string{"abc", "xy"}),
		mk("unique", "a b a", "whitespace", []any{"un"}, []string{"a", "b"}),
		mk("keep", "a b c", "whitespace", []any{"keep"}, []string{"b", "c"}),
		mk("porter2", "running dogs", "whitespace", []any{"stem"}, []string{"run", "dog"}),
		mk("word delimiter", "Wi-Fi4Net", "whitespace", []any{"wd"}, []string{"Wi", "Fi", "4", "Net", "WiFi4Net"}),
		mk("cjk bigram", "日本語", "standard_v2", []any{"cjk"}, []string{"日本", "本語"}),
		mk("common grams", "the cat", "whitespace", []any{"cg"}, []string{"the", "the_cat", "cat"}),
		mk("pattern capture", "ab12", "whitespace", []any{"cap"}, []string{"ab12", "12"}),
		mk("decompounder", "sunflower", "whitespace", []any{"dec"}, []string{"sunflower", "sun", "flower"}),
		mk("stemmer override", "ran runs", "whitespace", []any{"ov", "porter_stem"}, []string{"run", "run"}),
		mk("stopwords custom", "x y", "whitespace", []any{"sw"}, []string{"y"}),
		mk("reverse", "abc", "whitespace", []any{"reverse"}, []string{"cba"}),
		mk(
			"german normalization",
			"Müller Goethe",
			"whitespace",
			[]any{"german_normalization"},
			[]string{"Muller", "Gothe"},
		),
		mk("arabic normalization", "أكل", "whitespace", []any{"arabic_normalization"}, []string{"اكل"}),
		mk("scandinavian folding", "blåbær", "whitespace", []any{"scandinavian_folding"}, []string{"blabar"}),
		mk(
			"keyword repeat",
			"cats",
			"whitespace",
			[]any{"keyword_repeat", "porter_stem", "unique"},
			[]string{"cats", "cat"},
		),
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			body := map[string]any{"text": tt.text, "tokenizer": tt.tok}
			if tt.filters != nil {
				body["tokenFilters"] = tt.filters
			}

			r := s.must(t, http.MethodPost, "/indexes('more')/search.analyze?"+apiVer, body, http.StatusOK)

			var got []string

			toks, _ := r.body["tokens"].([]any)
			for _, tk := range toks {
				m, _ := tk.(map[string]any)
				term, _ := m["token"].(string)
				got = append(got, term)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestMoreAnalysisRejections(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	tests := []struct {
		mut  func(ix map[string]any)
		name string
	}{
		{name: "microsoft tokenizer", mut: func(ix map[string]any) {
			ix["tokenizers"] = []any{def("MicrosoftLanguageTokenizer", "ms", nil)}
		}},
		{name: "unsupported stemmer language", mut: func(ix map[string]any) {
			ix["tokenFilters"] = []any{def("StemmerTokenFilter", "x", map[string]any{"language": "lightFrench"})}
		}},
		{name: "shingle sizes", mut: func(ix map[string]any) {
			ix["tokenFilters"] = []any{def("ShingleTokenFilter", "x", map[string]any{"maxShingleSize": 1})}
		}},
		{name: "ngram bounds", mut: func(ix map[string]any) {
			ix["tokenizers"] = []any{def("NGramTokenizer", "x", map[string]any{"minGram": 3, "maxGram": 2})}
		}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			ix := moreAnalysisIndex()
			ix["tokenizers"], ix["tokenFilters"] = nil, nil
			tt.mut(ix)

			r := s.admin2(t, http.MethodPost, "/indexes?"+apiVer, ix)
			require.Equal(t, http.StatusBadRequest, r.status, string(r.raw))
		})
	}
}

func TestLanguageAnalyzersFollowLucene(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, moreAnalysisIndex(), http.StatusCreated)

	tests := []struct {
		name     string
		analyzer string
		text     string
		want     []string
	}{
		{
			"english porter",
			"en.lucene",
			"The running generalizations of John's dogs",
			[]string{"run", "gener", "john", "dog"},
		},
		{"french elision stem", "fr.lucene", "l'amour des chevaux", []string{"amour", "cheval"}},
		{"german stem", "de.lucene", "Häuser und Kinder", []string{"haus", "kind"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := s.must(t, http.MethodPost, "/indexes('more')/search.analyze?"+apiVer,
				map[string]any{"text": tt.text, "analyzer": tt.analyzer}, http.StatusOK)

			var got []string

			toks, _ := r.body["tokens"].([]any)
			for _, tk := range toks {
				m, _ := tk.(map[string]any)
				term, _ := m["token"].(string)
				got = append(got, term)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}
