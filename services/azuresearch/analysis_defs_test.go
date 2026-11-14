package azuresearch_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func analysisIndex(name string) map[string]any {
	return map[string]any{
		"name": name,
		"fields": []any{
			map[string]any{"name": "id", "type": "Edm.String", "key": true},
			map[string]any{"name": "body", "type": "Edm.String", "searchable": true, "analyzer": "csv"},
		},
		"analyzers": []any{
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternAnalyzer", "name": "csv",
				"pattern": ",", "lowercase": false, "stopwords": []any{"skip"},
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternAnalyzer", "name": "defaults",
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.CustomAnalyzer", "name": "pipeline",
				"tokenizer": "keyword_v2", "charFilters": []any{"html_strip", "swap", "digits"},
				"tokenFilters": []any{"cut", "edge"},
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.CustomAnalyzer", "name": "capture",
				"tokenizer": "quoted", "tokenFilters": []any{"len", "elide", "one"},
			},
		},
		"tokenizers": []any{
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternTokenizer", "name": "quoted",
				"pattern": `"([^"]+)"`, "group": 1,
			},
		},
		"charFilters": []any{
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.MappingCharFilter", "name": "swap",
				"mappings": []any{"a=>4", "ab=>X"},
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternReplaceCharFilter", "name": "digits",
				"pattern": `(\d)(\d)`, "replacement": "$2-$1",
			},
		},
		"tokenFilters": []any{
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternReplaceTokenFilter", "name": "cut",
				"pattern": "o+", "replacement": "0",
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.EdgeNGramTokenFilterV2", "name": "edge",
				"minGram": 1, "maxGram": 3, "side": "back",
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.LengthTokenFilter", "name": "len", "min": 2, "max": 7,
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.ElisionTokenFilter", "name": "elide",
				"articles": []any{"l", "d"},
			},
			map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.LimitTokenFilter", "name": "one", "maxTokenCount": 2,
			},
		},
	}
}

func TestCustomAnalysisDefinitions(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")
	s.must(t, http.MethodPost, "/indexes?"+apiVer, analysisIndex("an"), http.StatusCreated)

	tests := []struct {
		body map[string]any
		name string
		want []string
	}{
		{map[string]any{"text": "A,B,skip,,Cd", "analyzer": "csv"}, "pattern config", []string{"A", "B", "Cd"}},
		{
			map[string]any{"text": "Hello, World_1!", "analyzer": "defaults"}, "pattern defaults",
			[]string{"hello", "world_1"},
		},
		{map[string]any{"text": "<b>foo</b> a12", "analyzer": "pipeline"}, "char and token filters",
			[]string{"2", "42", "-42"}},
		{
			map[string]any{"text": `"l'amour" "x" "d’eau" "toolongvalue" "keep"`, "analyzer": "capture"},
			"tokenizer group",
			[]string{"amour", "eau"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			r := s.must(t, http.MethodPost, "/indexes('an')/search.analyze?"+apiVer, tt.body, http.StatusOK)

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

func TestCustomAnalysisValidation(t *testing.T) {
	t.Parallel()

	s := newStack(t, "standard")

	tests := []struct {
		mutate func(map[string]any)
		name   string
	}{
		{
			func(d map[string]any) { d["charFilters"] = []any{map[string]any{"name": "z", "@odata.type": "#nope"}} },
			"char type",
		},
		{func(d map[string]any) {
			d["tokenizers"] = []any{map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternTokenizer", "name": "q", "pattern": "(", "group": 0,
			}}
		}, "bad regex"},
		{func(d map[string]any) {
			d["tokenizers"] = []any{map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.PatternTokenizer", "name": "q", "pattern": "a", "group": 3,
			}}
		}, "group out of range"},
		{func(d map[string]any) {
			d["tokenFilters"] = []any{map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.LengthTokenFilter", "name": "l", "min": 5, "max": 5,
			}}
		}, "length range"},
		{func(d map[string]any) {
			d["charFilters"] = []any{map[string]any{
				"@odata.type": "#Microsoft.Azure.Search.MappingCharFilter", "name": "m", "mappings": []any{"nope"},
			}}
		}, "mapping form"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			def := analysisIndex("bad" + tt.name[:3])
			tt.mutate(def)

			r := s.admin2(t, http.MethodPost, "/indexes?"+apiVer, def)
			require.Equal(t, http.StatusBadRequest, r.status)
		})
	}
}
