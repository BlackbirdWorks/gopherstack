package sparql_test

import (
	"regexp"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

var blankLabel = regexp.MustCompile(`_:[A-Za-z0-9]+`)

func TestParseJSONLDContainersAndKeywords(t *testing.T) {
	t.Parallel()

	const (
		ex = "http://example.org/"
	)

	g := func(s, p, o string) string {
		return "<" + ex + s + "> <" + ex + p + "> " + o + " <" + sparql.DefaultGraph + ">"
	}

	tests := []struct {
		name    string
		input   string
		wantErr string
		want    []string
	}{
		{
			name:  "reverse keyword",
			input: `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `bob","@reverse":{"knows":{"@id":"` + ex + `alice"}}}`,
			want:  []string{g("alice", "knows", "<"+ex+"bob>")},
		},
		{
			name: "reverse term",
			input: `{"@context":{"@vocab":"` + ex + `","parentOf":{"@reverse":"` + ex + `childOf"}},"@id":"` + ex + `p",` +
				`"parentOf":[{"@id":"` + ex + `c1"},{"@id":"` + ex + `c2"}]}`,
			want: []string{g("c1", "childOf", "<"+ex+"p>"), g("c2", "childOf", "<"+ex+"p>")},
		},
		{
			name:    "reverse literal",
			input:   `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `s","@reverse":{"p":"lit"}}`,
			wantErr: "reverse property value must be a node",
		},
		{
			name:  "nest",
			input: `{"@context":{"@vocab":"` + ex + `","details":"@nest"},"@id":"` + ex + `s","details":{"name":"N","age":4}}`,
			want:  []string{g("s", "name", `"N"`), g("s", "age", `"4"^^<http://www.w3.org/2001/XMLSchema#integer>`)},
		},
		{
			name:    "nest with id",
			input:   `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `s","@nest":{"@id":"` + ex + `x"}}`,
			wantErr: "@nest object must not contain @id",
		},
		{
			name:  "index keyword ignored",
			input: `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `s","@index":"i","p":{"@value":"v","@index":"j"}}`,
			want:  []string{g("s", "p", `"v"`)},
		},
		{
			name: "index container",
			input: `{"@context":{"@vocab":"` + ex + `","p":{"@container":"@index"}},"@id":"` + ex + `s",` +
				`"p":{"k1":"a","k2":["b","c"]}}`,
			want: []string{g("s", "p", `"a"`), g("s", "p", `"b"`), g("s", "p", `"c"`)},
		},
		{
			name: "language map",
			input: `{"@context":{"@vocab":"` + ex + `","label":{"@container":"@language"}},"@id":"` + ex + `s",` +
				`"label":{"en":"hi","de":["hallo","moin"],"@none":"plain"}}`,
			want: []string{g("s", "label", `"hi"@en`), g("s", "label", `"hallo"@de`), g("s", "label", `"moin"@de`),
				g("s", "label", `"plain"`)},
		},
		{
			name: "default language",
			input: `{"@context":{"@vocab":"` + ex + `","@language":"fr","en":{"@language":"en"},"raw":{"@language":null}},` +
				`"@id":"` + ex + `s","p":"bonjour","en":"hello","raw":"x"}`,
			want: []string{g("s", "p", `"bonjour"@fr`), g("s", "en", `"hello"@en`), g("s", "raw", `"x"`)},
		},
		{
			name: "id map",
			input: `{"@context":{"@vocab":"` + ex + `","kids":{"@container":"@id"}},"@id":"` + ex + `s",` +
				`"kids":{"` + ex + `a":{"name":"A"},"` + ex + `b":{"@id":"` + ex + `own","name":"B"}}}`,
			want: []string{
				g("s", "kids", "<"+ex+"a>"),
				g("a", "name", `"A"`),
				g("s", "kids", "<"+ex+"own>"),
				g("own", "name", `"B"`),
			},
		},
		{
			name: "type map",
			input: `{"@context":{"@vocab":"` + ex + `","things":{"@container":"@type"}},"@id":"` + ex + `s",` +
				`"things":{"Car":{"@id":"` + ex + `c","name":"Vroom"},"Boat":"` + ex + `b"}}`,
			want: []string{
				g(
					"s",
					"things",
					"<"+ex+"c>",
				),
				"<" + ex + "c> <" + sparql.RDFType + "> <" + ex + "Car> <" + sparql.DefaultGraph + ">",
				g("c", "name", `"Vroom"`),
				g("s", "things", "<"+ex+"b>"),
				"<" + ex + "b> <" + sparql.RDFType + "> <" + ex + "Boat> <" + sparql.DefaultGraph + ">",
			},
		},
		{
			name:  "set container",
			input: `{"@context":{"@vocab":"` + ex + `","tags":{"@container":"@set"}},"@id":"` + ex + `s","tags":["a","b"]}`,
			want:  []string{g("s", "tags", `"a"`), g("s", "tags", `"b"`)},
		},
		{
			name: "graph container",
			input: `{"@context":{"@vocab":"` + ex + `","claims":{"@container":"@graph"}},"@id":"` + ex + `s",` +
				`"claims":{"@id":"` + ex + `fact","p":"v"}}`,
			want: []string{
				g("s", "claims", "_:b"),
				"<" + ex + "fact> <" + ex + "p> \"v\" _:b",
			},
		},
		{
			name: "graph object value",
			input: `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `s",` +
				`"p":{"@id":"` + ex + `named","@graph":{"@id":"` + ex + `n","q":1}}}`,
			want: []string{
				g("s", "p", "<"+ex+"named>"),
				"<" + ex + "n> <" + ex + "q> \"1\"^^<http://www.w3.org/2001/XMLSchema#integer> <" + ex + "named>",
			},
		},
		{
			name: "scoped context",
			input: `{"@context":{"@vocab":"` + ex + `","inner":{"@context":{"@vocab":"http://scoped.example/"}}},` +
				`"@id":"` + ex + `s","inner":{"@id":"` + ex + `i","v":"x"}}`,
			want: []string{
				g("s", "inner", "<"+ex+"i>"),
				"<" + ex + "i> <http://scoped.example/v> \"x\" <" + sparql.DefaultGraph + ">",
			},
		},
		{
			name:    "protected redefinition",
			input:   `{"@context":[{"@protected":true,"p":"` + ex + `p"},{"p":"` + ex + `other"}],"@id":"` + ex + `s","p":"v"}`,
			wantErr: "protected term",
		},
		{
			name:  "protected same definition",
			input: `{"@context":[{"@protected":true,"p":"` + ex + `p"},{"p":"` + ex + `p"}],"@id":"` + ex + `s","p":"v"}`,
			want:  []string{g("s", "p", `"v"`)},
		},
		{
			name:  "keyword alias",
			input: `{"@context":{"id":"@id","type":"@type","@vocab":"` + ex + `"},"id":"` + ex + `s","type":"T","p":"v"}`,
			want: []string{
				g("s", "p", `"v"`),
				"<" + ex + "s> <" + sparql.RDFType + "> <" + ex + "T> <" + sparql.DefaultGraph + ">",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qs, err := sparql.ParseRDF(sparql.FormatJSONLD, []byte(tt.input), sparql.ParseOptions{})
			if tt.wantErr != "" {
				require.ErrorContains(t, err, tt.wantErr)

				return
			}

			require.NoError(t, err)

			var got []string

			for _, q := range qs {
				line := q.S.String() + " " + q.P.String() + " " + q.O.String() + " " + q.G.String()
				got = append(got, blankLabel.ReplaceAllString(line, "_:b"))
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
