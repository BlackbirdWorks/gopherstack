package sparql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

func person(scoped string) string {
	const ex = "http://example.org/"

	return `{"@context":{"@vocab":"` + ex + `","Person":{"@id":"` + ex + `Person","@context":{` + scoped + `}}},`
}

func TestParseJSONLDScopedContexts(t *testing.T) {
	t.Parallel()

	const ex = "http://example.org/"

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
			name: "type scoped context applies to the node",
			input: person(`"nick":"`+ex+`alias"`) +
				`"@id":"` + ex + `a","@type":"Person","nick":"Al"}`,
			want: []string{
				"<" + ex + "a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + ex + "Person> <" + sparql.DefaultGraph + ">",
				g("a", "alias", `"Al"`),
			},
		},
		{
			name: "type scoped context does not propagate",
			input: person(`"nick":"`+ex+`alias"`) +
				`"@id":"` + ex + `a","@type":"Person","knows":{"@id":"` + ex + `b","nick":"B"}}`,
			want: []string{
				"<" + ex + "a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + ex + "Person> <" + sparql.DefaultGraph + ">",
				g("a", "knows", "<"+ex+"b>"),
				g("b", "nick", `"B"`),
			},
		},
		{
			name: "type scoped context propagates when asked",
			input: person(`"@propagate":true,"nick":"`+ex+`alias"`) +
				`"@id":"` + ex + `a","@type":"Person","knows":{"@id":"` + ex + `b","nick":"B"}}`,
			want: []string{
				"<" + ex + "a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <" + ex + "Person> <" + sparql.DefaultGraph + ">",
				g("a", "knows", "<"+ex+"b>"),
				g("b", "alias", `"B"`),
			},
		},
		{
			name: "non propagating local context",
			input: `{"@context":{"@vocab":"` + ex + `","@propagate":false,"nick":"` + ex + `alias"},"@id":"` + ex + `a",` +
				`"nick":"A","knows":{"@id":"` + ex + `b","nick":"B"}}`,
			want: []string{g("a", "alias", `"A"`), g("a", "knows", "<"+ex+"b>")},
		},
		{
			name:  "included blocks",
			input: `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `a","@included":[{"@id":"` + ex + `b","p":"v"}]}`,
			want:  []string{g("b", "p", `"v"`)},
		},
		{
			name:    "included value object",
			input:   `{"@context":{"@vocab":"` + ex + `"},"@id":"` + ex + `a","@included":[{"@value":"x"}]}`,
			wantErr: "@included values must be node objects",
		},
		{
			name:    "import is remote",
			input:   `{"@context":{"@import":"http://remote/ctx","@vocab":"` + ex + `"},"@id":"` + ex + `a","p":"v"}`,
			wantErr: "remote JSON-LD contexts are not supported",
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
				got = append(got, q.S.String()+" "+q.P.String()+" "+q.O.String()+" "+q.G.String())
			}

			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
