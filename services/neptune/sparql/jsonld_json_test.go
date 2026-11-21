package sparql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

func TestParseJSONLDJSONAndDirection(t *testing.T) {
	t.Parallel()

	const ex = "http://example.org/"

	ctx := `"@context":{"@vocab":"` + ex + `","j":{"@id":"` + ex + `j","@type":"@json"}},"@id":"` + ex + `s",`
	jsonDT := sparql.NSRDF + "JSON"

	tests := []struct {
		name     string
		input    string
		wantErr  string
		wantVal  string
		wantDT   string
		wantLang string
	}{
		{
			name:    "value object sorted keys",
			input:   `{` + ctx + `"p":{"@value":{"b":1,"a":[true,null,"x"]},"@type":"@json"}}`,
			wantVal: `{"a":[true,null,"x"],"b":1}`, wantDT: jsonDT,
		},
		{
			name:    "term typed json keeps array whole",
			input:   `{` + ctx + `"j":[3,2,1]}`,
			wantVal: `[3,2,1]`, wantDT: jsonDT,
		},
		{
			name:    "term typed json scalar string escaping",
			input:   `{` + ctx + `"j":"a\"b\\c\n\u0001é/"}`,
			wantVal: `"a\"b\\c\n\u0001é/"`, wantDT: jsonDT,
		},
		{
			name:    "utf16 key order",
			input:   `{` + ctx + `"j":{"😀":1,"￮":2,"a":3}}`,
			wantVal: "{\"a\":3,\"\U0001F600\":1,\"￮\":2}", wantDT: jsonDT,
		},
		{
			name:    "number formats",
			input:   `{` + ctx + `"j":[1.0,-0,1e21,1e-7,0.000001,1E+2,123456789012345680000,4.5e-3,1e400]}`,
			wantErr: "IEEE 754",
		},
		{
			name:    "number formats ok",
			input:   `{` + ctx + `"j":[1.0,-0,1e21,1e-7,0.000001,1E+2,123456789012345680000,4.5e-3,333333333.33333329,1e1]}`,
			wantVal: `[1,0,1e+21,1e-7,0.000001,100,123456789012345680000,0.0045,333333333.3333333,10]`,
			wantDT:  jsonDT,
		},
		{
			name:    "json null value",
			input:   `{` + ctx + `"p":{"@value":null,"@type":"@json"}}`,
			wantVal: `null`, wantDT: jsonDT,
		},
		{
			name:     "direction dropped keeps language",
			input:    `{` + ctx + `"p":{"@value":"hi","@language":"en","@direction":"rtl"}}`,
			wantVal:  "hi",
			wantLang: "en",
		},
		{
			name:    "direction only is plain literal",
			input:   `{` + ctx + `"p":{"@value":"hi","@direction":"ltr"}}`,
			wantVal: "hi",
		},
		{
			name:    "null direction ok",
			input:   `{` + ctx + `"p":{"@value":"hi","@direction":null}}`,
			wantVal: "hi",
		},
		{
			name:    "json with language",
			input:   `{` + ctx + `"p":{"@value":"x","@type":"@json","@language":"en"}}`,
			wantErr: "invalid value object",
		},
		{
			name:    "json with direction",
			input:   `{` + ctx + `"p":{"@value":"x","@type":"@json","@direction":"ltr"}}`,
			wantErr: "invalid value object",
		},
		{
			name:    "typed with language",
			input:   `{` + ctx + `"p":{"@value":"x","@type":"http://x/y","@language":"en"}}`,
			wantErr: "invalid value object",
		},
		{
			name:    "bad direction",
			input:   `{` + ctx + `"p":{"@value":"hi","@direction":"up"}}`,
			wantErr: "invalid base direction",
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
			require.Len(t, qs, 1)
			assert.Equal(t, tt.wantVal, qs[0].O.Value)
			assert.Equal(t, tt.wantDT, qs[0].O.Datatype)
			assert.Equal(t, tt.wantLang, qs[0].O.Lang)
		})
	}
}

func TestParseJSONLDJSONListContainer(t *testing.T) {
	t.Parallel()

	const ex = "http://example.org/"

	in := `{"@context":{"jl":{"@id":"` + ex + `jl","@type":"@json","@container":"@list"}},"@id":"` + ex + `s","jl":[1,2]}`

	qs, err := sparql.ParseRDF(sparql.FormatJSONLD, []byte(in), sparql.ParseOptions{})
	require.NoError(t, err)

	var firsts []string

	for _, q := range qs {
		if q.P.Value == sparql.RDFFirst {
			firsts = append(firsts, q.O.Value)
			assert.Equal(t, sparql.NSRDF+"JSON", q.O.Datatype)
		}
	}

	assert.Equal(t, []string{"[1,2]"}, firsts)
}
