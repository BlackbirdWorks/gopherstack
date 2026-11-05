package sparql_test

import (
	"context"
	"encoding/json"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

func TestParseRDF(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format sparql.Format
		input  string
		want   []string
	}{
		{"ntriples", sparql.FormatNTriples, "<urn:s> <urn:p> \"a\\nb\"@EN .\n_:b <urn:p> <urn:o> .\n# c\n",
			[]string{`<urn:s> <urn:p> "a\nb"@en`}},
		{"ntriples typed", sparql.FormatNTriples, `<urn:s> <urn:p> "1"^^<http://www.w3.org/2001/XMLSchema#integer> .`,
			[]string{`<urn:s> <urn:p> "1"^^<http://www.w3.org/2001/XMLSchema#integer>`}},
		{"nquads graph", sparql.FormatNQuads, `<urn:s> <urn:p> <urn:o> <urn:g> .`,
			[]string{`<urn:s> <urn:p> <urn:o> <urn:g>`}},
		{"turtle lists", sparql.FormatTurtle, `@prefix : <urn:> . :a :p ( 1 2 ) .`,
			[]string{`<urn:a> <urn:p>`, `rdf-syntax-ns#first> "1"`, `rdf-syntax-ns#rest> <http://www.w3.org/1999/02/22-rdf-syntax-ns#nil>`}},
		{"turtle sugar", sparql.FormatTurtle, "PREFIX : <urn:>\nBASE <http://b/>\n:a a :T ; :n 1.5, -2, 3e2, true ; :rel <x> ; :s '''long\nstring''' .",
			[]string{`<urn:a> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <urn:T>`, `"1.5"^^<http://www.w3.org/2001/XMLSchema#decimal>`,
				`"-2"^^<http://www.w3.org/2001/XMLSchema#integer>`, `"3e2"^^<http://www.w3.org/2001/XMLSchema#double>`,
				`"true"^^<http://www.w3.org/2001/XMLSchema#boolean>`, `<http://b/x>`, `"long\nstring"`}},
		{"trig", sparql.FormatTriG, `@prefix : <urn:> . :g { :a :p :b } GRAPH :h { :c :p :d } :e :p :f .`,
			[]string{`<urn:a> <urn:p> <urn:b> <urn:g>`, `<urn:c> <urn:p> <urn:d> <urn:h>`,
				`<urn:e> <urn:p> <urn:f> <http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph>`}},
		{"rdfxml", sparql.FormatRDFXML, `<rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://ex/" xml:base="http://base/">
<ex:Thing rdf:ID="t1" ex:attr="v"><ex:name xml:lang="en">N</ex:name><ex:n rdf:datatype="http://www.w3.org/2001/XMLSchema#integer">5</ex:n>
<ex:child><ex:Thing rdf:about="c"/></ex:child><ex:ref rdf:parseType="Resource"><ex:x>1</ex:x></ex:ref>
<ex:list rdf:parseType="Collection"><rdf:Description rdf:about="m1"/></ex:list></ex:Thing></rdf:RDF>`,
			[]string{`<http://base/#t1> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://ex/Thing>`,
				`<http://ex/attr> "v"`, `<http://ex/name> "N"@en`, `<http://ex/n> "5"^^<http://www.w3.org/2001/XMLSchema#integer>`,
				`<http://ex/child> <http://base/c>`, `<http://ex/x> "1"`, `<http://base/m1>`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qs, err := sparql.ParseRDF(tt.format, []byte(tt.input), sparql.ParseOptions{})
			require.NoError(t, err)

			out, err := sparql.SerializeQuads(sparql.FormatNQuads, qs)
			require.NoError(t, err)

			for _, w := range tt.want {
				assert.Contains(t, string(out), w)
			}
		})
	}
}

func TestParseRDFErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name       string
		format     sparql.Format
		input      string
		wantRecord int
		wantKept   int
	}{
		{"ntriples bad line", sparql.FormatNTriples, "<urn:s> <urn:p> <urn:o> .\n<urn:s> |urn:p> <urn:o> .\n", 2, 1},
		{"turtle undefined prefix", sparql.FormatTurtle, "ex:a ex:p ex:b .", 1, 0},
		{"nquads missing dot", sparql.FormatNQuads, "<urn:s> <urn:p> <urn:o>\n", 1, 0},
		{"rdfxml not xml", sparql.FormatRDFXML, "<rdf:RDF", 0, 0},
		{"literal subject", sparql.FormatNTriples, `"x" <urn:p> <urn:o> .`, 1, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			qs, err := sparql.ParseRDF(tt.format, []byte(tt.input), sparql.ParseOptions{})
			require.Error(t, err)

			var pe *sparql.ParseError

			require.ErrorAs(t, err, &pe)
			assert.Equal(t, tt.wantRecord, pe.Record)
			assert.Len(t, qs, tt.wantKept)
		})
	}
}

func TestParseRDFLenientSkipsBadLines(t *testing.T) {
	t.Parallel()

	qs, errs := sparql.ParseRDFLenient(sparql.FormatNTriples,
		[]byte("<urn:a> <urn:p> <urn:o> .\nbad\n<urn:b> <urn:p> <urn:o> .\nworse\n"), sparql.ParseOptions{}, true)
	assert.Len(t, qs, 2)
	require.Len(t, errs, 2)
	assert.Equal(t, 2, errs[0].Record)
	assert.Equal(t, 4, errs[1].Record)
}

func TestSerializeRoundTrip(t *testing.T) {
	t.Parallel()

	src := `@prefix : <urn:> . :a :p "x"@en, 5, true ; :q :b . :b a :T .`
	qs, err := sparql.ParseRDF(sparql.FormatTurtle, []byte(src), sparql.ParseOptions{})
	require.NoError(t, err)

	for _, f := range []sparql.Format{
		sparql.FormatNTriples, sparql.FormatNQuads, sparql.FormatTurtle, sparql.FormatTriG, sparql.FormatRDFXML,
	} {
		out, serr := sparql.SerializeQuads(f, qs)
		require.NoError(t, serr, f)

		back, perr := sparql.ParseRDF(f, out, sparql.ParseOptions{})
		require.NoError(t, perr, string(out))
		assert.Len(t, back, len(qs), string(out))
	}
}

func TestExportImport(t *testing.T) {
	t.Parallel()

	s := newFixtureStore(t)
	data := s.Export()

	s2 := sparql.NewStore()
	require.NoError(t, s2.Import(data))
	assert.Equal(t, s.Len(), s2.Len())
	assert.Equal(t, string(data), string(s2.Export()))
}

func TestResultFormats(t *testing.T) {
	t.Parallel()

	q := pfx + `SELECT ?s ?v ?l WHERE { VALUES (?v ?l) { ("x,y" "q\"q"@en) (5 UNDEF) } BIND(IF(?v = 5, BNODE(), ex:a) AS ?s) }`

	tests := []struct {
		name   string
		format sparql.ResultFormat
		want   []string
	}{
		{"json", sparql.ResultJSON, []string{`"vars":["s","v","l"]`, `"xml:lang":"en"`, `"type":"bnode"`,
			`"datatype":"http://www.w3.org/2001/XMLSchema#integer"`}},
		{"xml", sparql.ResultXML, []string{`<variable name="s"/>`, `<literal xml:lang="en">`, "<uri>http://example.org/a</uri>", "<bnode>"}},
		{"csv", sparql.ResultCSV, []string{"s,v,l\r\n", `"x,y"`, `"q""q"`, "_:"}},
		{"tsv", sparql.ResultTSV, []string{"?s\t?v\t?l\r\n", `"x,y"`, `"q\"q"@en`}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := sparql.NewStore().Query(context.Background(), q)
			require.NoError(t, err)

			out, err := sparql.WriteResult(res, tt.format)
			require.NoError(t, err)

			for _, w := range tt.want {
				assert.Contains(t, string(out), w)
			}

			if tt.format == sparql.ResultJSON {
				assert.True(t, json.Valid(out))
			}
		})
	}
}

func TestAskFormats(t *testing.T) {
	t.Parallel()

	res := &sparql.Result{Kind: sparql.ResultAsk, Bool: true}

	for f, want := range map[sparql.ResultFormat]string{
		sparql.ResultJSON: `"boolean":true`, sparql.ResultXML: "<boolean>true</boolean>", sparql.ResultBoolean: "true",
	} {
		out, err := sparql.WriteResult(res, f)
		require.NoError(t, err)
		assert.True(t, strings.Contains(string(out), want), string(out))
	}
}
