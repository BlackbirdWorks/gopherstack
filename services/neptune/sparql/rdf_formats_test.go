package sparql_test

import (
	"context"
	"encoding/binary"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

func mkQuad(s, p, o, g sparql.Term) sparql.Quad {
	q := sparql.Quad{G: g}
	q.S, q.P, q.O = s, p, o

	return q
}

func extQuads() []sparql.Quad {
	s := sparql.IRI("http://example.org/s")

	return []sparql.Quad{
		mkQuad(s, sparql.IRI(sparql.RDFType), sparql.IRI("http://example.org/T"), sparql.Term{}),
		mkQuad(s, sparql.IRI("http://example.org/p"), sparql.LangLiteral("hi", "en"), sparql.Term{}),
		mkQuad(s, sparql.IRI("http://example.org/n"), sparql.Typed("1", sparql.XSDInteger), sparql.Term{}),
		mkQuad(sparql.Blank("b1"), sparql.IRI("http://example.org/p"), sparql.Literal("x"),
			sparql.IRI("http://example.org/g")),
	}
}

func TestSerializeExtendedFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		want   []string
		format sparql.Format
	}{
		{"jsonld", []string{
			`"@id":"http://example.org/s"`, `"@type":["http://example.org/T"]`,
			`{"@language":"en","@value":"hi"}`,
			`{"@type":"http://www.w3.org/2001/XMLSchema#integer","@value":"1"}`,
			`"@graph":[{"@id":"_:b1"`,
		}, sparql.FormatJSONLD},
		{"trix", []string{
			`<trix xmlns="http://www.w3.org/2004/03/trix/trix-1/">`, `<graph>`,
			`<uri>http://example.org/g</uri>`, `<id>b1</id>`,
			`<plainLiteral xml:lang="en">hi</plainLiteral>`,
			`<typedLiteral datatype="http://www.w3.org/2001/XMLSchema#integer">1</typedLiteral>`,
		}, sparql.FormatTriX},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			out, err := sparql.SerializeQuads(tt.format, extQuads())
			require.NoError(t, err)

			for _, w := range tt.want {
				assert.Contains(t, string(out), w)
			}
		})
	}
}

func TestSerializeRDF4JBinary(t *testing.T) {
	t.Parallel()

	q := mkQuad(sparql.IRI("urn:s"), sparql.IRI("urn:p"), sparql.LangLiteral("a", "en"), sparql.Term{})

	out, err := sparql.SerializeQuads(sparql.FormatRDF4JBinary, []sparql.Quad{q})
	require.NoError(t, err)

	utf16 := func(s string) []byte {
		b := binary.BigEndian.AppendUint32(nil, uint32(len(s)))
		for _, r := range s {
			b = append(b, 0, byte(r))
		}

		return b
	}

	want := append([]byte("BRDF"), 0, 0, 0, 1, 1, 1)
	want = append(want, utf16("urn:s")...)
	want = append(want, 1)
	want = append(want, utf16("urn:p")...)
	want = append(want, 4)
	want = append(want, utf16("a")...)
	want = append(want, utf16("en")...)
	want = append(want, 0, 127)

	assert.Equal(t, want, out)
}

func TestWriteResultBinaryTable(t *testing.T) {
	t.Parallel()

	row := func(x, y sparql.Term) sparql.Binding { return sparql.Binding{"x": x, "y": y} }
	res := &sparql.Result{Kind: sparql.ResultSelect, Vars: []string{"x", "y"}, Rows: []sparql.Binding{
		row(sparql.IRI("urn:a"), sparql.Blank("n")),
		row(sparql.IRI("urn:a"), sparql.Typed("1", sparql.XSDInteger)),
		{"y": sparql.Literal("z")},
	}}

	out, err := sparql.WriteResult(res, sparql.ResultBinary)
	require.NoError(t, err)

	str := func(s string) []byte { return append(binary.BigEndian.AppendUint32(nil, uint32(len(s))), s...) }

	want := append([]byte("BRTR"), 0, 0, 0, 3, 0, 0, 0, 2)
	want = append(want, str("x")...)
	want = append(want, str("y")...)
	want = append(want, 4)
	want = append(want, str("urn:a")...)
	want = append(want, 5)
	want = append(want, str("n")...)
	want = append(want, 1, 8)
	want = append(want, str("1")...)
	want = append(want, 4)
	want = append(want, str(sparql.XSDInteger)...)
	want = append(want, 0, 6)
	want = append(want, str("z")...)
	want = append(want, 127)

	assert.Equal(t, want, out)
}

func TestParseJSONLD(t *testing.T) {
	t.Parallel()

	const (
		ex = "http://example.org/"
		xs = "http://www.w3.org/2001/XMLSchema#"
	)

	tests := []struct {
		name    string
		input   string
		wantErr string
		want    []string
	}{
		{
			name:  "compact",
			input: `{"@context":{"ex":"http://example.org/","name":"ex:name"},"@id":"ex:s","@type":"ex:T","name":"A"}`,
			want: []string{
				`<` + ex + `s> <` + sparql.RDFType + `> <` + ex + `T>`, `<` + ex + `s> <` + ex + `name> "A"`,
			},
		},
		{
			name: "vocab and native",
			input: `{"@context":{"@vocab":"http://example.org/"},"@id":"http://example.org/s",` +
				`"n":7,"d":1.5,"b":true,"l":{"@value":"hi","@language":"EN"}}`,
			want: []string{
				`<` + ex + `n> "7"^^<` + xs + `integer>`, `<` + ex + `d> "1.5E0"^^<` + xs + `double>`,
				`<` + ex + `b> "true"^^<` + xs + `boolean>`, `<` + ex + `l> "hi"@en`,
			},
		},
		{
			name:  "nested node",
			input: `{"@id":"http://example.org/s","http://example.org/p":{"http://example.org/q":"v"}}`,
			want:  []string{`<` + ex + `p> _:`, `<` + ex + `q> "v"`},
		},
		{
			name: "named graph",
			input: `{"@id":"http://example.org/g","@graph":[{"@id":"http://example.org/s",` +
				`"http://example.org/p":{"@id":"http://example.org/o"}}]}`,
			want: []string{`<` + ex + `s> <` + ex + `p> <` + ex + `o> <` + ex + `g>`},
		},
		{
			name:  "list",
			input: `[{"@id":"http://example.org/s","http://example.org/p":{"@list":["a","b"]}}]`,
			want:  []string{sparql.RDFFirst, sparql.RDFRest, sparql.RDFNil},
		},
		{
			name:    "remote context",
			input:   `{"@context":"http://example.org/ctx","@id":"http://example.org/s"}`,
			wantErr: "remote JSON-LD contexts are not supported",
		},
		{name: "not json", input: `{`, wantErr: "invalid JSON-LD"},
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

			var all strings.Builder
			for _, q := range qs {
				all.WriteString(q.S.String() + " " + q.P.String() + " " + q.O.String())

				if q.G.Value != sparql.DefaultGraph {
					all.WriteString(" " + q.G.String())
				}

				all.WriteString("\n")
			}

			for _, w := range tt.want {
				assert.Contains(t, all.String(), w)
			}
		})
	}
}

func TestLoadJSONLDDocument(t *testing.T) {
	t.Parallel()

	store := sparql.NewStore()
	u, err := sparql.ParseUpdate("LOAD <s3://b/doc.jsonld>")
	require.NoError(t, err)

	_, err = store.Apply(t.Context(), u, sparql.UpdateOptions{
		Fetch: func(context.Context, string) ([]byte, error) {
			return []byte(`{"@id":"http://example.org/s","http://example.org/p":"v"}`), nil
		},
	})
	require.NoError(t, err)
	assert.Equal(t, 1, store.Len())
}
