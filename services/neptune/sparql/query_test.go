package sparql_test

import (
	"context"
	"sort"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

const fixture = `
@prefix ex: <http://example.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .
ex:alice a foaf:Person ; foaf:name "Alice" ; foaf:age 30 ; foaf:knows ex:bob, ex:carol ; ex:city "Paris"@fr .
ex:bob a foaf:Person ; foaf:name "Bob" ; foaf:age 25 ; foaf:knows ex:carol .
ex:carol a foaf:Person ; foaf:name "Carol" ; foaf:age 35 ; ex:height 1.75 .
ex:dave a foaf:Person ; foaf:name "Dave" .
ex:g1 { ex:alice ex:score 10 . ex:bob ex:score 20 . }
ex:g2 { ex:carol ex:score 30 . ex:alice ex:score 10 . }
`

func newFixtureStore(t *testing.T) *sparql.Store {
	t.Helper()

	qs, err := sparql.ParseRDF(sparql.FormatTriG, []byte(fixture), sparql.ParseOptions{})
	require.NoError(t, err)

	s := sparql.NewStore()
	s.Add(qs)

	return s
}

func rows(r *sparql.Result) []string {
	out := make([]string, 0, len(r.Rows))

	for _, row := range r.Rows {
		parts := make([]string, 0, len(r.Vars))

		for _, v := range r.Vars {
			if t, ok := row[v]; ok {
				parts = append(parts, v+"="+t.String())
			} else {
				parts = append(parts, v+"=-")
			}
		}

		out = append(out, strings.Join(parts, " "))
	}

	return out
}

const pfx = "PREFIX ex: <http://example.org/> PREFIX foaf: <http://xmlns.com/foaf/0.1/> "

func iri(s string) string { return "<http://example.org/" + s + ">" }

func TestSelectQueries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		query   string
		want    []string
		ordered bool
	}{
		{"bgp", `SELECT ?n WHERE { ?p a foaf:Person ; foaf:name ?n }`,
			[]string{`n="Alice"`, `n="Bob"`, `n="Carol"`, `n="Dave"`}, false},
		{"optional", `SELECT ?n ?a WHERE { ?p foaf:name ?n OPTIONAL { ?p foaf:age ?a } } ORDER BY ?n`,
			[]string{`n="Alice" a="30"^^<http://www.w3.org/2001/XMLSchema#integer>`,
				`n="Bob" a="25"^^<http://www.w3.org/2001/XMLSchema#integer>`,
				`n="Carol" a="35"^^<http://www.w3.org/2001/XMLSchema#integer>`, `n="Dave" a=-`}, true},
		{"optional filter bound", `SELECT ?n WHERE { ?p foaf:name ?n OPTIONAL { ?p foaf:age ?a } FILTER(!BOUND(?a)) }`,
			[]string{`n="Dave"`}, false},
		{"filter numeric", `SELECT ?n WHERE { ?p foaf:name ?n ; foaf:age ?a FILTER(?a >= 30) } ORDER BY DESC(?n)`,
			[]string{`n="Carol"`, `n="Alice"`}, true},
		{"filter regex", `SELECT ?n WHERE { ?p foaf:name ?n FILTER regex(?n, "^[ab]", "i") }`,
			[]string{`n="Alice"`, `n="Bob"`}, false},
		{
			"filter string funcs",
			`SELECT ?n WHERE { ?p foaf:name ?n FILTER(STRLEN(?n) = 3 && CONTAINS(LCASE(?n), "o")) }`,
			[]string{`n="Bob"`},
			false,
		},
		{"union", `SELECT ?x WHERE { { ex:alice foaf:knows ?x } UNION { ex:bob foaf:knows ?x } } ORDER BY ?x`,
			[]string{"x=" + iri("bob"), "x=" + iri("carol"), "x=" + iri("carol")}, true},
		{"minus", `SELECT ?p WHERE { ?p a foaf:Person MINUS { ?p foaf:age ?a } }`,
			[]string{"p=" + iri("dave")}, false},
		{"not exists", `SELECT ?p WHERE { ?p a foaf:Person FILTER NOT EXISTS { ?p foaf:knows ?k } } ORDER BY ?p`,
			[]string{"p=" + iri("carol"), "p=" + iri("dave")}, true},
		{"exists", `SELECT ?p WHERE { ?p a foaf:Person FILTER EXISTS { ?p foaf:knows ex:carol } } ORDER BY ?p`,
			[]string{"p=" + iri("alice"), "p=" + iri("bob")}, true},
		{"order limit offset", `SELECT ?n WHERE { ?p foaf:name ?n } ORDER BY ?n LIMIT 2 OFFSET 1`,
			[]string{`n="Bob"`, `n="Carol"`}, true},
		{"distinct", `SELECT DISTINCT ?x WHERE { ?p foaf:knows ?x } ORDER BY ?x`,
			[]string{"x=" + iri("bob"), "x=" + iri("carol")}, true},
		{"bind", `SELECT ?d WHERE { ex:alice foaf:age ?a BIND(?a * 2 AS ?d) }`,
			[]string{`d="60"^^<http://www.w3.org/2001/XMLSchema#integer>`}, false},
		{"values", `SELECT ?n WHERE { VALUES ?p { ex:alice ex:dave } ?p foaf:name ?n } ORDER BY ?n`,
			[]string{`n="Alice"`, `n="Dave"`}, true},
		{"values undef", `SELECT ?p ?q WHERE { VALUES (?p ?q) { (ex:alice UNDEF) (UNDEF ex:bob) } }`,
			[]string{"p=" + iri("alice") + " q=-", "p=- q=" + iri("bob")}, false},
		{"count group", `SELECT ?p (COUNT(?k) AS ?c) WHERE { ?p foaf:knows ?k } GROUP BY ?p ORDER BY ?p`,
			[]string{"p=" + iri("alice") + ` c="2"^^<http://www.w3.org/2001/XMLSchema#integer>`,
				"p=" + iri("bob") + ` c="1"^^<http://www.w3.org/2001/XMLSchema#integer>`}, true},
		{
			"aggregates",
			`SELECT (SUM(?a) AS ?s) (MIN(?a) AS ?mn) (MAX(?a) AS ?mx) (AVG(?a) AS ?av) WHERE { ?p foaf:age ?a }`,
			[]string{
				`s="90"^^<http://www.w3.org/2001/XMLSchema#integer> ` +
					`mn="25"^^<http://www.w3.org/2001/XMLSchema#integer> ` +
					`mx="35"^^<http://www.w3.org/2001/XMLSchema#integer> ` +
					`av="30.0"^^<http://www.w3.org/2001/XMLSchema#decimal>`,
			},
			false,
		},
		{"count star empty", `SELECT (COUNT(*) AS ?c) WHERE { ?s ex:nothing ?o }`,
			[]string{`c="0"^^<http://www.w3.org/2001/XMLSchema#integer>`}, false},
		{
			"group concat sample",
			`SELECT (GROUP_CONCAT(?n; SEPARATOR=",") AS ?g) (COUNT(DISTINCT ?n) AS ?d) ` +
				`WHERE { ?p foaf:name ?n FILTER(?n IN ("Alice","Bob")) }`,
			[]string{`g="Alice,Bob" d="2"^^<http://www.w3.org/2001/XMLSchema#integer>`},
			false,
		},
		{"having", `SELECT ?p WHERE { ?p foaf:knows ?k } GROUP BY ?p HAVING (COUNT(?k) > 1)`,
			[]string{"p=" + iri("alice")}, false},
		{
			"subselect",
			`SELECT ?n WHERE { { SELECT ?p WHERE { ?p foaf:age ?a } ORDER BY DESC(?a) LIMIT 1 } ?p foaf:name ?n }`,
			[]string{`n="Carol"`},
			false,
		},
		{"path sequence", `SELECT ?n WHERE { ex:alice foaf:knows/foaf:name ?n } ORDER BY ?n`,
			[]string{`n="Bob"`, `n="Carol"`}, true},
		{"path alt", `SELECT ?o WHERE { ex:bob (foaf:name|foaf:age) ?o } ORDER BY ?o`,
			[]string{`o="25"^^<http://www.w3.org/2001/XMLSchema#integer>`, `o="Bob"`}, true},
		{"path inverse", `SELECT ?p WHERE { ex:carol ^foaf:knows ?p } ORDER BY ?p`,
			[]string{"p=" + iri("alice"), "p=" + iri("bob")}, true},
		{"path plus", `SELECT ?x WHERE { ex:alice foaf:knows+ ?x } ORDER BY ?x`,
			[]string{"x=" + iri("bob"), "x=" + iri("carol")}, true},
		{"path star", `SELECT ?x WHERE { ex:bob foaf:knows* ?x } ORDER BY ?x`,
			[]string{"x=" + iri("bob"), "x=" + iri("carol")}, true},
		{"path optional", `SELECT ?x WHERE { ex:carol foaf:knows? ?x }`, []string{"x=" + iri("carol")}, false},
		{"path negated", `SELECT ?o WHERE { ex:bob !(rdf:type|foaf:name|foaf:age) ?o }`,
			[]string{"o=" + iri("carol"), `o="20"^^<http://www.w3.org/2001/XMLSchema#integer>`}, false},
		{"graph var", `SELECT ?g ?s WHERE { GRAPH ?g { ?s ex:score ?v } } ORDER BY ?g ?s`,
			[]string{"g=" + iri("g1") + " s=" + iri("alice"), "g=" + iri("g1") + " s=" + iri("bob"),
				"g=" + iri("g2") + " s=" + iri("alice"), "g=" + iri("g2") + " s=" + iri("carol")}, true},
		{"graph const", `SELECT ?s WHERE { GRAPH ex:g2 { ?s ex:score ?v } } ORDER BY ?s`,
			[]string{"s=" + iri("alice"), "s=" + iri("carol")}, true},
		{"default graph is union deduped", `SELECT ?v WHERE { ex:alice ex:score ?v }`,
			[]string{`v="10"^^<http://www.w3.org/2001/XMLSchema#integer>`}, false},
		{"from", `SELECT ?s FROM ex:g1 WHERE { ?s ex:score ?v } ORDER BY ?s`,
			[]string{"s=" + iri("alice"), "s=" + iri("bob")}, true},
		{"from named", `SELECT ?g FROM NAMED ex:g2 WHERE { GRAPH ?g { ?s ex:score ?v } } LIMIT 1`,
			[]string{"g=" + iri("g2")}, true},
		{"lang filter", `SELECT ?c WHERE { ?p ex:city ?c FILTER(LANG(?c) = "fr" && LANGMATCHES(LANG(?c), "fr")) }`,
			[]string{`c="Paris"@fr`}, false},
		{"decimal arithmetic", `SELECT ?h WHERE { ex:carol ex:height ?x BIND(?x + 0.25 AS ?h) }`,
			[]string{`h="2.0"^^<http://www.w3.org/2001/XMLSchema#decimal>`}, false},
		{
			"coalesce if",
			`SELECT ?n (IF(BOUND(?a), "yes", "no") AS ?has) WHERE { ?p foaf:name ?n ` +
				`OPTIONAL { ?p foaf:age ?a } } ORDER BY ?n LIMIT 4`,
			[]string{`n="Alice" has="yes"`, `n="Bob" has="yes"`, `n="Carol" has="yes"`, `n="Dave" has="no"`},
			true,
		},
		{
			"str functions",
			`SELECT (CONCAT(UCASE(SUBSTR(?n,1,1)), SUBSTR(?n,2)) AS ?c) ` +
				`(STRBEFORE("a-b","-") AS ?b) (REPLACE("abc","b","X") AS ?r) WHERE { ex:bob foaf:name ?n }`,
			[]string{`c="Bob" b="a" r="aXc"`},
			false,
		},
		{
			"datatype functions",
			`SELECT (DATATYPE(?a) AS ?d) (ISNUMERIC(?a) AS ?n) (STR(?a) AS ?s) WHERE { ex:bob foaf:age ?a }`,
			[]string{
				`d=<http://www.w3.org/2001/XMLSchema#integer> ` +
					`n="true"^^<http://www.w3.org/2001/XMLSchema#boolean> s="25"`,
			},
			false,
		},
		{"in not in", `SELECT ?n WHERE { ?p foaf:name ?n FILTER(?n NOT IN ("Alice","Bob","Carol")) }`,
			[]string{`n="Dave"`}, false},
		{"predefined prefixes", `SELECT ?p WHERE { ?p rdf:type <http://xmlns.com/foaf/0.1/Person> } LIMIT 1`,
			[]string{"p=" + iri("alice")}, true},
		{"select star", `SELECT * WHERE { ex:bob foaf:age ?a }`,
			[]string{`a="25"^^<http://www.w3.org/2001/XMLSchema#integer>`}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			s := newFixtureStore(t)
			res, err := s.Query(context.Background(), pfx+tt.query)
			require.NoError(t, err)

			got := rows(res)
			if !tt.ordered {
				sort.Strings(got)
				sort.Strings(tt.want)
			}

			assert.Equal(t, tt.want, got)
		})
	}
}

func TestAskConstructDescribe(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name        string
		query       string
		wantTriples int
		wantKind    sparql.ResultKind
		wantBool    bool
	}{
		{name: "ask true", query: `ASK { ex:alice foaf:knows ex:bob }`, wantKind: sparql.ResultAsk, wantBool: true},
		{name: "ask false", query: `ASK { ex:bob foaf:knows ex:alice }`, wantKind: sparql.ResultAsk},
		{
			name:     "construct",
			query:    `CONSTRUCT { ?b ex:knownBy ?a } WHERE { ?a foaf:knows ?b }`,
			wantKind: sparql.ResultGraph, wantTriples: 3,
		},
		{
			name: "construct where", query: `CONSTRUCT WHERE { ex:bob foaf:age ?a }`,
			wantKind: sparql.ResultGraph, wantTriples: 1,
		},
		{name: "describe", query: `DESCRIBE ex:dave`, wantKind: sparql.ResultGraph, wantTriples: 2},
		{
			name: "describe var", query: `DESCRIBE ?p WHERE { ?p foaf:name "Dave" }`,
			wantKind: sparql.ResultGraph, wantTriples: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			res, err := newFixtureStore(t).Query(context.Background(), pfx+tt.query)
			require.NoError(t, err)
			assert.Equal(t, tt.wantKind, res.Kind)
			assert.Len(t, res.Triples, tt.wantTriples)
			assert.Equal(t, tt.wantBool, res.Bool)
		})
	}
}

func TestConstructTemplateInstantiation(t *testing.T) {
	t.Parallel()

	res, err := newFixtureStore(t).Query(context.Background(),
		pfx+`CONSTRUCT { ?b ex:knownBy ?a } WHERE { ?a foaf:knows ?b }`)
	require.NoError(t, err)
	assert.Contains(t, res.Triples, sparql.Triple{
		S: sparql.IRI("http://example.org/bob"), P: sparql.IRI("http://example.org/knownBy"),
		O: sparql.IRI("http://example.org/alice"),
	})
}

func TestQuerySyntaxErrors(t *testing.T) {
	t.Parallel()

	tests := []struct{ name, query string }{
		{"empty", ``},
		{"garbage", `HELLO WORLD`},
		{"unclosed group", `SELECT * WHERE { ?s ?p ?o`},
		{"undefined prefix", `SELECT * WHERE { ?s nope:p ?o }`},
		{"service", `SELECT * WHERE { SERVICE <http://x/> { ?s ?p ?o } }`},
		{"trailing", `SELECT * WHERE { ?s ?p ?o } garbage`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, err := sparql.ParseQuery(tt.query)
			require.Error(t, err)

			var se *sparql.SyntaxError

			assert.ErrorAs(t, err, &se)
		})
	}
}
