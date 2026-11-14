package sparql_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

const describeData = `@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .
@prefix : <https://example.com/> .
:JaneDoe :firstName "Jane" .
:JaneDoe :knows :JohnDoe .
:JohnDoe :firstName "John" .
:JaneDoe :knows _:b1 .
_:b1 :knows :RichardRoe .
:RichardRoe :knows :JaneDoe .
:RichardRoe :firstName "Richard" .
_:s1 rdf:type rdf:Statement .
_:s1 rdf:subject :JaneDoe .
_:s1 rdf:predicate :knows .
_:s1 rdf:object :JohnDoe .
_:s1 :knowsFrom "Berlin" .
:ref_s2 rdf:type rdf:Statement .
:ref_s2 rdf:subject :JaneDoe .
:ref_s2 rdf:predicate :knows .
:ref_s2 rdf:object :JohnDoe .
:ref_s2 :knowsSince 1988 .
`

func TestDescribeHintModes(t *testing.T) {
	t.Parallel()

	const head = "PREFIX hint: <http://aws.amazon.com/neptune/vocab/v01/QueryHints#>\n"

	tests := []struct {
		name  string
		query string
		want  int
	}{
		{"default symmetric", "DESCRIBE <https://example.com/JaneDoe>", 6},
		{
			"forward",
			head + `DESCRIBE <https://example.com/JaneDoe> { hint:Query hint:describeMode "ForwardOneStep" }`,
			3,
		},
		{
			"symmetric",
			head + `DESCRIBE <https://example.com/JaneDoe> { hint:Query hint:describeMode "SymmetricOneStep" }`,
			6,
		},
		{"cbd", head + `DESCRIBE <https://example.com/JaneDoe> { hint:Query hint:describeMode "CBD" }`, 10},
		{"scbd", head + `DESCRIBE <https://example.com/JaneDoe> { hint:Query hint:describeMode "SCBD" }`, 11},
		{"statement limit", head + `DESCRIBE <https://example.com/JaneDoe> {
			hint:Query hint:describeMode "CBD" . hint:Query hint:describeStatementLimit 4 }`, 4},
		{"iteration limit", head + `DESCRIBE <https://example.com/JaneDoe> {
			hint:Query hint:describeMode "CBD" . hint:Query hint:describeIterationLimit 1 }`, 3},
		{"in where clause", head + `DESCRIBE ?s WHERE {
			hint:Query hint:describeMode "ForwardOneStep" . ?s <https://example.com/firstName> "Jane" }`, 3},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			store := sparql.NewStore()
			qs, err := sparql.ParseRDF(sparql.FormatTurtle, []byte(describeData), sparql.ParseOptions{})
			require.NoError(t, err)
			store.Add(qs)

			q, err := sparql.ParseQuery(tt.query)
			require.NoError(t, err)

			res, err := store.Run(t.Context(), q)
			require.NoError(t, err)
			assert.Len(t, res.Triples, tt.want)
		})
	}
}

func TestQueryHintValidation(t *testing.T) {
	t.Parallel()

	const head = "PREFIX hint: <http://aws.amazon.com/neptune/vocab/v01/QueryHints#>\n"

	tests := []struct {
		name    string
		query   string
		wantErr string
		queryID string
		timeout int64
	}{
		{name: "timeout integer", query: head + "SELECT * { hint:Query hint:queryTimeout 10 . ?s ?p ?o }", timeout: 10},
		{
			name:    "timeout string",
			query:   head + `SELECT * { hint:Query hint:queryTimeout "25" . ?s ?p ?o }`,
			timeout: 25,
		},
		{name: "timeout twice", query: head + `SELECT * { hint:Query hint:queryTimeout 1 .
			{ SELECT ?s { hint:Query hint:queryTimeout 2 . ?s ?p ?o } } }`,
			wantErr: "Query hint 'queryTimeout' must be set only once"},
		{name: "timeout in subquery", query: head + `SELECT * { ?s ?p ?o .
			{ SELECT ?s { hint:Query hint:queryTimeout 7 . ?s ?p ?x } } }`, timeout: 7},
		{name: "timeout not a number", query: head + `SELECT * { hint:Query hint:queryTimeout "soon" . ?s ?p ?o }`,
			wantErr: "queryTimeout"},
		{name: "query id", query: head + `SELECT * { hint:Query hint:queryId "abc" . ?s ?p ?o }`, queryID: "abc"},
		{name: "join order ordered", query: head + `SELECT * { hint:Query hint:joinOrder "Ordered" . ?s ?p ?o }`},
		{name: "join order invalid", query: head + `SELECT * { hint:Query hint:joinOrder "Random" . ?s ?p ?o }`,
			wantErr: "joinOrder"},
		{name: "range safe prior", query: head + `SELECT * { ?s ?p ?o . hint:Prior hint:rangeSafe 'true' }`},
		{name: "use dfe", query: head + `SELECT * { hint:Query hint:useDFE true . ?s ?p ?o }`},
		{name: "bottom up subquery", query: head + `SELECT * { { SELECT * WHERE {
			hint:SubQuery hint:evaluationStrategy "BottomUp" . ?s ?p ?o } } }`},
		{name: "bad describe mode", query: head + `DESCRIBE <urn:a> { hint:Query hint:describeMode "Nope" }`,
			wantErr: "describeMode"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			q, err := sparql.ParseQuery(tt.query)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)

				return
			}

			require.NoError(t, err)
			assert.Equal(t, tt.timeout, q.Hints.TimeoutMs)
			assert.Equal(t, tt.queryID, q.Hints.QueryID)

			store := sparql.NewStore()
			res, err := store.Run(t.Context(), q)
			require.NoError(t, err)
			assert.Empty(t, res.Rows)
		})
	}
}
