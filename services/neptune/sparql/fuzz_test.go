package sparql_test

import (
	"context"
	"testing"
	"time"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

const fuzzSeedData = `INSERT DATA { <http://a> <http://p> "1", 2 . GRAPH <http://g> { <http://b> <http://p> 3 } }`

func fuzzStore(ctx context.Context) *sparql.Store {
	s := sparql.NewStore()
	_, _ = s.Update(ctx, fuzzSeedData, sparql.UpdateOptions{})

	return s
}

func FuzzQuery(f *testing.F) {
	for _, s := range []string{
		"SELECT * WHERE { ?s ?p ?o }",
		"SELECT ?s (COUNT(?o) AS ?c) WHERE { ?s ?p ?o FILTER(?o > 1) } GROUP BY ?s HAVING (?c > 0) LIMIT 2",
		"CONSTRUCT { ?s ?p ?o } WHERE { ?s ?p ?o OPTIONAL { ?o ?q ?r } }",
		"ASK { { ?s ?p ?o } UNION { GRAPH ?g { ?s ?p ?o } } MINUS { ?s ?p 1 } }",
		"DESCRIBE <http://a>",
		"SELECT (STR(?o) AS ?x) WHERE { ?s ?p ?o BIND(REPLACE(STR(?o),'a','b') AS ?z) VALUES ?o { 1 2 } }",
		"SELECT * WHERE { ?s <http://p>+ ?o . ?s (<http://p>|^<http://p>)* ?o }",
	} {
		f.Add(s)
	}

	f.Fuzz(func(_ *testing.T, src string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		defer cancel()

		_, _ = fuzzStore(ctx).Query(ctx, src)
	})
}

func FuzzUpdate(f *testing.F) {
	for _, s := range []string{
		`INSERT DATA { <http://a> <http://p> "x" }`,
		`DELETE WHERE { ?s ?p ?o }`,
		`DELETE { ?s ?p ?o } INSERT { ?s ?p "n" } WHERE { ?s ?p ?o }`,
		`CLEAR ALL; DROP GRAPH <http://g>; CREATE GRAPH <http://h>`,
		`COPY <http://g> TO <http://h>; MOVE DEFAULT TO <http://h>; ADD <http://g> TO DEFAULT`,
		`WITH <http://g> DELETE { ?s ?p ?o } WHERE { ?s ?p ?o }`,
	} {
		f.Add(s)
	}

	f.Fuzz(func(_ *testing.T, src string) {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)

		defer cancel()

		_, _ = fuzzStore(ctx).Update(ctx, src, sparql.UpdateOptions{})
	})
}
