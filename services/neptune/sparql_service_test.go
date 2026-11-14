package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparqlServiceFederation(t *testing.T) {
	t.Parallel()

	_, remote := newEmbeddedBackend(t)
	sparqlUpdate(t, remote, `INSERT DATA {
		<http://example.org/alice> <http://xmlns.com/foaf/0.1/knows> <http://example.org/bob> .
		<http://example.org/bob> <http://xmlns.com/foaf/0.1/name> "Bob"@en }`)

	endpoint := "http://" + clusterAddr(remote) + "/sparql"

	tests := []struct {
		name       string
		query      string
		wantBody   []string
		wantStatus int
	}{
		{name: "join with local data", wantStatus: 200,
			query: `PREFIX foaf: <http://xmlns.com/foaf/0.1/>
				SELECT ?p ?n WHERE { ?p a <http://example.org/Person> .
				SERVICE <` + endpoint + `> { ?p foaf:knows ?f . ?f foaf:name ?n } }`,
			wantBody: []string{`"value":"http://example.org/alice"`, `"value":"Bob"`, `"xml:lang":"en"`}},
		{name: "variable endpoint", wantStatus: 200,
			query: `PREFIX foaf: <http://xmlns.com/foaf/0.1/>
				SELECT ?f WHERE { <http://example.org/ep> <http://example.org/url> ?svc .
				SERVICE ?svc { ?p foaf:knows ?f } }`,
			wantBody: []string{`"value":"http://example.org/bob"`}},
		{name: "silent unreachable", wantStatus: 200,
			query: `SELECT ?p WHERE { ?p a <http://example.org/Person> .
				SERVICE SILENT <http://127.0.0.1:1/sparql> { ?p ?x ?y } }`,
			wantBody: []string{`"value":"http://example.org/alice"`}},
		{name: "unreachable fails", wantStatus: 500,
			query:    `SELECT ?p WHERE { SERVICE <http://127.0.0.1:1/sparql> { ?p ?x ?y } }`,
			wantBody: []string{"SERVICE"}},
		{name: "non http endpoint", wantStatus: 500,
			query:    `SELECT ?p WHERE { SERVICE <ftp://example.org/sparql> { ?p ?x ?y } }`,
			wantBody: []string{"http(s)"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, local := newEmbeddedBackend(t)
			sparqlUpdate(t, local, `INSERT DATA { <http://example.org/alice> a <http://example.org/Person> .
				<http://example.org/ep> <http://example.org/url> <`+endpoint+`> }`)

			res := sparqlDo(t, local, http.MethodGet, "/sparql?"+url.Values{"query": {tt.query}}.Encode(), "", "", "")
			require.Equal(t, tt.wantStatus, res.status, res.body)

			for _, w := range tt.wantBody {
				assert.Contains(t, res.body, w)
			}
		})
	}
}
