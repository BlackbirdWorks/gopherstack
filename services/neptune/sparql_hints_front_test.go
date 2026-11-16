package neptune_test

import (
	"encoding/json/v2"
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestSparqlQueryHints(t *testing.T) {
	t.Parallel()

	const head = "PREFIX hint: <http://aws.amazon.com/neptune/vocab/v01/QueryHints#>\n"

	tests := []struct {
		name       string
		query      string
		wantCode   string
		wantMsg    string
		wantBody   string
		wantStatus int
	}{
		{name: "hint does not change results", wantStatus: 200, wantBody: `"value":"v"`,
			query: head + `SELECT ?o { hint:Query hint:joinOrder "Ordered" . hint:Query hint:queryTimeout 5000 .
				?s <http://example.org/p> ?o }`},
		{name: "timeout at parameter limit", wantStatus: 400, wantCode: "MalformedQueryException",
			wantMsg: "Query hint 'queryTimeout' must be less than neptune_query_timeout DB Parameter Group",
			query:   head + `SELECT * { hint:Query hint:queryTimeout 120000 . ?s ?p ?o }`},
		{name: "timeout set twice", wantStatus: 400, wantCode: "MalformedQueryException",
			wantMsg: "Query hint 'queryTimeout' must be set only once",
			query: head + `SELECT * { hint:Query hint:queryTimeout 10 .
				{ SELECT ?s { hint:Query hint:queryTimeout 20 . ?s ?p ?o } } }`},
		{name: "describe forward", wantStatus: 200, wantBody: "example.org/p",
			query: head + `DESCRIBE <http://example.org/s> { hint:Query hint:describeMode "ForwardOneStep" }`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)
			sparqlUpdate(t, c, `INSERT DATA { <http://example.org/s> <http://example.org/p> "v" }`)

			res := sparqlDo(t, c, http.MethodGet, "/sparql?"+url.Values{"query": {tt.query}}.Encode(), "", "", "")
			require.Equal(t, tt.wantStatus, res.status, res.body)

			if tt.wantCode != "" {
				var e map[string]string
				require.NoError(t, json.Unmarshal([]byte(res.body), &e, jsonx.V1()), res.body)
				assert.Equal(t, tt.wantCode, e["code"])
				assert.Contains(t, e["detailedMessage"], tt.wantMsg)
			}

			assert.Contains(t, res.body, tt.wantBody)
		})
	}
}

func TestSparqlNumericStorageLimits(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		literal  string
		wantCode string
		status   int
	}{
		{name: "decimal within 64 bits", literal: "3.14159265358979", status: 200},
		{name: "decimal beyond 64 bits", literal: "0.123456789012345678901234", status: 400,
			wantCode: "InvalidNumericDataException"},
		{name: "double within range", literal: "1.5e300", status: 200},
		{name: "double beyond range", literal: "1e999", status: 400, wantCode: "InvalidNumericDataException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)
			res := sparqlDo(t, c, http.MethodGet, "/sparql?"+url.Values{
				"query": {"SELECT ?s WHERE { ?s ?p " + tt.literal + " }"},
			}.Encode(), "", "", "")
			require.Equal(t, tt.status, res.status, res.body)

			if tt.wantCode != "" {
				assert.Contains(t, res.body, tt.wantCode)
			}
		})
	}
}
