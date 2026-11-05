package neptune_test

import (
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/testclient"
	"github.com/blackbirdworks/gopherstack/services/neptune"
)

const (
	formCT   = "application/x-www-form-urlencoded"
	exampleT = "<http://example.org/s> <http://example.org/p> <http://example.org/o> ."
)

type httpResult struct {
	header http.Header
	body   string
	status int
}

func sparqlDo(t *testing.T, c neptune.DBCluster, method, path, contentType, accept, body string) httpResult {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+clusterAddr(c)+path, strings.NewReader(body))
	require.NoError(t, err)

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if accept != "" {
		req.Header.Set("Accept", accept)
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return httpResult{status: resp.StatusCode, header: resp.Header, body: string(raw)}
}

func sparqlUpdate(t *testing.T, c neptune.DBCluster, update string) {
	t.Helper()

	res := sparqlDo(t, c, http.MethodPost, "/sparql", formCT, "", url.Values{"update": {update}}.Encode())
	require.Equal(t, http.StatusOK, res.status, res.body)
}

func TestSparqlProtocol(t *testing.T) {
	t.Parallel()

	const insert = `INSERT DATA { <http://example.org/s> <http://example.org/p> "v" . ` +
		`GRAPH <http://example.org/g> { <http://example.org/s> <http://example.org/q> 1 } }`

	tests := []struct {
		name        string
		method      string
		path        string
		contentType string
		accept      string
		body        string
		wantStatus  int
		wantType    string
		wantBody    string
		wantCode    string
	}{
		{name: "form query json default", method: "POST", path: "/sparql", contentType: formCT,
			body:       url.Values{"query": {"SELECT ?o WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "application/sparql-results+json", wantBody: `"value":"v"`},
		{name: "direct sparql-query", method: "POST", path: "/sparql", contentType: "application/sparql-query",
			body:       "SELECT ?o WHERE { ?s <http://example.org/p> ?o }",
			wantStatus: 200, wantType: "application/sparql-results+json", wantBody: `"bindings"`},
		{name: "get query xml", method: "GET", accept: "application/sparql-results+xml",
			path:       "/sparql?" + url.Values{"query": {"SELECT ?o WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "application/sparql-results+xml", wantBody: "<literal>v</literal>"},
		{name: "csv", method: "GET", accept: "text/csv",
			path:       "/sparql?" + url.Values{"query": {"SELECT ?o WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "text/csv", wantBody: "o\r\nv\r\n"},
		{name: "tsv", method: "GET", accept: "text/tab-separated-values",
			path:       "/sparql?" + url.Values{"query": {"SELECT ?o WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "text/tab-separated-values", wantBody: "?o\r\n\"v\"\r\n"},
		{name: "ask json", method: "GET",
			path:       "/sparql?" + url.Values{"query": {"ASK { ?s ?p ?o }"}}.Encode(),
			wantStatus: 200, wantBody: `"boolean":true`},
		{name: "ask boolean text", method: "GET", accept: "text/boolean",
			path:       "/sparql?" + url.Values{"query": {"ASK { ?s ?p ?o }"}}.Encode(),
			wantStatus: 200, wantType: "text/boolean", wantBody: "true"},
		{name: "construct default nquads", method: "GET",
			path:       "/sparql?" + url.Values{"query": {"CONSTRUCT { ?s <http://example.org/z> ?o } WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "application/n-quads", wantBody: `<http://example.org/s> <http://example.org/z> "v" .`},
		{name: "construct turtle", method: "GET", accept: "text/turtle",
			path:       "/sparql?" + url.Values{"query": {"CONSTRUCT WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "text/turtle", wantBody: `<http://example.org/p> "v" .`},
		{name: "construct rdfxml", method: "GET", accept: "application/rdf+xml",
			path:       "/sparql?" + url.Values{"query": {"CONSTRUCT WHERE { ?s <http://example.org/p> ?o }"}}.Encode(),
			wantStatus: 200, wantType: "application/rdf+xml", wantBody: "rdf:Description"},
		{name: "graph query via default-graph-uri", method: "GET",
			path: "/sparql?" + url.Values{
				"query":             {"SELECT ?o WHERE { ?s ?p ?o }"},
				"default-graph-uri": {"http://example.org/g"},
			}.Encode(),
			wantStatus: 200, wantBody: `"value":"1"`},
		{name: "explain static", method: "GET",
			path:       "/sparql?" + url.Values{"query": {"SELECT * WHERE { ?s ?p ?o }"}, "explain": {"static"}}.Encode(),
			wantStatus: 200, wantType: "text/html;charset=UTF-8", wantBody: "TriplePattern"},
		{name: "malformed query", method: "GET", path: "/sparql?" + url.Values{"query": {"SELEKT"}}.Encode(),
			wantStatus: 400, wantCode: "MalformedQueryException"},
		{name: "missing parameter", method: "POST", path: "/sparql", contentType: formCT, body: "",
			wantStatus: 400, wantCode: "MissingParameterException"},
		{name: "both query and update", method: "POST", path: "/sparql", contentType: formCT,
			body:       url.Values{"query": {"ASK {}"}, "update": {"CLEAR ALL"}}.Encode(),
			wantStatus: 400, wantCode: "BadRequestException"},
		{name: "unsupported content type", method: "POST", path: "/sparql", contentType: "text/plain", body: "x",
			wantStatus: 415, wantCode: "UnsupportedMediaTypeException"},
		{name: "unacceptable", method: "GET", accept: "image/png",
			path:       "/sparql?" + url.Values{"query": {"ASK {}"}}.Encode(),
			wantStatus: 406, wantCode: "UnsupportedMediaTypeException"},
		{name: "update via get rejected", method: "GET", path: "/sparql?" + url.Values{"update": {"CLEAR ALL"}}.Encode(),
			wantStatus: 405, wantCode: "BadRequestException"},
		{name: "bad method", method: "DELETE", path: "/sparql", wantStatus: 405, wantCode: "BadRequestException"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)
			sparqlUpdate(t, c, insert)

			res := sparqlDo(t, c, tt.method, tt.path, tt.contentType, tt.accept, tt.body)
			require.Equal(t, tt.wantStatus, res.status, res.body)

			if tt.wantType != "" {
				assert.Equal(t, tt.wantType, strings.TrimSuffix(res.header.Get("Content-Type"), ";charset=UTF-8")+
					map[bool]string{true: ";charset=UTF-8", false: ""}[strings.HasSuffix(tt.wantType, "UTF-8")])
			}

			if tt.wantCode != "" {
				var e map[string]string
				require.NoError(t, json.Unmarshal([]byte(res.body), &e), res.body)
				assert.Equal(t, tt.wantCode, e["code"])
				assert.NotEmpty(t, e["detailedMessage"])
				assert.NotEmpty(t, e["requestId"])
			}

			assert.Contains(t, res.body, tt.wantBody)
		})
	}
}

func TestSparqlUpdateRoundTrip(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		updates []string
		query   string
		want    []string
		notWant []string
	}{
		{"insert delete data", []string{
			"INSERT DATA { <urn:a> <urn:p> 1, 2 }", "DELETE DATA { <urn:a> <urn:p> 1 }",
		}, "SELECT ?o WHERE { <urn:a> <urn:p> ?o }", []string{`"value":"2"`}, []string{`"value":"1"`}},
		{"delete insert where", []string{
			"INSERT DATA { <urn:a> <urn:p> 1 }",
			"DELETE { ?s <urn:p> ?o } INSERT { ?s <urn:p> 9 } WHERE { ?s <urn:p> ?o }",
		}, "SELECT ?o WHERE { <urn:a> <urn:p> ?o }", []string{`"value":"9"`}, []string{`"value":"1"`}},
		{"relative iri uses neptune default base", []string{"INSERT DATA { <node1> <id> \"n1\" }"},
			"SELECT ?p WHERE { <http://aws.amazon.com/neptune/default/node1> ?p ?o }",
			[]string{"http://aws.amazon.com/neptune/default/id"}, nil},
		{"named graph insert and drop", []string{
			"INSERT DATA { GRAPH <urn:g> { <urn:a> <urn:p> 1 } }", "DROP GRAPH <urn:g>",
		}, "SELECT * WHERE { ?s ?p ?o }", nil, []string{"urn:a"}},
		{"default graph uses neptune vocab", []string{"INSERT DATA { <urn:a> <urn:p> 1 }"},
			"SELECT ?g WHERE { GRAPH ?g { ?s ?p ?o } }",
			[]string{"http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph"}, nil},
		{"clear all", []string{"INSERT DATA { <urn:a> <urn:p> 1 }", "CLEAR ALL"},
			"SELECT * WHERE { ?s ?p ?o }", nil, []string{"urn:a"}},
		{"copy graph", []string{
			"INSERT DATA { GRAPH <urn:g1> { <urn:a> <urn:p> 1 } }", "COPY <urn:g1> TO <urn:g2>",
		}, "SELECT ?g WHERE { GRAPH ?g { <urn:a> <urn:p> 1 } }", []string{"urn:g1", "urn:g2"}, nil},
		{"multi operation request", []string{
			"INSERT DATA { <urn:a> <urn:p> 1 } ; INSERT DATA { <urn:b> <urn:p> 2 }",
		}, "SELECT ?s WHERE { ?s <urn:p> ?o }", []string{"urn:a", "urn:b"}, nil},
		{"failed request rolls back", nil, "SELECT * WHERE { ?s ?p ?o }", nil, []string{"urn:"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)

			for _, u := range tt.updates {
				sparqlUpdate(t, c, u)
			}

			if tt.name == "failed request rolls back" {
				res := sparqlDo(t, c, http.MethodPost, "/sparql", formCT, "",
					url.Values{"update": {"INSERT DATA { <urn:x> <urn:p> 1 } ; LOAD <s3://nope/missing.nt>"}}.Encode())
				assert.GreaterOrEqual(t, res.status, 400)
			}

			res := sparqlDo(t, c, http.MethodPost, "/sparql", "application/sparql-query", "", tt.query)
			require.Equal(t, http.StatusOK, res.status, res.body)

			for _, w := range tt.want {
				assert.Contains(t, res.body, w)
			}

			for _, n := range tt.notWant {
				assert.NotContains(t, res.body, n)
			}
		})
	}
}

func TestSparqlStatusEndpoint(t *testing.T) {
	t.Parallel()

	_, c := newEmbeddedBackend(t)

	res := sparqlDo(t, c, http.MethodGet, "/sparql/status", "", "", "")
	require.Equal(t, http.StatusOK, res.status)
	assert.Contains(t, res.body, "runningQueryCount")

	res = sparqlDo(t, c, http.MethodGet, "/sparql/status?queryId=nope", "", "", "")
	assert.Equal(t, http.StatusBadRequest, res.status)
}

func TestSparqlPersistence(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	sparqlUpdate(t, c, `INSERT DATA { <urn:a> <urn:p> "kept"@en . GRAPH <urn:g> { <urn:b> <urn:p> _:x } }`)

	snap := b.Snapshot(t.Context())

	b2 := neptune.NewInMemoryBackend("000000000000", "us-east-1")
	b2.EnableEngine(neptune.EngineConfig{InProcessGremlin: true, OpenCypher: true})
	t.Cleanup(b2.Close)
	require.NoError(t, b2.Restore(t.Context(), snap))

	c2 := waitCluster(t, b2, "c1", "available")
	res := sparqlDo(t, c2, http.MethodPost, "/sparql", "application/sparql-query", "",
		"SELECT ?g ?s ?o WHERE { GRAPH ?g { ?s <urn:p> ?o } } ORDER BY ?s")
	require.Equal(t, http.StatusOK, res.status, res.body)
	assert.Contains(t, res.body, `"xml:lang":"en"`)
	assert.Contains(t, res.body, `"type":"bnode"`)
	assert.Contains(t, res.body, "urn:g")
}

func TestSparqlStoreSurvivesStopStart(t *testing.T) {
	t.Parallel()

	b, c := newEmbeddedBackend(t)
	sparqlUpdate(t, c, `INSERT DATA { <urn:a> <urn:p> 1 }`)

	_, err := b.StopDBCluster(t.Context(), "c1")
	require.NoError(t, err)
	waitCluster(t, b, "c1", "stopped")

	_, err = b.StartDBCluster(t.Context(), "c1")
	require.NoError(t, err)

	c2 := waitCluster(t, b, "c1", "available")
	res := sparqlDo(t, c2, http.MethodGet, "/sparql?"+url.Values{"query": {"ASK { <urn:a> <urn:p> 1 }"}}.Encode(), "", "", "")
	assert.Contains(t, res.body, `"boolean":true`)
}

func sparqlSigned(t *testing.T, c neptune.DBCluster, method, path, contentType, body, accessKey string) (int, string) {
	t.Helper()

	req, err := http.NewRequestWithContext(t.Context(), method, "http://"+clusterAddr(c)+path, strings.NewReader(body))
	require.NoError(t, err)

	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	if accessKey != "" {
		dpSign(t, req, body, accessKey, dpSecret, "neptune-db")
	}

	resp, err := testclient.New(t).Do(req)
	require.NoError(t, err)

	defer func() { _ = resp.Body.Close() }()

	raw, err := io.ReadAll(resp.Body)
	require.NoError(t, err)

	return resp.StatusCode, string(raw)
}
