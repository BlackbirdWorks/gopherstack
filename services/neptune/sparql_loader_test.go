package neptune_test

import (
	"encoding/json"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	sdk_s3 "github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/services/neptune"
	s3backend "github.com/blackbirdworks/gopherstack/services/s3"
)

func newLoaderCluster(t *testing.T, files map[string]string) neptune.DBCluster {
	t.Helper()

	s3b := s3backend.NewInMemoryBackend(nil)
	_, err := s3b.CreateBucket(t.Context(), &sdk_s3.CreateBucketInput{Bucket: aws.String("lake")})
	require.NoError(t, err)

	for k, v := range files {
		_, err = s3b.PutObject(t.Context(), &sdk_s3.PutObjectInput{
			Bucket: aws.String("lake"), Key: aws.String(k), Body: strings.NewReader(v),
		})
		require.NoError(t, err)
	}

	b, c := newEmbeddedBackend(t)
	b.SetS3Backend(s3b)

	return c
}

type loaderEnvelope struct {
	Payload map[string]any `json:"payload"`
	Status  string         `json:"status"`
}

func loaderStart(t *testing.T, c neptune.DBCluster, body string) (int, loaderEnvelope, string) {
	t.Helper()

	res := sparqlDo(t, c, http.MethodPost, "/loader", "application/json", "", body)

	var env loaderEnvelope

	_ = json.Unmarshal([]byte(res.body), &env)

	return res.status, env, res.body
}

func loaderWait(t *testing.T, c neptune.DBCluster, id, query string) loaderEnvelope {
	t.Helper()

	var env loaderEnvelope

	require.Eventually(t, func() bool {
		res := sparqlDo(t, c, http.MethodGet, "/loader/"+id+query, "", "", "")
		if res.status != http.StatusOK {
			return false
		}

		env = loaderEnvelope{}
		_ = json.Unmarshal([]byte(res.body), &env)

		st, _ := env.Payload["overallStatus"].(map[string]any)
		s, _ := st["status"].(string)

		return s != "LOAD_NOT_STARTED" && s != "LOAD_IN_PROGRESS" && s != "LOAD_IN_QUEUE"
	}, 10*time.Second, 10*time.Millisecond)

	return env
}

func overall(env loaderEnvelope) map[string]any {
	m, _ := env.Payload["overallStatus"].(map[string]any)

	return m
}

func countTriples(t *testing.T, c neptune.DBCluster, where string) string {
	t.Helper()

	res := sparqlDo(t, c, http.MethodGet, "/sparql?"+url.Values{
		"query": {"SELECT (COUNT(*) AS ?c) WHERE { " + where + " }"},
	}.Encode(), "", "", "")
	require.Equal(t, http.StatusOK, res.status, res.body)

	var out struct {
		Results struct {
			Bindings []map[string]map[string]string `json:"bindings"`
		} `json:"results"`
	}

	require.NoError(t, json.Unmarshal([]byte(res.body), &out))
	require.Len(t, out.Results.Bindings, 1)

	return out.Results.Bindings[0]["c"]["value"]
}

func TestBulkLoaderFormats(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		format string
		file   string
		data   string
		want   string
	}{
		{"ntriples", "ntriples", "data/a.nt",
			"<urn:s> <urn:p> <urn:o1> .\n<urn:s> <urn:p> \"two\" .\n# comment\n<urn:s2> <urn:p> 3 .\n", "3"},
		{"turtle", "turtle", "data/a.ttl",
			"@prefix ex: <http://example.org/> .\nex:a a ex:T ; ex:p 1, 2 ; ex:q [ ex:r \"x\" ] .\n", "5"},
		{"nquads", "nquads", "data/a.nq",
			"<urn:s> <urn:p> <urn:o> <urn:g> .\n<urn:s> <urn:p> <urn:o2> .\n", "2"},
		{"rdfxml", "rdfxml", "data/a.rdf",
			`<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#" xmlns:ex="http://example.org/">` +
				`<ex:T rdf:about="http://example.org/a"><ex:p>1</ex:p><ex:q rdf:resource="http://example.org/b"/></ex:T></rdf:RDF>`, "3"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newLoaderCluster(t, map[string]string{tt.file: tt.data})
			status, env, raw := loaderStart(t, c, `{"source":"s3://lake/data/","format":"`+tt.format+
				`","iamRoleArn":"arn:aws:iam::000000000000:role/r","region":"us-east-1"}`)
			require.Equal(t, http.StatusOK, status, raw)
			assert.Equal(t, "200 OK", env.Status)

			id, _ := env.Payload["loadId"].(string)
			require.NotEmpty(t, id)

			final := loaderWait(t, c, id, "")
			assert.Equal(t, "200 OK", final.Status)
			assert.Equal(t, "LOAD_COMPLETED", overall(final)["status"])
			assert.Equal(t, "s3://lake/data/", overall(final)["fullUri"])
			assert.EqualValues(t, 0, overall(final)["parsingErrors"])
			assert.Equal(t, tt.want, countTriples(t, c, "?s ?p ?o"))

			assert.Equal(t, []any{map[string]any{"LOAD_COMPLETED": float64(1)}}, final.Payload["feedCount"])
		})
	}
}

func TestBulkLoaderBehaviours(t *testing.T) {
	t.Parallel()

	good := "<urn:s> <urn:p> <urn:o> .\n"
	bad := "<urn:s> <urn:p> <urn:o> .\nthis is not rdf\n<urn:s2> <urn:p> <urn:o> .\n"

	tests := []struct {
		name       string
		files      map[string]string
		request    string
		query      string
		wantStatus string
		wantCount  string
		wantErrors int
		wantParse  float64
	}{
		{name: "fail on error default stops", files: map[string]string{"x.nt": bad},
			request:    `{"source":"s3://lake/x.nt","format":"ntriples"}`,
			wantStatus: "LOAD_FAILED", wantCount: "1", wantErrors: 1, wantParse: 1, query: "?errors=TRUE"},
		{name: "skip bad lines when failOnError false", files: map[string]string{"x.nt": bad},
			request:    `{"source":"s3://lake/x.nt","format":"ntriples","failOnError":"FALSE"}`,
			wantStatus: "LOAD_COMPLETED", wantCount: "2", wantErrors: 1, wantParse: 1, query: "?errors=TRUE"},
		{name: "named graph parser configuration", files: map[string]string{"x.nt": good},
			request:    `{"source":"s3://lake/x.nt","format":"ntriples","parserConfiguration":{"namedGraphUri":"urn:custom"}}`,
			wantStatus: "LOAD_COMPLETED", wantCount: "1"},
		{name: "duplicates counted", files: map[string]string{"x.nt": good + good},
			request:    `{"source":"s3://lake/x.nt","format":"ntriples"}`,
			wantStatus: "LOAD_COMPLETED", wantCount: "1"},
		{name: "json boolean failOnError", files: map[string]string{"x.nt": bad},
			request:    `{"source":"s3://lake/x.nt","format":"ntriples","failOnError":false}`,
			wantStatus: "LOAD_COMPLETED", wantCount: "2", wantParse: 1},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newLoaderCluster(t, tt.files)
			status, env, raw := loaderStart(t, c, tt.request)
			require.Equal(t, http.StatusOK, status, raw)

			id, _ := env.Payload["loadId"].(string)
			final := loaderWait(t, c, id, tt.query)

			assert.Equal(t, tt.wantStatus, overall(final)["status"])
			assert.EqualValues(t, tt.wantParse, overall(final)["parsingErrors"])
			assert.Equal(t, tt.wantCount, countTriples(t, c, "?s ?p ?o"))

			if tt.wantErrors > 0 {
				errs, _ := final.Payload["errors"].(map[string]any)
				logs, _ := errs["errorLogs"].([]any)
				require.Len(t, logs, tt.wantErrors)
				assert.Equal(t, "PARSING_ERROR", logs[0].(map[string]any)["errorCode"])
				assert.EqualValues(t, 2, logs[0].(map[string]any)["recordNum"])
				assert.Equal(t, id, errs["loadId"])
			}
		})
	}
}

func TestBulkLoaderRequestErrors(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		body     string
		wantCode string
		wantMsg  string
	}{
		{"missing source", `{"format":"ntriples"}`, "MissingParameterException", ""},
		{"bad format", `{"source":"s3://lake/x.nt","format":"jsonld"}`, "BadRequestException", "Invalid format"},
		{"csv unsupported", `{"source":"s3://lake/x.nt","format":"csv"}`, "BadRequestException", "property-graph CSV"},
		{"missing bucket", `{"source":"s3://nobucket/x.nt","format":"ntriples"}`, "BadRequestException", "Failed to start"},
		{"no files", `{"source":"s3://lake/none/","format":"ntriples"}`, "BadRequestException", "no files"},
		{"not s3", `{"source":"http://example.com/x.nt","format":"ntriples"}`, "BadRequestException", "not an Amazon S3 URI"},
		{"malformed json", `{`, "BadRequestException", "Invalid loader request"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newLoaderCluster(t, map[string]string{"x.nt": "<urn:s> <urn:p> <urn:o> .\n"})
			status, _, raw := loaderStart(t, c, tt.body)
			require.Equal(t, http.StatusBadRequest, status, raw)

			var e map[string]string
			require.NoError(t, json.Unmarshal([]byte(raw), &e))
			assert.Equal(t, tt.wantCode, e["code"])
			assert.Contains(t, e["detailedMessage"], tt.wantMsg)
		})
	}
}

func TestBulkLoaderListCancelDependencies(t *testing.T) {
	t.Parallel()

	c := newLoaderCluster(t, map[string]string{
		"good.nt": "<urn:s> <urn:p> <urn:o> .\n", "bad.nt": "garbage\n",
	})

	_, env, _ := loaderStart(t, c, `{"source":"s3://lake/bad.nt","format":"ntriples"}`)
	badID, _ := env.Payload["loadId"].(string)
	loaderWait(t, c, badID, "")

	_, env, raw := loaderStart(t, c,
		`{"source":"s3://lake/good.nt","format":"ntriples","queueRequest":"TRUE","dependencies":["`+badID+`"]}`)
	depID, _ := env.Payload["loadId"].(string)
	require.NotEmpty(t, depID, raw)

	final := loaderWait(t, c, depID, "")
	assert.Equal(t, "LOAD_FAILED_BECAUSE_DEPENDENCY_NOT_SATISFIED", overall(final)["status"])
	assert.Equal(t, "0", countTriples(t, c, "?s ?p ?o"))

	list := sparqlDo(t, c, http.MethodGet, "/loader", "", "", "")
	require.Equal(t, http.StatusOK, list.status)

	var l loaderEnvelope

	require.NoError(t, json.Unmarshal([]byte(list.body), &l))
	assert.ElementsMatch(t, []any{badID, depID}, l.Payload["loadIds"])

	limited := sparqlDo(t, c, http.MethodGet, "/loader?limit=1", "", "", "")
	require.NoError(t, json.Unmarshal([]byte(limited.body), &l))
	assert.Len(t, l.Payload["loadIds"], 1)

	byQuery := sparqlDo(t, c, http.MethodGet, "/loader?loadId="+badID, "", "", "")
	assert.Equal(t, http.StatusOK, byQuery.status)

	unknown := sparqlDo(t, c, http.MethodGet, "/loader/nope", "", "", "")
	assert.Equal(t, http.StatusBadRequest, unknown.status)

	cancelDone := sparqlDo(t, c, http.MethodDelete, "/loader/"+badID, "", "", "")
	assert.Equal(t, http.StatusBadRequest, cancelDone.status)

	cancelMissing := sparqlDo(t, c, http.MethodDelete, "/loader/nope", "", "", "")
	assert.Equal(t, http.StatusNotFound, cancelMissing.status)
}

func TestSparqlLoadFromS3(t *testing.T) {
	t.Parallel()

	c := newLoaderCluster(t, map[string]string{
		"d/a.ttl": "@prefix ex: <http://example.org/> . ex:a ex:p ex:b .\n",
	})

	sparqlUpdate(t, c, "LOAD <s3://lake/d/a.ttl> INTO GRAPH <urn:loaded>")
	assert.Equal(t, "1", countTriples(t, c, "GRAPH <urn:loaded> { ?s ?p ?o }"))

	res := sparqlDo(t, c, http.MethodPost, "/sparql", formCT, "",
		url.Values{"update": {"LOAD <s3://lake/missing.ttl>"}}.Encode())
	assert.GreaterOrEqual(t, res.status, 400)

	silent := sparqlDo(t, c, http.MethodPost, "/sparql", formCT, "",
		url.Values{"update": {"LOAD SILENT <s3://lake/missing.ttl>"}}.Encode())
	assert.Equal(t, http.StatusOK, silent.status)
}
