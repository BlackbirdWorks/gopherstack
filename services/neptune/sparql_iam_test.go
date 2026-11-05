package neptune_test

import (
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparqlDataPlaneIAM(t *testing.T) {
	t.Parallel()

	ask := "/sparql?" + url.Values{"query": {"ASK { ?s ?p ?o }"}}.Encode()
	updateBody := url.Values{"update": {"INSERT DATA { <urn:a> <urn:p> 1 }"}}.Encode()
	deleteBody := url.Values{"update": {"DELETE WHERE { ?s ?p ?o }"}}.Encode()

	tests := []struct {
		name       string
		method     string
		path       string
		body       string
		accessKey  string
		enforce    bool
		wantBody   string
		wantStatus int
	}{
		{name: "unsigned denied", method: "GET", path: ask, enforce: true, wantStatus: 403, wantBody: "AccessDeniedException"},
		{name: "read key may query", method: "GET", path: ask, accessKey: "AKIAREAD", enforce: true, wantStatus: 200},
		{name: "read key cannot insert", method: "POST", path: "/sparql", body: updateBody, accessKey: "AKIAREAD",
			enforce: true, wantStatus: 403, wantBody: "neptune-db:WriteDataViaQuery"},
		{name: "full key may insert", method: "POST", path: "/sparql", body: updateBody, accessKey: "AKIAFULL",
			enforce: true, wantStatus: 200},
		{name: "delete needs delete action", method: "POST", path: "/sparql", body: deleteBody, accessKey: "AKIAREAD",
			enforce: true, wantStatus: 403, wantBody: "neptune-db:DeleteDataViaQuery"},
		{name: "none key cannot query", method: "GET", path: ask, accessKey: "AKIANONE", enforce: true,
			wantStatus: 403, wantBody: "neptune-db:ReadDataViaQuery"},
		{name: "enforce off admits signed", method: "POST", path: "/sparql", body: updateBody, accessKey: "AKIANONE",
			wantStatus: 200},
		{name: "status needs action", method: "GET", path: "/sparql/status", accessKey: "AKIAREAD", enforce: true,
			wantStatus: 403, wantBody: "neptune-db:GetQueryStatus"},
		{name: "status allowed", method: "GET", path: "/sparql/status", accessKey: "AKIAFULL", enforce: true,
			wantStatus: 200},
		{name: "gsp read allowed", method: "GET", path: "/sparql/gsp/?default", accessKey: "AKIAREAD", enforce: true,
			wantStatus: 404},
		{name: "gsp write denied for read key", method: "PUT", path: "/sparql/gsp/?default", accessKey: "AKIAREAD",
			enforce: true, wantStatus: 403, wantBody: "neptune-db:WriteDataViaQuery"},
		{name: "loader needs start action", method: "POST", path: "/loader", accessKey: "AKIAFULL", enforce: true,
			body: `{"source":"s3://b/k","format":"ntriples"}`, wantStatus: 403, wantBody: "neptune-db:StartLoaderJob"},
		{name: "loader list needs action", method: "GET", path: "/loader", accessKey: "AKIAREAD", enforce: true,
			wantStatus: 403, wantBody: "neptune-db:ListLoaderJobs"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newIAMCluster(t, true, tt.enforce)
			ct := ""

			if tt.body != "" {
				ct = formCT
			}

			code, body := sparqlSigned(t, c, tt.method, tt.path, ct, tt.body, tt.accessKey)
			require.Equal(t, tt.wantStatus, code, body)
			assert.Contains(t, body, tt.wantBody)
		})
	}
}
