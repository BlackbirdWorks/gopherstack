package neptune_test

import (
	"encoding/json/v2"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blackbirdworks/gopherstack/pkgs/jsonx"
)

func TestSparqlLoadFromWebServer(t *testing.T) {
	t.Parallel()

	web := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/typed":
			w.Header().Set("Content-Type", "application/ld+json")
			_, _ = w.Write([]byte(`{"@id":"http://example.org/s","http://example.org/p":"loaded"}`))
		case "/ext.ttl":
			_, _ = w.Write([]byte(`<http://example.org/s> <http://example.org/p> "loaded" .`))
		case "/broken":
			w.Header().Set("Content-Type", "text/turtle")
			_, _ = w.Write([]byte(`<http://example.org/s> <http://example.org/p>`))
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(web.Close)

	tests := []struct {
		name       string
		path       string
		wantCode   string
		wantMsg    string
		wantStatus int
		wantTriple bool
	}{
		{name: "content type wins over missing extension", path: "/typed", wantStatus: 200, wantTriple: true},
		{name: "extension fallback", path: "/ext.ttl", wantStatus: 200, wantTriple: true},
		{name: "invalid syntax", path: "/broken", wantStatus: 400, wantCode: "InvalidParameterException",
			wantMsg: "Invalid syntax in the specified file"},
		{name: "missing document", path: "/nothing", wantStatus: 400, wantCode: "InvalidParameterException",
			wantMsg: "HTTP 404"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)

			res := sparqlDo(t, c, http.MethodPost, "/sparql", formCT, "",
				url.Values{"update": {"LOAD <" + web.URL + tt.path + ">"}}.Encode())
			require.Equal(t, tt.wantStatus, res.status, res.body)

			if tt.wantCode != "" {
				var e map[string]string
				require.NoError(t, json.Unmarshal([]byte(res.body), &e, jsonx.V1()), res.body)
				assert.Equal(t, tt.wantCode, e["code"])
				assert.Contains(t, e["detailedMessage"], tt.wantMsg)
			}

			ask := sparqlDo(t, c, http.MethodGet, "/sparql?"+url.Values{
				"query": {`ASK { <http://example.org/s> <http://example.org/p> "loaded" }`},
			}.Encode(), "", "", "")
			assert.Contains(
				t,
				ask.body,
				map[bool]string{true: `"boolean":true`, false: `"boolean":false`}[tt.wantTriple],
			)
		})
	}
}
