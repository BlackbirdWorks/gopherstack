package neptune_test

import (
	"net/http"
	"net/url"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSparqlExtendedMediaTypes(t *testing.T) {
	t.Parallel()

	const insert = `INSERT DATA { <http://example.org/s> <http://example.org/p> "v" }`

	construct := "/sparql?" + url.Values{"query": {"CONSTRUCT WHERE { ?s <http://example.org/p> ?o }"}}.Encode()
	sel := "/sparql?" + url.Values{"query": {"SELECT ?o WHERE { ?s <http://example.org/p> ?o }"}}.Encode()

	tests := []struct {
		name     string
		path     string
		accept   string
		wantType string
		wantBody string
	}{
		{"jsonld", construct, "application/ld+json", "application/ld+json;charset=UTF-8",
			`"http://example.org/p":[{"@value":"v"}]`},
		{"trix", construct, "application/trix", "application/trix;charset=UTF-8", "<plainLiteral>v</plainLiteral>"},
		{"rdf4j binary", construct, "application/x-binary-rdf", "application/x-binary-rdf", "BRDF"},
		{
			"binary table",
			sel,
			"application/x-binary-rdf-results-table",
			"application/x-binary-rdf-results-table",
			"BRTR",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			_, c := newEmbeddedBackend(t)
			sparqlUpdate(t, c, insert)

			res := sparqlDo(t, c, http.MethodGet, tt.path, "", tt.accept, "")
			require.Equal(t, http.StatusOK, res.status, res.body)
			assert.Equal(t, tt.wantType, res.header.Get("Content-Type"))
			assert.Contains(t, res.body, tt.wantBody)
		})
	}
}
