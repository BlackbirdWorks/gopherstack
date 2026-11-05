package neptune_test

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraphStoreProtocol(t *testing.T) {
	t.Parallel()

	nt := "application/n-triples"
	ttl := "text/turtle"

	t.Run("crud", func(t *testing.T) {
		t.Parallel()

		_, c := newEmbeddedBackend(t)
		g := "/sparql/gsp/?graph=urn:g1"

		res := sparqlDo(t, c, http.MethodGet, g, "", "", "")
		assert.Equal(t, http.StatusNotFound, res.status)

		res = sparqlDo(t, c, http.MethodPut, g, nt, "", "<urn:a> <urn:p> <urn:b> .\n")
		assert.Equal(t, http.StatusCreated, res.status, res.body)

		res = sparqlDo(t, c, http.MethodGet, g, "", nt, "")
		require.Equal(t, http.StatusOK, res.status)
		assert.Equal(t, "<urn:a> <urn:p> <urn:b> .\n", res.body)
		assert.Contains(t, res.header.Get("Content-Type"), nt)

		res = sparqlDo(t, c, http.MethodPost, g, ttl, "", "@prefix : <urn:> . :c :p :d .")
		assert.Equal(t, http.StatusNoContent, res.status, res.body)

		res = sparqlDo(t, c, http.MethodGet, g, "", nt, "")
		assert.Contains(t, res.body, "<urn:a> <urn:p> <urn:b> .")
		assert.Contains(t, res.body, "<urn:c> <urn:p> <urn:d> .")

		res = sparqlDo(t, c, http.MethodPut, g, nt, "", "<urn:x> <urn:p> <urn:y> .\n")
		assert.Equal(t, http.StatusNoContent, res.status)

		res = sparqlDo(t, c, http.MethodGet, g, "", nt, "")
		assert.Equal(t, "<urn:x> <urn:p> <urn:y> .\n", res.body)

		res = sparqlDo(t, c, http.MethodHead, g, "", nt, "")
		assert.Equal(t, http.StatusOK, res.status)
		assert.Empty(t, res.body)

		res = sparqlDo(t, c, http.MethodDelete, g, "", "", "")
		assert.Equal(t, http.StatusNoContent, res.status)

		res = sparqlDo(t, c, http.MethodDelete, g, "", "", "")
		assert.Equal(t, http.StatusNotFound, res.status)
	})

	t.Run("default graph", func(t *testing.T) {
		t.Parallel()

		_, c := newEmbeddedBackend(t)

		res := sparqlDo(t, c, http.MethodPost, "/sparql/gsp/?default", nt, "", "<urn:a> <urn:p> <urn:b> .\n")
		assert.Equal(t, http.StatusCreated, res.status)
		assert.Equal(t, "1", countTriples(t, c, "GRAPH <http://aws.amazon.com/neptune/vocab/v01/DefaultNamedGraph> { ?s ?p ?o }"))
	})

	t.Run("quads body updates every named graph and the url graph", func(t *testing.T) {
		t.Parallel()

		_, c := newEmbeddedBackend(t)
		body := "<urn:JohnDoe> <urn:votedFor> <urn:Labour> <urn:votes:2005> .\n" +
			"<urn:JohnDoe> <urn:votedFor> <urn:Conservative> <urn:votes:2019> .\n"

		res := sparqlDo(t, c, http.MethodPut, "/sparql/gsp/?graph=urn:votes", "application/n-quads", "", body)
		require.Equal(t, http.StatusCreated, res.status, res.body)

		assert.Equal(t, "2", countTriples(t, c, "GRAPH <urn:votes> { ?s ?p ?o }"))
		assert.Equal(t, "1", countTriples(t, c, "GRAPH <urn:votes:2005> { ?s ?p ?o }"))
		assert.Equal(t, "1", countTriples(t, c, "GRAPH <urn:votes:2019> { ?s ?p ?o }"))
	})

	t.Run("negotiation", func(t *testing.T) {
		t.Parallel()

		_, c := newEmbeddedBackend(t)
		sparqlDo(t, c, http.MethodPut, "/sparql/gsp/?graph=urn:g", nt, "", "<urn:a> <urn:p> <urn:b> .\n")

		res := sparqlDo(t, c, http.MethodGet, "/sparql/gsp/?graph=urn:g", "", ttl, "")
		assert.Contains(t, res.header.Get("Content-Type"), ttl)
		assert.Contains(t, res.body, "<urn:a> <urn:p> <urn:b> .")

		res = sparqlDo(t, c, http.MethodGet, "/sparql/gsp/?graph=urn:g", "", "application/rdf+xml", "")
		assert.Contains(t, res.body, "rdf:resource")

		res = sparqlDo(t, c, http.MethodGet, "/sparql/gsp/?graph=urn:g", "", "image/png", "")
		assert.Equal(t, http.StatusNotAcceptable, res.status)
	})

	t.Run("errors", func(t *testing.T) {
		t.Parallel()

		_, c := newEmbeddedBackend(t)

		tests := []struct {
			name, method, path, ct, body string
			want                         int
		}{
			{"two graphs", "POST", "/sparql/gsp/?graph=urn:a&graph=urn:b", nt, "", 400},
			{"default and graph", "GET", "/sparql/gsp/?default&graph=urn:b", "", "", 400},
			{"no target", "GET", "/sparql/gsp/", "", "", 400},
			{"bad content type", "PUT", "/sparql/gsp/?graph=urn:a", "application/json", "{}", 415},
			{"bad body", "PUT", "/sparql/gsp/?graph=urn:a", nt, "not rdf", 400},
			{"bad method", "PATCH", "/sparql/gsp/?graph=urn:a", "", "", 405},
		}

		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				t.Parallel()

				res := sparqlDo(t, c, tt.method, tt.path, tt.ct, "", tt.body)
				assert.Equal(t, tt.want, res.status, res.body)
				assert.Contains(t, res.body, "detailedMessage")
			})
		}
	})
}
