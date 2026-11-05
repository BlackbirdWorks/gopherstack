package neptune

import (
	"errors"
	"io"
	"net/http"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

var errGraphParams = errors.New("only one graph can be specified in the request URL")

type gspFormat struct {
	mime string
	rdf  sparql.Format
}

func gspFormats() []gspFormat {
	return []gspFormat{
		{"application/n-triples", sparql.FormatNTriples}, {"text/plain", sparql.FormatNTriples},
		{mimeNQuads, sparql.FormatNQuads}, {"text/x-nquads", sparql.FormatNQuads},
		{mimeTurtle, sparql.FormatTurtle}, {"text/n3", sparql.FormatTurtle},
		{"application/rdf+xml", sparql.FormatRDFXML}, {"application/trig", sparql.FormatTriG},
		{"application/x-trig", sparql.FormatTriG},
	}
}

func gspTarget(r *http.Request) (sparql.Term, error) {
	q := r.URL.Query()
	graphs := q["graph"]
	_, hasDefault := q["default"]

	n := len(graphs)
	if hasDefault {
		n++
	}

	switch {
	case n > 1:
		return sparql.Term{}, errGraphParams
	case hasDefault:
		return sparql.IRI(sparql.DefaultGraph), nil
	case n == 1:
		return sparql.IRI(graphs[0]), nil
	default:
		return sparql.Term{}, errors.New("a 'graph' or 'default' query parameter is required")
	}
}

func (f *cypherFront) serveGSP(w http.ResponseWriter, r *http.Request) {
	g, err := gspTarget(r)
	if err != nil {
		writeCypherError(w, http.StatusBadRequest, codeBadRequest, err.Error())

		return
	}

	store := f.rdf.store

	switch r.Method {
	case http.MethodGet, http.MethodHead:
		if !authorizeActions(w, r, langSparql, actionRead) {
			return
		}

		f.gspGet(w, r, g)
	case http.MethodPut, http.MethodPost:
		if !authorizeActions(w, r, langSparql, queryActions(true, r.Method == http.MethodPut)...) {
			return
		}

		f.gspWrite(w, r, g, r.Method == http.MethodPut)
	case http.MethodDelete:
		if !authorizeActions(w, r, langSparql, queryActions(false, true)...) {
			return
		}

		if store.ClearGraph(g) == 0 {
			writeCypherError(w, http.StatusNotFound, codeBadRequest, "Graph "+g.Value+" does not exist")

			return
		}

		w.WriteHeader(http.StatusNoContent)
	default:
		w.Header().Set("Allow", "GET, HEAD, PUT, POST, DELETE")
		writeCypherError(w, http.StatusMethodNotAllowed, codeBadRequest, "Unsupported method for the Graph Store endpoint")
	}
}

func (f *cypherFront) gspGet(w http.ResponseWriter, r *http.Request, g sparql.Term) {
	qs := f.rdf.store.GraphQuads(g)
	if len(qs) == 0 {
		writeCypherError(w, http.StatusNotFound, codeBadRequest, "Graph "+g.Value+" does not exist")

		return
	}

	offers := make([]offer, 0, len(gspFormats()))
	for _, gf := range gspFormats() {
		offers = append(offers, offer{mime: gf.mime, rdf: gf.rdf})
	}

	o, ok := negotiate(r.Header.Get("Accept"), append([]offer{{mime: mimeNQuads, rdf: sparql.FormatNQuads}}, offers...))
	if !ok {
		writeCypherError(w, http.StatusNotAcceptable, codeUnsupMedia, "No supported RDF media type matches the Accept header")

		return
	}

	body, err := sparql.SerializeQuads(o.rdf, qs)
	if err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	w.Header().Set("Content-Type", o.mime+";charset=UTF-8")
	w.WriteHeader(http.StatusOK)

	if r.Method != http.MethodHead {
		_, _ = w.Write(body)
	}
}

func (f *cypherFront) gspWrite(w http.ResponseWriter, r *http.Request, g sparql.Term, replace bool) {
	ct := strings.ToLower(strings.TrimSpace(strings.SplitN(r.Header.Get("Content-Type"), ";", 2)[0]))

	format, ok := sparql.Format(0), false

	for _, gf := range gspFormats() {
		if gf.mime == ct {
			format, ok = gf.rdf, true
		}
	}

	if !ok {
		writeCypherError(w, http.StatusUnsupportedMediaType, codeUnsupMedia,
			"Content-Type must be an RDF serialization (N-Triples, N-Quads, Turtle, TriG or RDF/XML)")

		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, cypherMaxBody))
	if err != nil {
		writeCypherError(w, http.StatusBadRequest, codeBadRequest, err.Error())

		return
	}

	quads, err := sparql.ParseRDF(format, body, sparql.ParseOptions{Graph: g, Base: r.URL.String()})
	if err != nil {
		writeCypherError(w, http.StatusBadRequest, codeBadRequest, err.Error())

		return
	}

	existed := len(f.rdf.store.GraphQuads(g)) > 0
	f.rdf.store.ApplyGraphWrite(g, quads, replace)

	if existed {
		w.WriteHeader(http.StatusNoContent)

		return
	}

	w.WriteHeader(http.StatusCreated)
}
