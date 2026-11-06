package neptune

import (
	"context"
	"errors"
	"fmt"
	"html"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"

	"github.com/blackbirdworks/gopherstack/services/neptune/sparql"
)

const (
	sparqlPath       = "/sparql"
	gspPath          = "/sparql/gsp"
	sparqlStatusPath = "/sparql/status"
	langSparql       = "Sparql"

	mimeSparqlJSON = "application/sparql-results+json"
	mimeSparqlXML  = "application/sparql-results+xml"
	mimeNQuads     = "application/n-quads"
	mimeTurtle     = "text/turtle"
	mimeForm       = "application/x-www-form-urlencoded"
	mimeSparqlQ    = "application/sparql-query"
	mimeSparqlU    = "application/sparql-update"
	codeMalformed  = "MalformedQueryException"
	codeBadRequest = "BadRequestException"
	codeUnsupMedia = "UnsupportedMediaTypeException"
)

// serveRDF answers the SPARQL, Graph Store and loader routes; false when r is none of them.
func (f *cypherFront) serveRDF(w http.ResponseWriter, r *http.Request) bool {
	path := strings.ToLower(strings.TrimRight(r.URL.Path, "/"))

	switch {
	case path == sparqlPath:
		f.serveSparql(w, r)
	case path == sparqlStatusPath:
		f.serveSparqlStatus(w, r)
	case path == gspPath:
		f.serveGSP(w, r)
	case path == loaderPath || strings.HasPrefix(path, loaderPath+"/"):
		f.serveLoader(w, r)
	default:
		return false
	}

	return true
}

type sparqlCall struct {
	query, update, explain string
	from, named            []string
	usingG, usingN         []string
}

var errBodyTooLarge = errors.New("request body too large")

func readSparqlCall(w http.ResponseWriter, r *http.Request) (sparqlCall, error) {
	var c sparqlCall

	switch r.Method {
	case http.MethodGet:
		fill(&c, r.URL.Query())

		return c, nil
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, cypherMaxBody)

		ct := mediaType(r.Header.Get("Content-Type"))

		switch ct {
		case mimeSparqlQ, mimeSparqlU:
			body, err := io.ReadAll(r.Body)
			if err != nil {
				return c, fmt.Errorf("%w: %w", errBodyTooLarge, err)
			}

			if ct == mimeSparqlQ {
				c.query = string(body)
			} else {
				c.update = string(body)
			}

			fill(&c, r.URL.Query())

			return c, nil
		case mimeForm, "":
			if err := r.ParseForm(); err != nil {
				return c, fmt.Errorf("read form: %w", err)
			}

			fill(&c, r.Form)

			return c, nil
		default:
			return c, errUnsupportedMedia
		}
	default:
		return c, errMethodNotAllowed
	}
}

var errUnsupportedMedia = errors.New("unsupported media type")

func fill(c *sparqlCall, v url.Values) {
	if q := v.Get("query"); q != "" {
		c.query = q
	}

	if u := v.Get("update"); u != "" {
		c.update = u
	}

	c.explain = v.Get("explain")
	c.from, c.named = v["default-graph-uri"], v["named-graph-uri"]
	c.usingG, c.usingN = v["using-graph-uri"], v["using-named-graph-uri"]
}

func (f *cypherFront) serveSparql(w http.ResponseWriter, r *http.Request) {
	call, err := readSparqlCall(w, r)

	switch {
	case errors.Is(err, errMethodNotAllowed):
		w.Header().Set("Allow", "GET, POST")
		writeCypherError(w, http.StatusMethodNotAllowed, codeBadRequest, "Only GET and POST are supported")

		return
	case errors.Is(err, errUnsupportedMedia):
		writeCypherError(
			w,
			http.StatusUnsupportedMediaType,
			codeUnsupMedia,
			"Content-Type must be application/x-www-form-urlencoded, application/sparql-query or application/sparql-update",
		)

		return
	case err != nil:
		writeCypherError(w, http.StatusBadRequest, codeBadRequest, err.Error())

		return
	}

	switch {
	case call.query != "" && call.update != "":
		writeCypherError(w, http.StatusBadRequest, codeBadRequest, "Only one of 'query' and 'update' may be specified")
	case call.query != "":
		f.runSparqlQuery(w, r, call)
	case call.update != "":
		if r.Method != http.MethodPost {
			w.Header().Set("Allow", "POST")
			writeCypherError(w, http.StatusMethodNotAllowed, codeBadRequest, "SPARQL updates must be sent with POST")

			return
		}

		f.runSparqlUpdate(w, r, call)
	default:
		writeCypherError(
			w,
			http.StatusBadRequest,
			codeMissingParam,
			"Required parameter 'query' or 'update' is missing",
		)
	}
}

// mediaType returns the lower-cased media type of a Content-Type value, without parameters.
func mediaType(h string) string {
	mt, _, _ := strings.Cut(h, ";")

	return strings.ToLower(strings.TrimSpace(mt))
}

func iris(ss []string) []sparql.Term {
	out := make([]sparql.Term, len(ss))
	for i, s := range ss {
		out[i] = sparql.IRI(s)
	}

	return out
}

func (f *cypherFront) runSparqlQuery(w http.ResponseWriter, r *http.Request, call sparqlCall) {
	if !authorizeActions(w, r, langSparql, actionRead) {
		return
	}

	q, err := sparql.ParseQuery(call.query)
	if err != nil {
		writeSparqlFailure(r.Context(), w, err)

		return
	}

	if len(call.from)+len(call.named) > 0 {
		q.SetDataset(iris(call.from), iris(call.named))
	}

	ctx, finish := f.rdf.queries.begin(r.Context(), call.query)
	defer finish()

	if call.explain != "" {
		f.serveSparqlExplain(ctx, w, q, call)

		return
	}

	res, ok := negotiateResult(w, r, q)
	if !ok {
		return
	}

	out, err := f.rdf.store.Run(ctx, q)
	if err != nil {
		writeSparqlFailure(ctx, w, err)

		return
	}

	body, ctype, err := res.encode(out)
	if err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	w.Header().Set("Content-Type", ctype)
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func (f *cypherFront) serveSparqlExplain(ctx context.Context, w http.ResponseWriter, q *sparql.Query, call sparqlCall) {
	plan := q.Explain()

	if call.explain != "static" {
		out, err := f.rdf.store.Run(ctx, q)
		if err != nil {
			writeSparqlFailure(ctx, w, err)

			return
		}

		plan += fmt.Sprintf("Solutions: %d\n", resultSize(out))
	}

	w.Header().Set("Content-Type", "text/html;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, "<!DOCTYPE html>\n<html><head><title>Neptune SPARQL explain</title></head><body>\n<pre>\n"+
		html.EscapeString(plan)+"</pre>\n</body></html>\n")
}

func resultSize(r *sparql.Result) int {
	switch r.Kind {
	case sparql.ResultSelect:
		return len(r.Rows)
	case sparql.ResultGraph:
		return len(r.Triples)
	default:
		return 1
	}
}

func (f *cypherFront) runSparqlUpdate(w http.ResponseWriter, r *http.Request, call sparqlCall) {
	u, err := sparql.ParseUpdate(call.update)
	if err != nil {
		writeSparqlFailure(r.Context(), w, err)

		return
	}

	write, del := u.Access()
	if !authorizeActions(w, r, langSparql, queryActions(write, del)...) {
		return
	}

	if len(call.usingG)+len(call.usingN) > 0 {
		u.SetUsing(iris(call.usingG), iris(call.usingN))
	}

	ctx, finish := f.rdf.queries.begin(r.Context(), call.update)
	defer finish()

	if _, err = f.rdf.store.Apply(ctx, u, sparql.UpdateOptions{Fetch: f.rdf.fetch}); err != nil {
		writeSparqlFailure(ctx, w, err)

		return
	}

	w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
}

func writeSparqlFailure(ctx context.Context, w http.ResponseWriter, err error) {
	var se *sparql.SyntaxError
	if errors.As(err, &se) && ctx.Err() == nil {
		writeCypherError(w, http.StatusBadRequest, codeMalformed, se.Msg)

		return
	}

	if pe, ok := errors.AsType[*sparql.ParseError](err); ok {
		writeCypherError(w, http.StatusBadRequest, codeMalformed, pe.Msg)

		return
	}

	writeCypherFailure(ctx, w, err)
}

type offer struct {
	mime   string
	result sparql.ResultFormat
	rdf    sparql.Format
}

type encoder struct {
	mime   string
	result sparql.ResultFormat
	rdf    sparql.Format
	graph  bool
}

func (e encoder) encode(out *sparql.Result) ([]byte, string, error) {
	if e.graph {
		body, err := sparql.SerializeTriples(e.rdf, out.Triples)

		return body, e.mime + ";charset=UTF-8", err
	}

	body, err := sparql.WriteResult(out, e.result)

	return body, e.mime + ";charset=UTF-8", err
}

func selectOffers() []offer {
	return []offer{
		{mime: mimeSparqlJSON, result: sparql.ResultJSON}, {mime: "application/json", result: sparql.ResultJSON},
		{mime: mimeSparqlXML, result: sparql.ResultXML}, {mime: "application/xml", result: sparql.ResultXML},
		{mime: "text/xml", result: sparql.ResultXML}, {mime: "text/csv", result: sparql.ResultCSV},
		{mime: "text/tab-separated-values", result: sparql.ResultTSV},
	}
}

func askOffers() []offer {
	return []offer{
		{mime: mimeSparqlJSON, result: sparql.ResultJSON}, {mime: "application/json", result: sparql.ResultJSON},
		{mime: mimeSparqlXML, result: sparql.ResultXML}, {mime: "application/xml", result: sparql.ResultXML},
		{mime: "text/xml", result: sparql.ResultXML}, {mime: "text/boolean", result: sparql.ResultBoolean},
	}
}

func graphOffers() []offer {
	return []offer{
		{mime: mimeNQuads, rdf: sparql.FormatNQuads}, {mime: "text/x-nquads", rdf: sparql.FormatNQuads},
		{mime: "application/n-triples", rdf: sparql.FormatNTriples}, {mime: "text/plain", rdf: sparql.FormatNTriples},
		{mime: mimeTurtle, rdf: sparql.FormatTurtle}, {mime: "text/n3", rdf: sparql.FormatTurtle},
		{mime: "application/rdf+xml", rdf: sparql.FormatRDFXML}, {mime: "application/trig", rdf: sparql.FormatTriG},
	}
}

func negotiateResult(w http.ResponseWriter, r *http.Request, q *sparql.Query) (encoder, bool) {
	offers := graphOffers()
	graph := true

	switch {
	case q.IsAsk():
		offers, graph = askOffers(), false
	case q.IsSelect():
		offers, graph = selectOffers(), false
	}

	o, ok := negotiate(r.Header.Get("Accept"), offers)
	if !ok {
		writeCypherError(w, http.StatusNotAcceptable, codeUnsupMedia,
			"No supported media type matches the Accept header "+strconv.Quote(r.Header.Get("Accept")))

		return encoder{}, false
	}

	return encoder{mime: o.mime, result: o.result, rdf: o.rdf, graph: graph}, true
}

type acceptRange struct {
	mime string
	q    float64
}

func parseAccept(accept string) []acceptRange {
	var ranges []acceptRange

	for part := range strings.SplitSeq(accept, ",") {
		fields := strings.Split(strings.TrimSpace(part), ";")
		qv := 1.0

		for _, p := range fields[1:] {
			if k, v, ok := strings.Cut(strings.TrimSpace(p), "="); ok && k == "q" {
				if f, err := strconv.ParseFloat(v, 64); err == nil {
					qv = f
				}
			}
		}

		if qv > 0 {
			ranges = append(ranges, acceptRange{mime: mediaType(fields[0]), q: qv})
		}
	}

	sort.SliceStable(ranges, func(i, j int) bool { return ranges[i].q > ranges[j].q })

	return ranges
}

func rangeMatches(rng, mime string) bool {
	if rng == "*/*" || rng == mime {
		return true
	}

	prefix, wild := strings.CutSuffix(rng, "*")

	return wild && strings.HasPrefix(mime, prefix)
}

// negotiate picks the first offer matching the Accept header by descending quality; the first offer is the default.
func negotiate(accept string, offers []offer) (offer, bool) {
	if strings.TrimSpace(accept) == "" {
		return offers[0], true
	}

	for _, rg := range parseAccept(accept) {
		for _, o := range offers {
			if rangeMatches(rg.mime, o.mime) {
				return o, true
			}
		}
	}

	return offer{}, false
}

func (f *cypherFront) serveSparqlStatus(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	id := q.Get("queryId")
	t := f.rdf.queries

	switch {
	case r.Method == http.MethodGet && id == "":
		if authorizeActions(w, r, langSparql, actionStatus) {
			listQueries(w, t)
		}
	case r.Method == http.MethodGet:
		if authorizeActions(w, r, langSparql, actionStatus) {
			getQuery(w, t, id)
		}
	case r.Method == http.MethodPost && id != "" && strings.EqualFold(r.FormValue("cancelQuery"), "true"):
		if authorizeActions(w, r, langSparql, actionCancel) {
			cancelQuery(w, t, id)
		}
	default:
		w.Header().Set("Allow", "GET, POST")
		writeCypherError(w, http.StatusBadRequest, codeBadRequest,
			"Use GET to list queries or POST with cancelQuery=true and queryId to cancel one")
	}
}
