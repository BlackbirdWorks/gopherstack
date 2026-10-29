package neptune

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/google/uuid"

	"github.com/blackbirdworks/gopherstack/pkgs/bolt"
	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
	"github.com/blackbirdworks/gopherstack/pkgs/sgfilter"
	"github.com/blackbirdworks/gopherstack/services/neptune/gremlin"
	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const (
	cypherPath         = "/opencypher"
	cypherMaxBody      = 4 << 20
	cypherReadHeader   = 10 * time.Second
	cypherQueryTimeout = 2 * time.Minute
	cypherCloseTimeout = 5 * time.Second
	codeMissingParam   = "MissingParameterException"
)

// cypherFront serves a cluster's Neptune port: /openCypher, Bolt and (with no upstream) /gremlin are answered
// in-process over one graph; with an upstream, /gremlin is proxied to the Gremlin Server container instead.
type cypherFront struct {
	ctx            context.Context
	srv            *http.Server
	proxy          *httputil.ReverseProxy
	tr             *http.Transport
	graph          *opencypher.Graph
	queries        *queryTracker
	gremlin        *gremlin.Server
	gremlinQueries *queryTracker
	auth           func() clusterAuth
	cancel         context.CancelFunc
	upstream       string
	up             atomic.Bool
}

// startCypherFront listens on port. An empty innerAddr serves Gremlin in-process over graph.
func startCypherFront(
	host string, port int, innerAddr string, graph *opencypher.Graph, admit sgfilter.Admitter,
	auth func() clusterAuth,
) (*cypherFront, error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(host), strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listen for openCypher: %w", err)
	}

	if graph == nil {
		graph = opencypher.NewGraph()
	}

	f := &cypherFront{auth: auth, graph: graph, queries: newQueryTracker(), tr: httpclient.NewTransport()}
	f.ctx, f.cancel = context.WithCancel(context.Background())

	if innerAddr == "" {
		f.gremlinQueries = newQueryTracker()
		f.gremlin = gremlin.NewServer(f.ctx, graph, gremlin.Options{
			Track: f.gremlinQueries.begin, Authorize: gremlinAuthorizer,
		})
	} else {
		f.upstream = innerAddr
		f.proxy = httputil.NewSingleHostReverseProxy(&url.URL{Scheme: "http", Host: innerAddr})
		f.proxy.Transport = f.tr
	}

	f.srv = &http.Server{Handler: f, ReadHeaderTimeout: cypherReadHeader}

	go func() { _ = f.srv.Serve(bolt.Split(sgfilter.Guard(ln, admit), f.serveBolt)) }()

	return f, nil
}

// Close stops the listener and drops idle upstream connections.
func (f *cypherFront) Close() {
	f.cancel()

	ctx, cancel := context.WithTimeout(context.Background(), cypherCloseTimeout)
	defer cancel()

	_ = f.srv.Shutdown(ctx)
	_ = f.srv.Close()

	f.tr.CloseIdleConnections()
}

func (f *cypherFront) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if !f.up.Load() {
		http.Error(w, "cluster is not available", http.StatusServiceUnavailable)

		return
	}

	r, ok := f.authenticate(w, r)
	if !ok {
		return
	}

	if strings.EqualFold(strings.TrimRight(r.URL.Path, "/"), cypherPath) {
		f.serveOpenCypher(w, r)

		return
	}

	if !authorizeStatus(w, r) {
		return
	}

	if f.serveQueryStatus(w, r) {
		return
	}

	if f.gremlin != nil && strings.EqualFold(strings.TrimRight(r.URL.Path, "/"), gremlinPath) {
		f.gremlin.ServeHTTP(w, r)

		return
	}

	if f.proxy == nil {
		writeCypherError(w, http.StatusNotFound, "BadRequestException", "Unknown path "+r.URL.Path)

		return
	}

	if !authorizeProxied(w, r) {
		return
	}

	if enforcesFrames(r) {
		f.proxyGremlinWS(w, r)

		return
	}

	f.proxy.ServeHTTP(w, r)
}

type cypherRequest struct {
	Parameters any    `json:"parameters"`
	Query      string `json:"query"`
	OpenQuery  string `json:"openCypherQuery"`
	Explain    string `json:"explain"`
	ExplainAlt string `json:"explainMode"`
}

// cypherCall is one decoded /openCypher request.
type cypherCall struct {
	query   string
	params  string
	explain string
}

func writeCypherJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeCypherError(w http.ResponseWriter, status int, code, detail string) {
	writeCypherJSON(w, status, map[string]string{
		"requestId": uuid.NewString(), "code": code, "detailedMessage": detail,
	})
}

func readCypherRequest(w http.ResponseWriter, r *http.Request) (cypherCall, error) {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()

		return cypherCall{query: q.Get("query"), params: q.Get("parameters"), explain: q.Get("explain")}, nil
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, cypherMaxBody)

		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			return readCypherJSON(r.Body)
		}

		if err := r.ParseForm(); err != nil {
			return cypherCall{}, fmt.Errorf("read form: %w", err)
		}

		f := r.PostForm

		return cypherCall{query: f.Get("query"), params: f.Get("parameters"), explain: f.Get("explain")}, nil
	default:
		return cypherCall{}, errMethodNotAllowed
	}
}

var errMethodNotAllowed = errors.New("method not allowed")

func readCypherJSON(body io.Reader) (cypherCall, error) {
	var req cypherRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		return cypherCall{}, fmt.Errorf("decode request: %w", err)
	}

	call := cypherCall{query: req.Query, explain: req.Explain}
	if call.query == "" {
		call.query = req.OpenQuery
	}

	if call.explain == "" {
		call.explain = req.ExplainAlt
	}

	switch p := req.Parameters.(type) {
	case nil:
	case string:
		call.params = p
	default:
		raw, _ := json.Marshal(p)
		call.params = string(raw)
	}

	return call, nil
}

func (f *cypherFront) serveOpenCypher(w http.ResponseWriter, r *http.Request) {
	call, err := readCypherRequest(w, r)
	if errors.Is(err, errMethodNotAllowed) {
		w.Header().Set("Allow", "GET, POST")
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Only GET and POST are supported")

		return
	}

	if err != nil {
		writeCypherError(w, http.StatusBadRequest, opencypher.CodeMalformedQuery, err.Error())

		return
	}

	if strings.TrimSpace(call.query) == "" {
		writeCypherError(w, http.StatusBadRequest, codeMissingParam, "Required parameter 'query' is missing")

		return
	}

	if !authorizeActions(w, r, langOpenCypher, cypherAccessActions(call.query)...) {
		return
	}

	params, err := opencypher.ParseParameters(call.params)
	if err != nil {
		writeCypherFailure(r.Context(), w, err)

		return
	}

	ctx, finish := f.queries.begin(r.Context(), call.query)
	defer finish()

	if call.explain != "" {
		f.serveExplain(ctx, w, call, params)

		return
	}

	f.runQuery(ctx, w, call.query, params)
}

func (f *cypherFront) runQuery(ctx context.Context, w http.ResponseWriter, query string, params map[string]any) {
	res, err := f.graph.Execute(ctx, query, params)
	if err != nil {
		writeCypherFailure(ctx, w, err)

		return
	}

	rows, err := res.MarshalJSON()
	if err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	writeCypherJSON(w, http.StatusOK, map[string]json.RawMessage{"results": rowsOrEmpty(rows)})
}

func (f *cypherFront) serveExplain(ctx context.Context, w http.ResponseWriter, call cypherCall, params map[string]any) {
	plan, err := opencypher.Explain(call.query, call.explain)
	if err != nil {
		writeCypherFailure(ctx, w, err)

		return
	}

	if call.explain != "static" {
		res, xerr := f.graph.Execute(ctx, call.query, params)
		if xerr != nil {
			writeCypherFailure(ctx, w, xerr)

			return
		}

		plan += fmt.Sprintf("\nRows returned: %d\n", len(res.Rows))
	}

	w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, plan)
}

func rowsOrEmpty(rows []byte) json.RawMessage {
	if len(rows) == 0 {
		return json.RawMessage("[]")
	}

	return rows
}

func writeCypherFailure(ctx context.Context, w http.ResponseWriter, err error) {
	if errors.Is(context.Cause(ctx), errQueryCancelled) {
		writeCypherError(w, http.StatusInternalServerError, "CancelledByUserException",
			"The query was cancelled by the user")

		return
	}

	if ce, ok := opencypher.AsError(err); ok {
		writeCypherError(w, http.StatusBadRequest, ce.Code, ce.Detail)

		return
	}

	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		writeCypherError(w, http.StatusInternalServerError, "TimeLimitExceededException", err.Error())

		return
	}

	writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())
}

// serveBolt runs a Bolt connection against the cluster's graph; connections are refused while it is down.
func (f *cypherFront) serveBolt(c net.Conn) {
	if !f.up.Load() {
		_ = c.Close()

		return
	}

	bolt.Serve(f.ctx, c, bolt.Config{
		Backend: &boltBackend{graph: f.graph, queries: f.queries, auth: f.auth},
		Agent:   boltAgent, Database: boltDatabase,
	})
}
