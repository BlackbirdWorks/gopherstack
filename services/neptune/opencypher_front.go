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

	"github.com/blackbirdworks/gopherstack/pkgs/container"
	"github.com/blackbirdworks/gopherstack/pkgs/httpclient"
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

// cypherFront serves a cluster's Neptune port: /openCypher is answered in-process and everything else
// (the Gremlin WebSocket) is proxied to the Gremlin Server container.
type cypherFront struct {
	srv   *http.Server
	proxy *httputil.ReverseProxy
	tr    *http.Transport
	graph *opencypher.Graph
	up    atomic.Bool
}

func startCypherFront(host string, port int, innerAddr string) (*cypherFront, error) {
	ln, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp",
		net.JoinHostPort(container.BindHostFor(host), strconv.Itoa(port)))
	if err != nil {
		return nil, fmt.Errorf("listen for openCypher: %w", err)
	}

	f := &cypherFront{graph: opencypher.NewGraph(), tr: httpclient.NewTransport()}
	target := &url.URL{Scheme: "http", Host: innerAddr}
	f.proxy = httputil.NewSingleHostReverseProxy(target)
	f.proxy.Transport = f.tr
	f.srv = &http.Server{Handler: f, ReadHeaderTimeout: cypherReadHeader}

	go func() { _ = f.srv.Serve(ln) }()

	return f, nil
}

// Close stops the listener and drops idle upstream connections.
func (f *cypherFront) Close() {
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

	if strings.EqualFold(strings.TrimRight(r.URL.Path, "/"), cypherPath) {
		serveOpenCypher(f.graph, w, r)

		return
	}

	f.proxy.ServeHTTP(w, r)
}

type cypherRequest struct {
	Parameters any    `json:"parameters"`
	Query      string `json:"query"`
	OpenQuery  string `json:"openCypherQuery"`
}

func writeCypherJSON(w http.ResponseWriter, status int, body any) {
	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(body)
}

func writeCypherError(w http.ResponseWriter, status int, code, detail string) {
	writeCypherJSON(w, status, map[string]string{
		"requestId": uuid.NewString(), "code": code, "detail": detail,
	})
}

func readCypherRequest(w http.ResponseWriter, r *http.Request) (string, string, error) {
	switch r.Method {
	case http.MethodGet:
		q := r.URL.Query()

		return q.Get("query"), q.Get("parameters"), nil
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, cypherMaxBody)

		if strings.HasPrefix(r.Header.Get("Content-Type"), "application/json") {
			return readCypherJSON(r.Body)
		}

		if err := r.ParseForm(); err != nil {
			return "", "", fmt.Errorf("read form: %w", err)
		}

		return r.PostForm.Get("query"), r.PostForm.Get("parameters"), nil
	default:
		return "", "", errMethodNotAllowed
	}
}

var errMethodNotAllowed = errors.New("method not allowed")

func readCypherJSON(body io.Reader) (string, string, error) {
	var req cypherRequest
	if err := json.NewDecoder(body).Decode(&req); err != nil {
		return "", "", fmt.Errorf("decode request: %w", err)
	}

	query := req.Query
	if query == "" {
		query = req.OpenQuery
	}

	switch p := req.Parameters.(type) {
	case nil:
		return query, "", nil
	case string:
		return query, p, nil
	default:
		raw, _ := json.Marshal(p)

		return query, string(raw), nil
	}
}

func serveOpenCypher(g *opencypher.Graph, w http.ResponseWriter, r *http.Request) {
	query, rawParams, err := readCypherRequest(w, r)
	if errors.Is(err, errMethodNotAllowed) {
		w.Header().Set("Allow", "GET, POST")
		writeCypherError(w, http.StatusMethodNotAllowed, "BadRequestException", "Only GET and POST are supported")

		return
	}

	if err != nil {
		writeCypherError(w, http.StatusBadRequest, opencypher.CodeMalformedQuery, err.Error())

		return
	}

	if strings.TrimSpace(query) == "" {
		writeCypherError(w, http.StatusBadRequest, codeMissingParam, "Required parameter 'query' is missing")

		return
	}

	params, err := opencypher.ParseParameters(rawParams)
	if err != nil {
		writeCypherFailure(w, err)

		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), cypherQueryTimeout)
	defer cancel()

	res, err := g.Execute(ctx, query, params)
	if err != nil {
		writeCypherFailure(w, err)

		return
	}

	rows, err := res.MarshalJSON()
	if err != nil {
		writeCypherError(w, http.StatusInternalServerError, "InternalFailureException", err.Error())

		return
	}

	writeCypherJSON(w, http.StatusOK, map[string]json.RawMessage{"results": rowsOrEmpty(rows)})
}

func rowsOrEmpty(rows []byte) json.RawMessage {
	if len(rows) == 0 {
		return json.RawMessage("[]")
	}

	return rows
}

func writeCypherFailure(w http.ResponseWriter, err error) {
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
