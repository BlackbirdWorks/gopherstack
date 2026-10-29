package gremlin

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/gorilla/websocket"

	"github.com/blackbirdworks/gopherstack/services/neptune/opencypher"
)

const (
	opEval           = "eval"
	opBytecode       = "bytecode"
	maxFrameBytes    = 4 << 20
	maxScriptBytes   = 1 << 20
	defaultBatchSize = 64
	maxBatchSize     = 10000
	defaultTimeout   = 2 * time.Minute
	writeTimeout     = 30 * time.Second
)

// Options configures a Server.
type Options struct {
	// Track registers a running query and returns its cancellable context and a finish callback.
	Track func(parent context.Context, query string) (context.Context, func())
	// Authorize, when set, vets each query before it runs; ctx is the request (or WebSocket handshake) context.
	Authorize func(ctx context.Context, write, del bool) error
	Timeout   time.Duration
}

// Server serves Gremlin over WebSocket and HTTP against one graph.
type Server struct {
	ctx   context.Context //nolint:containedctx // lifetime of the owning listener
	eng   *Engine
	graph *opencypher.Graph
	opt   Options
}

// NewServer returns a server whose WebSocket connections close when ctx is done.
func NewServer(ctx context.Context, g *opencypher.Graph, opt Options) *Server {
	if opt.Timeout <= 0 {
		opt.Timeout = defaultTimeout
	}

	return &Server{ctx: ctx, eng: NewEngine(), graph: g, opt: opt}
}

type codec int

const (
	codecV3 codec = iota
	codecV2
	codecBinary
)

type message struct {
	args      map[string]any
	id        string
	op        string
	processor string
	codec     codec
}

// ServeHTTP upgrades WebSocket requests and answers HTTP queries.
func (s *Server) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if websocket.IsWebSocketUpgrade(r) {
		s.serveWS(w, r)

		return
	}

	s.serveHTTP(w, r)
}

func (s *Server) serveWS(w http.ResponseWriter, r *http.Request) {
	up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}

	conn, err := up.Upgrade(w, r, nil)
	if err != nil {
		return
	}
	defer conn.Close()

	stop := context.AfterFunc(s.ctx, func() { _ = conn.Close() })
	defer stop()

	conn.SetReadLimit(maxFrameBytes)

	for {
		mt, data, rerr := conn.ReadMessage()
		if rerr != nil {
			return
		}

		if mt != websocket.TextMessage && mt != websocket.BinaryMessage {
			continue
		}

		if s.handleFrame(r.Context(), conn, mt, data) != nil {
			return
		}
	}
}

func parseFrame(mt int, data []byte) (*message, error) {
	if mt == websocket.TextMessage {
		return parseJSONRequest(data, codecV3)
	}

	if len(data) == 0 {
		return nil, invalidArg("Empty message")
	}

	n := int(data[0])
	if n+1 > len(data) {
		return nil, invalidArg("Malformed message: invalid content-type header")
	}

	mime, body := string(data[1:1+n]), data[1+n:]

	switch mime {
	case mimeGraphBinary:
		req, err := decodeBinaryRequest(body)
		if err != nil {
			return nil, err
		}

		return &message{id: req.id, op: req.op, processor: req.processor, args: req.args, codec: codecBinary}, nil
	case mimeGraphSONv3, mimeJSON:
		return parseJSONRequest(body, codecV3)
	case mimeGraphSONv2:
		return parseJSONRequest(body, codecV2)
	}

	return nil, invalidArg("Serializer for content-type %s not found", mime)
}

func parseJSONRequest(body []byte, c codec) (*message, error) {
	var raw struct {
		Args      map[string]any `json:"args"`
		RequestID string         `json:"requestId"`
		Op        string         `json:"op"`
		Processor string         `json:"processor"`
	}

	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()

	if err := dec.Decode(&raw); err != nil {
		return nil, invalidArg("Malformed message: %v", err)
	}

	if raw.RequestID == "" {
		raw.RequestID = uuid.NewString()
	}

	msg := &message{id: raw.RequestID, op: raw.Op, processor: raw.Processor, codec: c, args: map[string]any{}}

	for k, v := range raw.Args {
		d, err := decodeGraphSON(v)
		if err != nil {
			return nil, err
		}

		msg.args[k] = d
	}

	return msg, nil
}

func (s *Server) handleFrame(ctx context.Context, conn *websocket.Conn, mt int, data []byte) error {
	msg, err := parseFrame(mt, data)
	if err != nil {
		return writeResult(conn, &message{id: uuid.Nil.String(), codec: codecV3}, nil, err)
	}

	out, err := s.execute(ctx, msg)

	return writeResult(conn, msg, out, err)
}

func (s *Server) execute(ctx context.Context, msg *message) ([]any, error) {
	switch msg.op {
	case opEval:
		return s.evalMessage(ctx, msg)
	case opBytecode:
		return s.bytecodeMessage(ctx, msg)
	case "close":
		return nil, nil
	}

	return nil, invalidArg("Message with op code [%s] is not recognized.", msg.op)
}

func (s *Server) queryContext(msg *message, label string) (context.Context, func()) {
	base := s.ctx
	finish := func() {}

	if s.opt.Track != nil {
		base, finish = s.opt.Track(s.ctx, label)
	}

	timeout := s.opt.Timeout
	if ms, ok := msg.args["evaluationTimeout"]; ok && isInt(ms) && toInt(ms) > 0 {
		timeout = min(timeout, time.Duration(toInt(ms))*time.Millisecond)
	}

	ctx, cancel := context.WithTimeout(base, timeout)

	return ctx, func() {
		cancel()
		finish()
	}
}

func (s *Server) evalMessage(reqCtx context.Context, msg *message) ([]any, error) {
	script, ok := msg.args["gremlin"].(string)
	if !ok || strings.TrimSpace(script) == "" {
		return nil, invalidArg("A message with an [eval] op code requires a [gremlin] argument.")
	}

	if len(script) > maxScriptBytes {
		return nil, invalidArg("The Gremlin script exceeds the maximum size of %d bytes", maxScriptBytes)
	}

	if err := s.authorize(reqCtx, ScriptAccess(script)); err != nil {
		return nil, err
	}

	ctx, finish := s.queryContext(msg, script)
	defer finish()

	return s.eng.Eval(ctx, s.graph, script, bindingsOf(msg.args["bindings"]))
}

func (s *Server) bytecodeMessage(reqCtx context.Context, msg *message) ([]any, error) {
	tr, ok := msg.args["gremlin"].(*Traversal)
	if !ok {
		return nil, invalidArg("A message with a [bytecode] op code requires a [gremlin] argument of bytecode.")
	}

	if err := s.authorize(reqCtx, tr.Access()); err != nil {
		return nil, err
	}

	ctx, finish := s.queryContext(msg, "bytecode: "+describe(tr))
	defer finish()

	return s.eng.Bytecode(ctx, s.graph, tr)
}

func (s *Server) authorize(ctx context.Context, a Access) error {
	if s.opt.Authorize == nil {
		return nil
	}

	return s.opt.Authorize(ctx, a.Write, a.Delete)
}

func describe(tr *Traversal) string {
	names := make([]string, len(tr.Steps))
	for i, st := range tr.Steps {
		names[i] = st.Name
	}

	return "g." + strings.Join(names, ".")
}

func bindingsOf(v any) map[string]any {
	m, ok := v.(*Map)
	if !ok {
		return nil
	}

	out := make(map[string]any, m.Len())

	for i := range m.Len() {
		k, val := m.Entry(i)
		if ks, isStr := k.(string); isStr {
			out[ks] = val
		}
	}

	return out
}

func batchSizeOf(msg *message) int {
	if n, ok := msg.args["batchSize"]; ok && isInt(n) {
		return int(min(max(toInt(n), 1), maxBatchSize))
	}

	return defaultBatchSize
}

// errorMessage renders the Neptune-style error document carried in the status message.
func errorMessage(id string, e *Error) string {
	b, _ := marshalNoEscape(map[string]string{jsonReqID: id, jsonCode: e.Code, "detailedMessage": e.Detail})

	return string(b)
}

func asGremlinError(err error) *Error {
	if e, ok := AsError(err); ok {
		return e
	}

	return &Error{Code: CodeInternal, Status: StatusServerError, Detail: err.Error()}
}

func writeResult(conn *websocket.Conn, msg *message, out []any, err error) error {
	if err != nil {
		e := asGremlinError(err)

		return writeFrame(conn, msg, e.Status, errorMessage(msg.id, e), nil, false)
	}

	if len(out) == 0 {
		return writeFrame(conn, msg, StatusNoContent, "", nil, false)
	}

	size := batchSizeOf(msg)

	for start := 0; start < len(out); start += size {
		end := min(start+size, len(out))
		code := StatusPartial

		if end == len(out) {
			code = StatusSuccess
		}

		if werr := writeFrame(conn, msg, code, "", out[start:end], true); werr != nil {
			return werr
		}
	}

	return nil
}

func writeFrame(conn *websocket.Conn, msg *message, code int, text string, data []any, hasData bool) error {
	mt, payload := encodeFrame(msg, code, text, data, hasData)

	_ = conn.SetWriteDeadline(time.Now().Add(writeTimeout))

	return conn.WriteMessage(mt, payload)
}

func encodeFrame(msg *message, code int, text string, data []any, hasData bool) (int, []byte) {
	if msg.codec == codecBinary {
		w := &bbWriter{}

		return websocket.BinaryMessage, w.response(msg.id, code, text, data, hasData)
	}

	payload, err := graphsonWriter{v2: msg.codec == codecV2}.response(msg.id, code, text, data, hasData)
	if err != nil {
		payload, _ = graphsonWriter{}.response(msg.id, StatusSerializationErr, err.Error(), nil, false)
	}

	return websocket.TextMessage, payload
}

type httpRequest struct {
	Bindings map[string]any `json:"bindings"`
	Gremlin  string         `json:"gremlin"`
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	req, err := readHTTPRequest(w, r)
	if err != nil {
		writeHTTPError(w, asGremlinError(err), uuid.NewString())

		return
	}

	id := uuid.NewString()
	msg := &message{id: id, op: opEval, args: map[string]any{"gremlin": req.Gremlin}}

	if req.Bindings != nil {
		b := NewMap()

		for k, v := range req.Bindings {
			d, derr := decodeGraphSON(toJSONNumber(v))
			if derr != nil {
				writeHTTPError(w, asGremlinError(derr), id)

				return
			}

			b.Put(k, d)
		}

		msg.args["bindings"] = b
	}

	out, err := s.evalMessage(r.Context(), msg)
	if err != nil {
		writeHTTPError(w, asGremlinError(err), id)

		return
	}

	body, merr := graphsonWriter{v2: strings.Contains(r.Header.Get("Accept"), "gremlin-v2.0")}.
		response(id, StatusSuccess, "", out, true)
	if merr != nil {
		writeHTTPError(w, &Error{Code: CodeInternal, Status: StatusSerializationErr, Detail: merr.Error()}, id)

		return
	}

	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write(body)
}

func toJSONNumber(v any) any {
	if f, ok := v.(float64); ok {
		return json.Number(formatFloat(f))
	}

	return v
}

func readHTTPRequest(w http.ResponseWriter, r *http.Request) (*httpRequest, error) {
	switch r.Method {
	case http.MethodGet:
		return &httpRequest{Gremlin: r.URL.Query().Get("gremlin")}, nil
	case http.MethodPost:
		r.Body = http.MaxBytesReader(w, r.Body, maxFrameBytes)

		var req httpRequest

		if err := json.NewDecoder(r.Body).Decode(&req); err != nil && !errors.Is(err, io.EOF) {
			return nil, invalidArg("Malformed request body: %v", err)
		}

		return &req, nil
	}

	return nil, invalidArg("Only GET and POST are supported")
}

func writeHTTPError(w http.ResponseWriter, e *Error, id string) {
	status := http.StatusInternalServerError

	switch e.Code {
	case CodeMalformedQuery, CodeInvalidParameter, CodeConstraintViolation, CodeUnsupported:
		status = http.StatusBadRequest
	case CodeUnauthorized:
		status = http.StatusForbidden
	}

	w.Header().Set("Content-Type", "application/json;charset=UTF-8")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(map[string]string{jsonReqID: id, jsonCode: e.Code, "detailedMessage": e.Detail})
}
