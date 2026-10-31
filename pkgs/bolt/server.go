package bolt

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"strconv"

	"github.com/google/uuid"
)

// Message tags.
const (
	msgHello     byte = 0x01
	msgGoodbye   byte = 0x02
	msgReset     byte = 0x0F
	msgRun       byte = 0x10
	msgBegin     byte = 0x11
	msgCommit    byte = 0x12
	msgRollback  byte = 0x13
	msgDiscard   byte = 0x2F
	msgPull      byte = 0x3F
	msgTelemetry byte = 0x54
	msgRoute     byte = 0x66
	msgLogon     byte = 0x6A
	msgLogoff    byte = 0x6B
	msgSuccess   byte = 0x70
	msgRecord    byte = 0x71
	msgIgnored   byte = 0x7E
	msgFailure   byte = 0x7F
)

// Failure codes sent to drivers.
const (
	CodeSyntax        = "Neo.ClientError.Statement.SyntaxError"
	CodeInvalid       = "Neo.ClientError.Request.Invalid"
	CodeUnauthorized  = "Neo.ClientError.Security.Unauthorized"
	CodeForbidden     = "Neo.ClientError.Security.Forbidden"
	CodeConstraint    = "Neo.ClientError.Schema.ConstraintValidationFailed"
	CodeTransient     = "Neo.TransientError.Transaction.Terminated"
	CodeDatabaseError = "Neo.DatabaseError.General.UnknownError"
	CodeProtocol      = "Neo.ClientError.Request.Invalid"
)

// Error is a failure reported to the client.
type Error struct {
	Code    string
	Message string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// Result is a fully materialised query result streamed to the client by PULL.
type Result struct {
	Fields []string
	Rows   [][]any
	Write  bool
}

// Session is one connection's view of the database. Run executes inside the open transaction, if any.
type Session interface {
	Run(ctx context.Context, query string, params map[string]any) (*Result, error)
	Begin(ctx context.Context) error
	Commit(ctx context.Context) error
	Rollback(ctx context.Context) error
	Close()
}

// Backend creates sessions and checks credentials.
type Backend interface {
	NewSession() Session
	Authenticate(ctx context.Context, auth map[string]any) error
}

// Config configures Serve.
type Config struct {
	Backend Backend
	// Agent is the HELLO "server" string.
	Agent string
	// Database is reported as the default database name.
	Database string
	// Addr is the address advertised by ROUTE; the connection's local address is used when empty.
	Addr string
}

type state int

const (
	stConnected state = iota
	stAuth
	stReady
	stStreaming
	stTxReady
	stTxStreaming
	stFailed
)

var errClosed = errors.New("bolt: connection closed")

type conn struct {
	sess      Session
	nc        net.Conn
	result    *Result
	w         *bufio.Writer
	enc       *Encoder
	r         *bufio.Reader
	cfg       Config
	pos       int
	qid       int64
	bookmarks uint64
	st        state
	ver       Version
	inTx      bool
}

// Serve speaks Bolt on nc until the client leaves or ctx ends, then closes nc.
func Serve(ctx context.Context, nc net.Conn, cfg Config) {
	defer func() { _ = nc.Close() }()

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	go func() {
		<-ctx.Done()
		_ = nc.Close()
	}()

	ver, err := Negotiate(nc)
	if err != nil {
		return
	}

	c := &conn{
		nc: nc, cfg: cfg, ver: ver, enc: NewEncoder(ver),
		r: bufio.NewReader(nc), w: bufio.NewWriter(nc), sess: cfg.Backend.NewSession(),
	}
	defer c.sess.Close()

	for {
		if c.step(ctx) != nil {
			return
		}
	}
}

func (c *conn) step(ctx context.Context) error {
	raw, err := readMessage(c.r)
	if err != nil {
		return err
	}

	tag, fields, err := parseMessage(raw)
	if err != nil {
		return err
	}

	if derr := c.dispatch(ctx, tag, fields); derr != nil {
		return derr
	}

	if ferr := c.w.Flush(); ferr != nil {
		return fmt.Errorf("flush: %w", ferr)
	}

	return nil
}

func parseMessage(raw []byte) (byte, []any, error) {
	v, err := NewDecoder(raw).Decode()
	if err != nil {
		return 0, nil, err
	}

	s, ok := v.(*Struct)
	if !ok {
		return 0, nil, ErrBadMessage
	}

	return s.Tag, s.Fields, nil
}

func (c *conn) send(tag byte, fields ...any) error {
	c.enc.Reset()

	if err := c.enc.encodeStruct(tag, fields, 0); err != nil {
		return err
	}

	return writeMessage(c.w, c.enc.Bytes())
}

func (c *conn) success(meta map[string]any) error { return c.send(msgSuccess, meta) }

func (c *conn) fail(err error) error {
	be := &Error{Code: CodeDatabaseError, Message: err.Error()}
	if x, ok := errors.AsType[*Error](err); ok {
		be = x
	}

	c.result = nil
	c.st = stFailed

	return c.send(msgFailure, map[string]any{"code": be.Code, "message": be.Message})
}

func (c *conn) violation(msg string) error {
	_ = c.fail(&Error{Code: CodeProtocol, Message: msg})
	_ = c.w.Flush()

	return errClosed
}

func (c *conn) dispatch(ctx context.Context, tag byte, f []any) error {
	switch tag {
	case msgGoodbye:
		return errClosed
	case msgReset:
		return c.reset(ctx)
	case msgTelemetry:
		return c.success(map[string]any{})
	}

	if c.st == stFailed {
		return c.send(msgIgnored)
	}

	return c.dispatchState(ctx, tag, f)
}

func (c *conn) dispatchState(ctx context.Context, tag byte, f []any) error {
	switch tag {
	case msgHello:
		return c.hello(ctx, f)
	case msgLogon:
		return c.logon(ctx, f)
	case msgLogoff:
		return c.logoff()
	case msgRun:
		return c.run(ctx, f)
	case msgPull, msgDiscard:
		return c.pull(tag, f)
	case msgBegin, msgCommit, msgRollback:
		return c.transaction(ctx, tag)
	case msgRoute:
		return c.route(f)
	default:
		return c.violation(fmt.Sprintf("unknown message 0x%02X", tag))
	}
}

func (c *conn) hello(ctx context.Context, f []any) error {
	if c.st != stConnected {
		return c.violation("HELLO outside CONNECTED")
	}

	extra := mapArg(f, 0)
	if c.legacyAuth() {
		if err := c.authenticate(ctx, extra); err != nil {
			return c.fail(err)
		}

		c.st = stReady
	} else {
		c.st = stAuth
	}

	return c.success(map[string]any{
		"server":        c.cfg.Agent,
		"connection_id": "bolt-" + uuid.NewString(),
		"hints":         map[string]any{},
	})
}

func (c *conn) legacyAuth() bool {
	return c.ver.Major < secondMajor || (c.ver.Major == secondMajor && c.ver.Minor < logonMinMinor)
}

func (c *conn) authenticate(ctx context.Context, auth map[string]any) error {
	if err := c.cfg.Backend.Authenticate(ctx, auth); err != nil {
		if _, ok := errors.AsType[*Error](err); ok {
			return err
		}

		return &Error{Code: CodeUnauthorized, Message: err.Error()}
	}

	return nil
}

func (c *conn) logon(ctx context.Context, f []any) error {
	if c.st != stAuth || c.legacyAuth() {
		return c.violation("LOGON outside AUTHENTICATION")
	}

	if err := c.authenticate(ctx, mapArg(f, 0)); err != nil {
		return c.fail(err)
	}

	c.st = stReady

	return c.success(map[string]any{})
}

func (c *conn) logoff() error {
	if c.st != stReady || c.legacyAuth() {
		return c.violation("LOGOFF outside READY")
	}

	c.st = stAuth

	return c.success(map[string]any{})
}

func (c *conn) reset(ctx context.Context) error {
	if c.st == stConnected || c.st == stAuth {
		return c.violation("RESET before authentication")
	}

	c.result = nil
	_ = c.endTx(ctx, false)
	c.st = stReady

	return c.success(map[string]any{})
}

func (c *conn) endTx(ctx context.Context, commit bool) error {
	if !c.inTx {
		return nil
	}

	c.inTx = false

	if commit {
		return c.sess.Commit(ctx)
	}

	return c.sess.Rollback(ctx)
}

func (c *conn) run(ctx context.Context, f []any) error {
	if c.st != stReady && c.st != stTxReady && c.st != stStreaming && c.st != stTxStreaming {
		return c.violation("RUN outside READY")
	}

	query, _ := argAt(f, 0).(string)
	params := mapArg(f, 1)

	res, err := c.sess.Run(ctx, query, params)
	if err != nil {
		return c.fail(err)
	}

	c.result, c.pos = res, 0
	meta := map[string]any{"fields": res.Fields, "t_first": int64(0)}

	if c.inTx {
		c.qid++
		meta["qid"] = c.qid - 1
		c.st = stTxStreaming
	} else {
		c.st = stStreaming
	}

	return c.success(meta)
}

func (c *conn) pull(tag byte, f []any) error {
	if c.result == nil || (c.st != stStreaming && c.st != stTxStreaming) {
		return c.violation("PULL outside STREAMING")
	}

	n := int64(-1)
	if v, ok := mapArg(f, 0)["n"].(int64); ok {
		n = v
	}

	if n == 0 || n < -1 {
		return c.fail(&Error{Code: CodeInvalid, Message: "n must be positive or -1"})
	}

	if n == -1 || n > int64(len(c.result.Rows)-c.pos) {
		n = int64(len(c.result.Rows) - c.pos)
	}

	if tag == msgPull {
		if err := c.sendRecords(int(n)); err != nil {
			return err
		}
	}

	c.pos += int(n)

	if c.pos < len(c.result.Rows) {
		return c.success(map[string]any{"has_more": true})
	}

	return c.finishStream()
}

func (c *conn) sendRecords(n int) error {
	for _, row := range c.result.Rows[c.pos : c.pos+n] {
		if err := c.send(msgRecord, row); err != nil {
			return err
		}
	}

	return nil
}

func (c *conn) finishStream() error {
	meta := map[string]any{"t_last": int64(0), "db": c.cfg.Database, "type": "r"}
	if c.result.Write {
		meta["type"] = "w"
	}

	c.result = nil

	if c.inTx {
		c.st = stTxReady
	} else {
		c.st = stReady
		meta["bookmark"] = c.nextBookmark()
	}

	return c.success(meta)
}

func (c *conn) nextBookmark() string {
	c.bookmarks++

	return "neptune:bookmark:" + strconv.FormatUint(c.bookmarks, 10)
}

func (c *conn) transaction(ctx context.Context, tag byte) error {
	if tag == msgBegin {
		return c.begin(ctx)
	}

	if !c.inTx || (c.st != stTxReady && c.st != stTxStreaming) {
		return c.violation("COMMIT/ROLLBACK outside a transaction")
	}

	c.result = nil
	c.st = stReady

	if err := c.endTx(ctx, tag == msgCommit); err != nil {
		return c.fail(err)
	}

	if tag == msgCommit {
		return c.success(map[string]any{"bookmark": c.nextBookmark()})
	}

	return c.success(map[string]any{})
}

func (c *conn) begin(ctx context.Context) error {
	if c.st != stReady {
		return c.violation("BEGIN outside READY")
	}

	if err := c.sess.Begin(ctx); err != nil {
		return c.fail(err)
	}

	c.inTx, c.qid, c.st = true, 0, stTxReady

	return c.success(map[string]any{})
}

func (c *conn) route(f []any) error {
	if c.st != stReady {
		return c.violation("ROUTE outside READY")
	}

	addr := c.cfg.Addr
	if addr == "" {
		addr = c.nc.LocalAddr().String()
	}

	db := c.routeDatabase(f)

	servers := make([]any, 0, 3) //nolint:mnd // ROUTE, READ, WRITE

	for _, role := range []string{"ROUTE", "READ", "WRITE"} {
		servers = append(servers, map[string]any{"role": role, "addresses": []string{addr}})
	}

	rt := map[string]any{"ttl": routeTTL, "db": db, "servers": servers}

	return c.success(map[string]any{"rt": rt})
}

func argAt(f []any, i int) any {
	if i < len(f) {
		return f[i]
	}

	return nil
}

func mapArg(f []any, i int) map[string]any {
	m, _ := argAt(f, i).(map[string]any)

	return m
}

const (
	routeExtraIdx = 2
	routeTTL      = int64(300)
)

// routeDatabase reads the requested database from ROUTE's extra map (4.4+) or bare string (4.3).
func (c *conn) routeDatabase(f []any) string {
	switch x := argAt(f, routeExtraIdx).(type) {
	case map[string]any:
		if s, _ := x["db"].(string); s != "" {
			return s
		}
	case string:
		if x != "" {
			return x
		}
	}

	return c.cfg.Database
}
